import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import psutil
from aiohttp import web

logger = logging.getLogger(__name__)

# ─── Настройки ────────────────────────────────────────────────────────────────

INTERVAL        = 10       # секунд между сбором метрик
HISTORY_SIZE    = 120      # сколько точек хранить (~1 час при 30с)
WEB_PORT        = 8080

# Пороги для Telegram
ALERT_RAM_PCT   = 90.0     # % использования RAM
ALERT_DISK_GB   = 10.0     # GB свободного места
ALERT_CPU_PCT   = 95.0     # % CPU
ALERT_DROP_MIN  = 1        # минимум drops для алерта

# Telegram (заполни свои)
TG_TOKEN  = ""
TG_CHAT_ID = ""

# Диск для мониторинга
DISK_PATH = "/"


# ─── Структуры данных ─────────────────────────────────────────────────────────

@dataclass
class Metrics:
    ts:           float
    cpu_pct:      float
    ram_used_gb:  float
    ram_free_gb:  float
    ram_pct:      float
    disk_used_gb: float
    disk_free_gb: float
    disk_pct:     float
    disk_write_mbps: float
    net_recv_mbps: float
    net_sent_mbps: float
    net_drop_in:  int
    net_drop_out: int
    net_err_in:   int
    net_err_out:  int


@dataclass
class MonitorState:
    history: deque = field(default_factory=lambda: deque(maxlen=HISTORY_SIZE))
    last_net_bytes_recv: int = 0
    last_net_bytes_sent: int = 0
    last_disk_write_bytes: int = 0
    last_collect_time: float = 0.0
    # алерты — чтобы не спамить одно и то же
    alerted: dict = field(default_factory=dict)


# ─── Сбор метрик ──────────────────────────────────────────────────────────────

def collect_metrics(state: MonitorState) -> Metrics:
    now = time.monotonic()
    elapsed = now - state.last_collect_time if state.last_collect_time else INTERVAL

    # CPU
    cpu_pct = psutil.cpu_percent(interval=None)

    # RAM
    ram = psutil.virtual_memory()
    ram_used_gb = ram.used / 1024 ** 3
    ram_free_gb = ram.available / 1024 ** 3
    ram_pct     = ram.percent

    # Диск
    disk = psutil.disk_usage(DISK_PATH)
    disk_used_gb = disk.used / 1024 ** 3
    disk_free_gb = disk.free / 1024 ** 3
    disk_pct     = disk.percent

    disk_io = psutil.disk_io_counters()
    disk_write_mbps = 0.0
    if state.last_disk_write_bytes and disk_io:
        diff = disk_io.write_bytes - state.last_disk_write_bytes
        disk_write_mbps = (diff / elapsed) / 1024 ** 2
    if disk_io:
        state.last_disk_write_bytes = disk_io.write_bytes

    # Сеть
    net = psutil.net_io_counters()
    net_recv_mbps = net_sent_mbps = 0.0
    if state.last_net_bytes_recv:
        net_recv_mbps = ((net.bytes_recv - state.last_net_bytes_recv) / elapsed) / 1024 ** 2 * 8
        net_sent_mbps = ((net.bytes_sent - state.last_net_bytes_sent) / elapsed) / 1024 ** 2 * 8
    state.last_net_bytes_recv = net.bytes_recv
    state.last_net_bytes_sent = net.bytes_sent
    state.last_collect_time   = now

    return Metrics(
        ts            = time.time(),
        cpu_pct       = cpu_pct,
        ram_used_gb   = round(ram_used_gb,  2),
        ram_free_gb   = round(ram_free_gb,  2),
        ram_pct       = ram_pct,
        disk_used_gb  = round(disk_used_gb, 2),
        disk_free_gb  = round(disk_free_gb, 2),
        disk_pct      = disk_pct,
        disk_write_mbps = round(disk_write_mbps, 2),
        net_recv_mbps = round(net_recv_mbps, 2),
        net_sent_mbps = round(net_sent_mbps, 2),
        net_drop_in   = net.dropin,
        net_drop_out  = net.dropout,
        net_err_in    = net.errin,
        net_err_out   = net.errout,
    )


# ─── Telegram ─────────────────────────────────────────────────────────────────

async def send_telegram(session: aiohttp.ClientSession, text: str):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        await session.post(url, json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"})
    except Exception as e:
        logger.error(f"[monitor] Telegram ошибка: {e}")


async def check_alerts(m: Metrics, state: MonitorState, session: aiohttp.ClientSession):
    alerts = []

    def once(key: str, msg: str, recover_key: Optional[str] = None):
        """Шлём алерт только один раз пока проблема не исчезнет."""
        if not state.alerted.get(key):
            alerts.append(msg)
            state.alerted[key] = True
        if recover_key and state.alerted.get(recover_key):
            state.alerted[recover_key] = False

    def recover(key: str, msg: str):
        if state.alerted.get(key):
            alerts.append(msg)
            state.alerted[key] = False

    # RAM
    if m.ram_pct >= ALERT_RAM_PCT:
        once("ram", f"⚠️ <b>RAM</b> {m.ram_pct:.1f}% — свободно {m.ram_free_gb:.1f} GB")
    else:
        recover("ram", f"✅ <b>RAM</b> в норме — {m.ram_pct:.1f}%")

    # Диск
    if m.disk_free_gb <= ALERT_DISK_GB:
        once("disk", f"⚠️ <b>Диск</b> свободно {m.disk_free_gb:.1f} GB ({m.disk_pct:.1f}%)")
    else:
        recover("disk", f"✅ <b>Диск</b> в норме — {m.disk_free_gb:.1f} GB свободно")

    # CPU
    if m.cpu_pct >= ALERT_CPU_PCT:
        once("cpu", f"⚠️ <b>CPU</b> {m.cpu_pct:.1f}%")
    else:
        recover("cpu", f"✅ <b>CPU</b> в норме — {m.cpu_pct:.1f}%")

    # Сеть — drops
    if m.net_drop_in >= ALERT_DROP_MIN or m.net_drop_out >= ALERT_DROP_MIN:
        once("drops", f"⚠️ <b>Сеть</b> потери пакетов: drop_in={m.net_drop_in} drop_out={m.net_drop_out}")

    for msg in alerts:
        logger.warning(f"[monitor] ALERT: {msg}")
        # await send_telegram(session, msg)


# ─── Веб-дашборд ──────────────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Monitor</title>
<meta http-equiv="refresh" content="30">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  body { background: #0f0f0f; color: #e0e0e0; font-family: monospace; margin: 0; padding: 20px; }
  h1 { color: #00d4aa; margin-bottom: 4px; }
  .updated { color: #666; font-size: 12px; margin-bottom: 20px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 20px; }
  .card { background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 8px; padding: 16px; }
  .card h3 { margin: 0 0 12px 0; color: #00d4aa; font-size: 14px; }
  .stats { display: flex; gap: 20px; margin-bottom: 12px; flex-wrap: wrap; }
  .stat { font-size: 13px; }
  .stat span { color: #00d4aa; font-weight: bold; }
  .warn span { color: #ff6b6b; }
  canvas { max-height: 160px; }
</style>
</head>
<body>
<h1>📊 System Monitor</h1>
<div class="updated">Обновляется каждые 30 сек | Последнее: <span id="ts"></span></div>
<div class="grid" id="grid"></div>
<script>
const data = __DATA__;
const labels = data.map(d => new Date(d.ts * 1000).toLocaleTimeString());

function card(title, stats, chartId, datasets) {
  return `<div class="card">
    <h3>${title}</h3>
    <div class="stats">${stats}</div>
    <canvas id="${chartId}"></canvas>
  </div>`;
}

function stat(label, value, warn) {
  return `<div class="stat ${warn ? 'warn' : ''}"><b>${label}</b><br><span>${value}</span></div>`;
}

const last = data[data.length - 1] || {};
document.getElementById('ts').textContent = last.ts ? new Date(last.ts * 1000).toLocaleTimeString() : '-';

const grid = document.getElementById('grid');

// CPU
grid.innerHTML += card('🖥 CPU', 
  stat('Загрузка', (last.cpu_pct || 0).toFixed(1) + '%', last.cpu_pct > 95),
  'cpu', []);

// RAM  
grid.innerHTML += card('🧠 RAM',
  stat('Использовано', (last.ram_used_gb || 0).toFixed(1) + ' GB') +
  stat('Свободно', (last.ram_free_gb || 0).toFixed(1) + ' GB') +
  stat('%', (last.ram_pct || 0).toFixed(1) + '%', last.ram_pct > 90),
  'ram', []);

// Диск
grid.innerHTML += card('💾 Диск',
  stat('Свободно', (last.disk_free_gb || 0).toFixed(1) + ' GB', last.disk_free_gb < 10) +
  stat('Запись', (last.disk_write_mbps || 0).toFixed(2) + ' MB/s') +
  stat('%', (last.disk_pct || 0).toFixed(1) + '%'),
  'disk', []);

// Сеть
grid.innerHTML += card('🌐 Сеть',
  stat('↓ Recv', (last.net_recv_mbps || 0).toFixed(1) + ' Mbit/s') +
  stat('↑ Sent', (last.net_sent_mbps || 0).toFixed(1) + ' Mbit/s') +
  stat('Drops', (last.net_drop_in || 0) + '/' + (last.net_drop_out || 0), last.net_drop_in > 0),
  'net', []);

function makeChart(id, label, values, color) {
  new Chart(document.getElementById(id), {
    type: 'line',
    data: {
      labels,
      datasets: [{ label, data: values, borderColor: color, backgroundColor: color + '22',
        borderWidth: 1.5, pointRadius: 0, fill: true, tension: 0.3 }]
    },
    options: {
      animation: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#555', maxTicksLimit: 6 }, grid: { color: '#222' } },
        y: { ticks: { color: '#888' }, grid: { color: '#222' } }
      }
    }
  });
}

makeChart('cpu',  'CPU %',       data.map(d => d.cpu_pct),        '#00d4aa');
makeChart('ram',  'RAM %',       data.map(d => d.ram_pct),         '#7c83fd');
makeChart('disk', 'Disk Write',  data.map(d => d.disk_write_mbps), '#ffd166');
makeChart('net',  'Net Recv Mbit/s', data.map(d => d.net_recv_mbps), '#ef476f');
</script>
</body>
</html>"""


async def handle_dashboard(request: web.Request) -> web.Response:
    state: MonitorState = request.app["state"]
    history = list(state.history)
    data = [
        {
            "ts":             m.ts,
            "cpu_pct":        m.cpu_pct,
            "ram_pct":        m.ram_pct,
            "ram_used_gb":    m.ram_used_gb,
            "ram_free_gb":    m.ram_free_gb,
            "disk_free_gb":   m.disk_free_gb,
            "disk_pct":       m.disk_pct,
            "disk_write_mbps":m.disk_write_mbps,
            "net_recv_mbps":  m.net_recv_mbps,
            "net_sent_mbps":  m.net_sent_mbps,
            "net_drop_in":    m.net_drop_in,
            "net_drop_out":   m.net_drop_out,
        }
        for m in history
    ]
    import json
    html = HTML.replace("__DATA__", json.dumps(data))
    return web.Response(text=html, content_type="text/html")


async def handle_api(request: web.Request) -> web.Response:
    state: MonitorState = request.app["state"]
    import json
    last = state.history[-1] if state.history else None
    if not last:
        return web.Response(text="{}", content_type="application/json")
    return web.Response(
        text=json.dumps(last.__dict__),
        content_type="application/json"
    )


# ─── Основной класс ───────────────────────────────────────────────────────────

class Monitor:
    def __init__(self):
        self._state   = MonitorState()
        self._running = False
        # прогрев psutil cpu_percent
        psutil.cpu_percent(interval=None)

    async def run(self):
        self._running = True
        logger.info(f"[monitor] Запущен | интервал={INTERVAL}s | порт={WEB_PORT}")

        async with aiohttp.ClientSession() as session:
            # запускаем веб-сервер
            app = web.Application()
            app["state"] = self._state
            app.router.add_get("/",    handle_dashboard)
            app.router.add_get("/api", handle_api)

            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "0.0.0.0", WEB_PORT)
            await site.start()
            logger.info(f"[monitor] Дашборд: http://localhost:{WEB_PORT}")

            while self._running:
                await asyncio.sleep(INTERVAL)
                try:
                    m = collect_metrics(self._state)
                    self._state.history.append(m)
                    self._log(m)
                    await check_alerts(m, self._state, session)
                except Exception as e:
                    logger.error(f"[monitor] Ошибка сбора метрик: {e}")

            await runner.cleanup()

    def _log(self, m: Metrics):
        dt = datetime.fromtimestamp(m.ts, tz=timezone.utc).strftime("%H:%M:%S")
        logger.info(
            f"[monitor] {dt} | "
            f"CPU {m.cpu_pct:5.1f}% | "
            f"RAM {m.ram_pct:5.1f}% ({m.ram_free_gb:.1f}GB free) | "
            f"Disk {m.disk_free_gb:.1f}GB free, write {m.disk_write_mbps:.2f}MB/s | "
            f"Net ↓{m.net_recv_mbps:.1f} ↑{m.net_sent_mbps:.1f} Mbit/s "
            f"drops {m.net_drop_in}/{m.net_drop_out}"
        )

    def stop(self):
        self._running = False