import csv
import threading
import tracemalloc

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import psutil
from aiohttp import web

from config import telegram_token, telegram_chat_id

logger = logging.getLogger(__name__)

INTERVAL        = 10
HISTORY_SIZE    = 120
WEB_PORT        = 8080
ALERT_RAM_PCT   = 90.0
ALERT_DISK_GB   = 10.0
ALERT_CPU_PCT   = 95.0
ALERT_DROP_MIN  = 1
TG_TOKEN        = telegram_token
TG_CHAT_ID      = telegram_chat_id
DISK_PATH       = "/"
MEM_LOG_PATH    = Path("memory_log.csv")
MEM_LOG_INTERVAL = 30
MEM_LOG_TOP_N   = 20


@dataclass
class Metrics:
    ts:              float
    cpu_pct:         float
    ram_used_gb:     float
    ram_free_gb:     float
    ram_pct:         float
    disk_used_gb:    float
    disk_free_gb:    float
    disk_pct:        float
    disk_write_mbps: float
    net_recv_mbps:   float
    net_sent_mbps:   float
    net_drop_in:     int
    net_drop_out:    int
    net_err_in:      int
    net_err_out:     int


@dataclass
class MonitorState:
    history:               deque = field(default_factory=lambda: deque(maxlen=HISTORY_SIZE))
    last_net_bytes_recv:   int   = 0
    last_net_bytes_sent:   int   = 0
    last_disk_write_bytes: int   = 0
    last_collect_time:     float = 0.0
    alerted:               dict  = field(default_factory=dict)


def collect_metrics(state: MonitorState) -> Metrics:
    now     = time.monotonic()
    elapsed = now - state.last_collect_time if state.last_collect_time else INTERVAL
    cpu_pct = psutil.cpu_percent(interval=None)
    ram         = psutil.virtual_memory()
    disk        = psutil.disk_usage(DISK_PATH)
    disk_io     = psutil.disk_io_counters()
    net         = psutil.net_io_counters()

    disk_write_mbps = 0.0
    if state.last_disk_write_bytes and disk_io:
        disk_write_mbps = ((disk_io.write_bytes - state.last_disk_write_bytes) / elapsed) / 1024 ** 2
    if disk_io:
        state.last_disk_write_bytes = disk_io.write_bytes

    net_recv_mbps = net_sent_mbps = 0.0
    if state.last_net_bytes_recv:
        net_recv_mbps = ((net.bytes_recv - state.last_net_bytes_recv) / elapsed) / 1024 ** 2 * 8
        net_sent_mbps = ((net.bytes_sent - state.last_net_bytes_sent) / elapsed) / 1024 ** 2 * 8
    state.last_net_bytes_recv = net.bytes_recv
    state.last_net_bytes_sent = net.bytes_sent
    state.last_collect_time   = now

    return Metrics(
        ts=time.time(), cpu_pct=cpu_pct,
        ram_used_gb=round(ram.used/1024**3,2), ram_free_gb=round(ram.available/1024**3,2), ram_pct=ram.percent,
        disk_used_gb=round(disk.used/1024**3,2), disk_free_gb=round(disk.free/1024**3,2), disk_pct=disk.percent,
        disk_write_mbps=round(disk_write_mbps,2),
        net_recv_mbps=round(net_recv_mbps,2), net_sent_mbps=round(net_sent_mbps,2),
        net_drop_in=net.dropin, net_drop_out=net.dropout, net_err_in=net.errin, net_err_out=net.errout,
    )


def _init_mem_log():
    if not MEM_LOG_PATH.exists():
        with open(MEM_LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow(["ts", "module", "line", "size_mb", "count"])


def _write_mem_snapshot():
    if not tracemalloc.is_tracing():
        return
    snapshot = tracemalloc.take_snapshot()
    stats    = snapshot.statistics("lineno")
    ts       = datetime.now(tz=timezone.utc).isoformat()
    with open(MEM_LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        for stat in stats[:MEM_LOG_TOP_N]:
            tb     = stat.traceback[0]
            parts  = tb.filename.replace("\\", "/").split("/")
            module = "/".join(parts[-2:]) if len(parts) >= 2 else tb.filename
            writer.writerow([ts, module, tb.lineno, round(stat.size/1024/1024, 4), stat.count])


async def send_telegram(text: str):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
            )
    except Exception as e:
        logger.error(f"[monitor] Telegram ошибка: {e}")


async def check_alerts(m: Metrics, state: MonitorState):
    alerts = []

    def once(key, msg):
        if not state.alerted.get(key):
            alerts.append(msg)
            state.alerted[key] = True

    def recover(key, msg):
        if state.alerted.get(key):
            alerts.append(msg)
            state.alerted[key] = False

    if m.ram_pct >= ALERT_RAM_PCT:
        once("ram",   f"⚠️ <b>RAM</b> {m.ram_pct:.1f}% — свободно {m.ram_free_gb:.1f} GB")
    else:
        recover("ram", f"✅ <b>RAM</b> в норме — {m.ram_pct:.1f}%")

    if m.disk_free_gb <= ALERT_DISK_GB:
        once("disk",  f"⚠️ <b>Диск</b> свободно {m.disk_free_gb:.1f} GB ({m.disk_pct:.1f}%)")
    else:
        recover("disk", f"✅ <b>Диск</b> в норме — {m.disk_free_gb:.1f} GB свободно")

    if m.cpu_pct >= ALERT_CPU_PCT:
        once("cpu",   f"⚠️ <b>CPU</b> {m.cpu_pct:.1f}%")
    else:
        recover("cpu", f"✅ <b>CPU</b> в норме — {m.cpu_pct:.1f}%")

    if m.net_drop_in >= ALERT_DROP_MIN or m.net_drop_out >= ALERT_DROP_MIN:
        once("drops", f"⚠️ <b>Сеть</b> потери: drop_in={m.net_drop_in} drop_out={m.net_drop_out}")

    for msg in alerts:
        logger.warning(f"[monitor] ALERT: {msg}")
        await send_telegram(msg)


HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"><title>Monitor</title>
<meta http-equiv="refresh" content="30">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  body{background:#0f0f0f;color:#e0e0e0;font-family:monospace;margin:0;padding:20px}
  h1{color:#00d4aa;margin-bottom:4px}.updated{color:#666;font-size:12px;margin-bottom:20px}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:20px}
  .card{background:#1a1a1a;border:1px solid #2a2a2a;border-radius:8px;padding:16px}
  .card h3{margin:0 0 12px 0;color:#00d4aa;font-size:14px}
  .stats{display:flex;gap:20px;margin-bottom:12px;flex-wrap:wrap}
  .stat{font-size:13px}.stat span{color:#00d4aa;font-weight:bold}.warn span{color:#ff6b6b}
  canvas{max-height:160px}
</style>
</head>
<body>
<h1>📊 System Monitor</h1>
<div class="updated">Обновляется каждые 30 сек | Последнее: <span id="ts"></span></div>
<div class="grid" id="grid"></div>
<script>
const data=__DATA__;
const labels=data.map(d=>new Date(d.ts*1000).toLocaleTimeString());
function stat(l,v,w){return`<div class="stat ${w?'warn':''}"><b>${l}</b><br><span>${v}</span></div>`}
const last=data[data.length-1]||{};
document.getElementById('ts').textContent=last.ts?new Date(last.ts*1000).toLocaleTimeString():'-';
const grid=document.getElementById('grid');
grid.innerHTML+=`<div class="card"><h3>🖥 CPU</h3><div class="stats">${stat('Загрузка',(last.cpu_pct||0).toFixed(1)+'%',last.cpu_pct>95)}</div><canvas id="cpu"></canvas></div>`;
grid.innerHTML+=`<div class="card"><h3>🧠 RAM</h3><div class="stats">${stat('Использовано',(last.ram_used_gb||0).toFixed(1)+' GB')+stat('Свободно',(last.ram_free_gb||0).toFixed(1)+' GB')+stat('%',(last.ram_pct||0).toFixed(1)+'%',last.ram_pct>90)}</div><canvas id="ram"></canvas></div>`;
grid.innerHTML+=`<div class="card"><h3>💾 Диск</h3><div class="stats">${stat('Свободно',(last.disk_free_gb||0).toFixed(1)+' GB',last.disk_free_gb<10)+stat('Запись',(last.disk_write_mbps||0).toFixed(2)+' MB/s')}</div><canvas id="disk"></canvas></div>`;
grid.innerHTML+=`<div class="card"><h3>🌐 Сеть</h3><div class="stats">${stat('↓ Recv',(last.net_recv_mbps||0).toFixed(1)+' Mbit/s')+stat('↑ Sent',(last.net_sent_mbps||0).toFixed(1)+' Mbit/s')+stat('Drops',(last.net_drop_in||0)+'/'+(last.net_drop_out||0),last.net_drop_in>0)}</div><canvas id="net"></canvas></div>`;
function makeChart(id,label,values,color){
  new Chart(document.getElementById(id),{type:'line',data:{labels,datasets:[{label,data:values,borderColor:color,backgroundColor:color+'22',borderWidth:1.5,pointRadius:0,fill:true,tension:0.3}]},options:{animation:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:'#555',maxTicksLimit:6},grid:{color:'#222'}},y:{ticks:{color:'#888'},grid:{color:'#222'}}}}});
}
makeChart('cpu','CPU %',data.map(d=>d.cpu_pct),'#00d4aa');
makeChart('ram','RAM %',data.map(d=>d.ram_pct),'#7c83fd');
makeChart('disk','Disk Write MB/s',data.map(d=>d.disk_write_mbps),'#ffd166');
makeChart('net','Net Recv Mbit/s',data.map(d=>d.net_recv_mbps),'#ef476f');
</script>
</body>
</html>"""


async def handle_dashboard(request: web.Request) -> web.Response:
    import json
    state = request.app["state"]
    data  = [
        {"ts":m.ts,"cpu_pct":m.cpu_pct,"ram_pct":m.ram_pct,"ram_used_gb":m.ram_used_gb,
         "ram_free_gb":m.ram_free_gb,"disk_free_gb":m.disk_free_gb,"disk_pct":m.disk_pct,
         "disk_write_mbps":m.disk_write_mbps,"net_recv_mbps":m.net_recv_mbps,
         "net_sent_mbps":m.net_sent_mbps,"net_drop_in":m.net_drop_in,"net_drop_out":m.net_drop_out}
        for m in state.history
    ]
    return web.Response(text=HTML.replace("__DATA__", json.dumps(data)), content_type="text/html")


async def handle_api(request: web.Request) -> web.Response:
    import json
    state = request.app["state"]
    last  = state.history[-1] if state.history else None
    return web.Response(text=json.dumps(last.__dict__ if last else {}), content_type="application/json")


class Monitor:
    def __init__(self):
        self._state           = MonitorState()
        self._running         = False
        self._last_mem_log_ts = 0.0
        tracemalloc.start()
        psutil.cpu_percent(interval=None)
        _init_mem_log()
        logger.info(f"[monitor] Лог памяти: {MEM_LOG_PATH.resolve()}")

    async def run(self):
        self._running = True
        logger.info(f"[monitor] Запущен | интервал={INTERVAL}s | порт={WEB_PORT}")

        app = web.Application()
        app["state"] = self._state
        app.router.add_get("/",    handle_dashboard)
        app.router.add_get("/api", handle_api)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", WEB_PORT).start()
        logger.info(f"[monitor] Дашборд: http://localhost:{WEB_PORT}")

        while self._running:
            await asyncio.sleep(INTERVAL)
            try:
                m = collect_metrics(self._state)
                self._state.history.append(m)
                self._log(m)
                await check_alerts(m, self._state)

                if time.monotonic() - self._last_mem_log_ts >= MEM_LOG_INTERVAL:
                    await asyncio.get_event_loop().run_in_executor(None, _write_mem_snapshot)
                    self._last_mem_log_ts = time.monotonic()

            except Exception as e:
                logger.error(f"[monitor] Ошибка: {e}")

        await runner.cleanup()

    def _log(self, m: Metrics):
        thread_count = threading.active_count()
        mem_info = ""
        if tracemalloc.is_tracing():
            snapshot = tracemalloc.take_snapshot()
            top      = snapshot.statistics("lineno")[:3]
            mem_info = " | top: " + ", ".join(f"{s.size/1024/1024:.1f}MB" for s in top)

        dt = datetime.fromtimestamp(m.ts, tz=timezone.utc).strftime("%H:%M:%S")
        logger.info(
            f"[monitor] {dt} | CPU {m.cpu_pct:5.1f}% | "
            f"RAM {m.ram_pct:5.1f}% ({m.ram_free_gb:.1f}GB free) | "
            f"Disk {m.disk_free_gb:.1f}GB free, write {m.disk_write_mbps:.2f}MB/s | "
            f"Net ↓{m.net_recv_mbps:.1f} ↑{m.net_sent_mbps:.1f} Mbit/s "
            f"drops {m.net_drop_in}/{m.net_drop_out} | threads {thread_count}{mem_info}"
        )

    def stop(self):
        self._running = False