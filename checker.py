#!/usr/bin/env python
import os
import gzip
import glob
from collections import defaultdict
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import argparse
import sys


def check_data_integrity(data_dir, symbol=None, start_date=None, end_date=None, plot=False):
    """Проверяет целостность собранных данных."""
    print(f"🔍 Проверка целостности данных в {data_dir}")
    
    # Находим все файлы
    data_dir = Path(data_dir)
    file_pattern = f"binance_{symbol or '*'}_*.csv.gz"
    files = list(data_dir.glob(file_pattern))
    
    if not files:
        print(f"❌ Файлы не найдены: {file_pattern}")
        return False
    
    print(f"📁 Найдено {len(files)} файлов")
    
    # Группируем файлы по символу и типу данных
    grouped_files = defaultdict(lambda: defaultdict(list))
    for file in files:
        # binance_btcusdt_20260112_10_trades.csv.gz
        parts = file.name.split('_')
        if len(parts) < 5:
            print(f"⚠️ Некорректное имя файла: {file.name}")
            continue
        
        symbol = parts[1]
        date_hour = parts[2] + "_" + parts[3]
        data_type = parts[4].split('.')[0]
        
        # Фильтр по датам
        if start_date or end_date:
            file_date = datetime.strptime(parts[2] + "_" + parts[3], "%Y%m%d_%H")
            if start_date and file_date < start_date:
                continue
            if end_date and file_date > end_date:
                continue
        
        grouped_files[symbol][data_type].append((date_hour, file))
    
    # Для каждого символа и типа данных
    for symbol, data_types in grouped_files.items():
        print(f"\n🪙 Символ: {symbol.upper()}")
        
        # Проверяем все типы данных
        for data_type, files in data_types.items():
            print(f"\n📊 Тип данных: {data_type}")
            
            # Сортируем файлы по дате/времени
            files.sort(key=lambda x: x[0])
            
            # Статистика по часам
            print(f"📅 Проверка по часам ({len(files)} файлов):")
            
            # Для сбора статистики
            hourly_stats = []
            
            for date_hour, file in files:
                # Считаем строки и собираем статистику
                stats = analyze_file(file, data_type)
                if stats['total_lines'] == 0:
                    print(f"  ❌ {date_hour}: Пустой файл {file.name}")
                    continue
                
                # Добавляем дату/час к статистике
                stats['date_hour'] = date_hour
                hourly_stats.append(stats)
                
                # Вывод статистики по файлу
                print(f"  ✅ {date_hour}: {stats['total_lines']:,} строк, " +
                      f"ID: {stats['first_id']} → {stats['last_id']}, " +
                      f"Время: {format_timestamp(stats['first_time'])} → {format_timestamp(stats['last_time'])}")
            
            if not hourly_stats:
                print("  ⚠️ Нет данных для анализа")
                continue
            
            # Проверка пропусков между файлами
            print("\n📏 Проверка непрерывности между файлами:")
            check_continuity(hourly_stats, data_type)
            
            # Визуализация
            if plot:
                plot_stats(hourly_stats, symbol, data_type)
    
    print("\n✨ Проверка завершена!")
    return True


def analyze_file(file, data_type):
    """Анализирует файл и возвращает базовую статистику."""
    stats = {
        'file': str(file),
        'total_lines': 0,
        'first_time': None,
        'last_time': None,
        'first_id': None,
        'last_id': None,
        'time_gaps': [],
        'id_gaps': []
    }
    
    try:
        with gzip.open(file, 'rt') as f:
            prev_time = None
            prev_id = None
            
            for line_num, line in enumerate(f):
                stats['total_lines'] += 1
                parts = line.strip().split(',')
                
                if not parts or len(parts) < 3:
                    continue
                
                # Общие поля: время и ID
                timestamp = int(parts[0])
                
                # ID зависит от типа данных
                if data_type == 'trades':
                    id_val = int(parts[1])  # TradeId
                elif data_type in ['orderbook', 'depth']:
                    id_val = int(parts[2] if data_type == 'depth' else parts[1])  # UpdateId
                else:
                    id_val = line_num  # Fallback
                
                # Сохраняем первые значения
                if stats['first_time'] is None:
                    stats['first_time'] = timestamp
                    stats['first_id'] = id_val
                
                # Проверяем разрывы
                if prev_time and timestamp - prev_time > 5000:  # Разрыв > 5 секунд
                    stats['time_gaps'].append((prev_time, timestamp, timestamp - prev_time))
                
                if prev_id and data_type == 'trades':
                    if id_val - prev_id > 5:  # Разрыв > 5 ID для trades
                        stats['id_gaps'].append((prev_id, id_val, id_val - prev_id))
                
                # Обновляем предыдущие значения
                prev_time = timestamp
                prev_id = id_val
            
            # Сохраняем последние значения
            if stats['total_lines'] > 0:
                stats['last_time'] = prev_time
                stats['last_id'] = prev_id
    
    except Exception as e:
        print(f"❌ Ошибка при чтении файла {file}: {e}")
    
    return stats


def check_continuity(hourly_stats, data_type):
    """Проверяет непрерывность между файлами по времени и ID."""
    for i in range(1, len(hourly_stats)):
        prev = hourly_stats[i-1]
        curr = hourly_stats[i]
        
        # Проверка по времени
        time_diff = curr['first_time'] - prev['last_time']
        if time_diff > 10000:  # Более 10 секунд
            print(f"  ⚠️ Пропуск времени между {prev['date_hour']} и {curr['date_hour']}: " +
                  f"{time_diff/1000:.1f} сек")
        
        # Проверка по ID (только для trades)
        if data_type == 'trades' and prev['last_id'] and curr['first_id']:
            id_diff = curr['first_id'] - prev['last_id']
            if id_diff > 5:  # Пропуск более 5 ID
                print(f"  ⚠️ Пропуск ID между {prev['date_hour']} и {curr['date_hour']}: " +
                      f"{id_diff} записей (с {prev['last_id']} до {curr['first_id']})")


def plot_stats(hourly_stats, symbol, data_type):
    """Создаёт график для визуальной проверки целостности."""
    df = pd.DataFrame(hourly_stats)
    
    # Преобразуем timestamp в datetime
    df['start_time'] = pd.to_datetime(df['first_time'], unit='ms')
    df['end_time'] = pd.to_datetime(df['last_time'], unit='ms')
    df['duration'] = (df['last_time'] - df['first_time']) / 1000  # длительность в сек
    
    # Расчет частоты данных (строк в секунду)
    df['frequency'] = df['total_lines'] / df['duration']
    
    # Визуализация
    fig, axs = plt.subplots(2, 1, figsize=(12, 10))
    
    # График 1: количество записей в час
    axs[0].bar(df['date_hour'], df['total_lines'])
    axs[0].set_title(f'{symbol.upper()} - {data_type} - Количество записей в час')
    axs[0].set_ylabel('Количество записей')
    axs[0].tick_params(axis='x', rotation=90)
    
    # График 2: частота данных (записей в секунду)
    axs[1].bar(df['date_hour'], df['frequency'])
    axs[1].set_title(f'{symbol.upper()} - {data_type} - Частота данных (записей/сек)')
    axs[1].set_ylabel('Записей в секунду')
    axs[1].tick_params(axis='x', rotation=90)
    
    plt.tight_layout()
    
    # Сохраняем график
    output_dir = Path('integrity_checks')
    output_dir.mkdir(exist_ok=True)
    plt.savefig(output_dir / f'{symbol}_{data_type}_integrity.png')
    print(f"📈 График сохранен: integrity_checks/{symbol}_{data_type}_integrity.png")


def format_timestamp(ts):
    """Форматирует timestamp для вывода."""
    if not ts:
        return "N/A"
    return datetime.fromtimestamp(ts/1000).strftime('%H:%M:%S')


def main():
    parser = argparse.ArgumentParser(description="Проверка целостности данных Binance")
    parser.add_argument("data_dir", help="Директория с файлами данных")
    parser.add_argument("--symbol", help="Символ для проверки (например, btcusdt)")
    parser.add_argument("--start", help="Начальная дата (YYYYMMDD_HH)")
    parser.add_argument("--end", help="Конечная дата (YYYYMMDD_HH)")
    parser.add_argument("--plot", action="store_true", help="Создать графики")
    
    args = parser.parse_args()
    
    start_date = None
    if args.start:
        start_date = datetime.strptime(args.start, "%Y%m%d_%H")
    
    end_date = None
    if args.end:
        end_date = datetime.strptime(args.end, "%Y%m%d_%H")
    
    check_data_integrity(args.data_dir, args.symbol, start_date, end_date, args.plot)


if __name__ == "__main__":
    main()