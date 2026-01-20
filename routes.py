from fastapi import APIRouter, HTTPException, Query, Request
from typing import Optional
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timezone

router = APIRouter()


@router.get("/data/schema")
async def get_data_schema(
    request: Request,
    exchange: str = Query(..., description="Exchange name: binance, gate"),
    symbol: str = Query(..., description="Symbol: btcusdt, ethusdt, etc."),
    data_type: str = Query(..., description="Data type: trades, bookticker, depth"),
    hour: Optional[str] = Query(None, description="Hour in format YYYYMMDD_HH (default: current)")
):
    """
    Получить схему данных и первые 10 строк.
    Создает временный merged файл, возвращает данные и удаляет файл.
    
    Query params:
        exchange: binance, gate
        symbol: btcusdt, ethusdt, etc.
        data_type: trades, bookticker, depth
        hour: YYYYMMDD_HH (опционально, по умолчанию текущий)
    
    Returns:
        {
            "exchange": "binance",
            "symbol": "btcusdt",
            "data_type": "trades",
            "hour": "20250120_14",
            "schema": {...},
            "sample_data": [...],
            "total_rows": 12345,
            "file_size_kb": 456.78
        }
    """
    exch = exchange.lower()
    sym = symbol.lower()
    dt = data_type.lower()
    
    if exch not in ["binance", "gate"]:
        raise HTTPException(400, "Exchange must be 'binance' or 'gate'")
    
    if dt not in ["trades", "bookticker", "depth"]:
        raise HTTPException(400, "Data type must be 'trades', 'bookticker', or 'depth'")
    
    # Получаем нужный CloudManager
    if exch == "binance":
        cloud_manager = request.app.state.manager_binance
    else:
        cloud_manager = request.app.state.manager_gate
    
    # Определяем час
    target_hour = hour if hour else datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    date, hour_part = target_hour.split('_')
    
    # Проверяем наличие conn_* файлов
    hour_dir = Path("collected_data") / exch / sym / target_hour
    if not hour_dir.exists():
        raise HTTPException(
            404, 
            f"No data directory found for {sym} at {target_hour}. "
            f"Symbol may not be added to collector or no data collected yet."
        )
    
    source_files = list(hour_dir.glob(f"conn_*_{dt}.parquet"))
    if not source_files:
        raise HTTPException(
            404,
            f"No {dt} data files found for {sym} at {target_hour}. "
            f"Data may not have been collected yet for this data type."
        )
    
    merged_file = None
    
    try:
        # Создаем временный merged файл через CloudManager
        merged_file = await cloud_manager.merge_parquet_files(
            symbol=sym,
            date=date,
            hour=hour_part,
            data_type=dt
        )
        
        if not merged_file or not merged_file.exists():
            raise HTTPException(500, "Failed to create merged file")
        
        # Читаем файл
        table = pq.read_table(merged_file)
        
        # Схема
        schema = {
            field.name: str(field.type) 
            for field in table.schema
        }
        
        # Первые 10 строк
        df = table.to_pandas().head(10)
        
        # Конвертируем timestamp в читаемый формат
        if 'timestamp_ms' in df.columns:
            df['timestamp_utc'] = df['timestamp_ms'].apply(
                lambda x: datetime.fromtimestamp(x / 1000, tz=timezone.utc).isoformat()
            )
        
        if 'trade_time_ms' in df.columns:
            df['trade_time_utc'] = df['trade_time_ms'].apply(
                lambda x: datetime.fromtimestamp(x / 1000, tz=timezone.utc).isoformat()
            )
        
        sample_data = df.to_dict(orient='records')
        
        # Статистика файла
        file_size_kb = merged_file.stat().st_size / 1024
        total_rows = len(table)
        
        return {
            "exchange": exch,
            "symbol": sym,
            "data_type": dt,
            "hour": target_hour,
            "schema": schema,
            "schema_details": [
                {
                    "name": field.name,
                    "type": str(field.type),
                    "nullable": field.nullable
                }
                for field in table.schema
            ],
            "sample_data": sample_data,
            "total_rows": total_rows,
            "file_size_kb": round(file_size_kb, 2),
            "compression": "zstd",
            "source_files_count": len(source_files)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Error processing data: {str(e)}")
    
    finally:
        # Удаляем временный merged файл
        if merged_file and merged_file.exists():
            try:
                merged_file.unlink()
            except Exception as e:
                print(f"Warning: Failed to delete temp file {merged_file}: {e}")


@router.get("/data/preview")
async def preview_data(
    request: Request,
    exchange: str = Query(..., description="Exchange name"),
    symbol: str = Query(..., description="Symbol"),
    data_type: str = Query(..., description="Data type"),
    hour: Optional[str] = Query(None, description="Hour in format YYYYMMDD_HH (default: current)"),
    limit: int = Query(10, ge=1, le=1000, description="Number of rows to return")
):
    """
    Предварительный просмотр данных.
    Создает временный merged файл, возвращает данные и удаляет файл.
    
    Query params:
        exchange: binance, gate
        symbol: btcusdt, ethusdt
        data_type: trades, bookticker, depth
        hour: YYYYMMDD_HH (опционально, по умолчанию текущий)
        limit: количество строк (1-1000, default: 10)
    """
    exch = exchange.lower()
    sym = symbol.lower()
    dt = data_type.lower()
    
    if exch not in ["binance", "gate"]:
        raise HTTPException(400, "Exchange must be 'binance' or 'gate'")
    
    if dt not in ["trades", "bookticker", "depth"]:
        raise HTTPException(400, "Data type must be 'trades', 'bookticker', or 'depth'")
    
    # Получаем нужный CloudManager
    if exch == "binance":
        cloud_manager = request.app.state.manager_binance
    else:
        cloud_manager = request.app.state.manager_gate
    
    # Определяем час
    target_hour = hour if hour else datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    date, hour_part = target_hour.split('_')
    
    merged_file = None
    
    try:
        # Создаем временный merged файл
        merged_file = await cloud_manager.merge_parquet_files(
            symbol=sym,
            date=date,
            hour=hour_part,
            data_type=dt
        )
        
        if not merged_file or not merged_file.exists():
            raise HTTPException(404, f"No data available for {sym}/{dt} at {target_hour}")
        
        table = pq.read_table(merged_file)
        df = table.to_pandas().head(limit)
        
        # Добавляем читаемые timestamp
        if 'timestamp_ms' in df.columns:
            df['timestamp_utc'] = df['timestamp_ms'].apply(
                lambda x: datetime.fromtimestamp(x / 1000, tz=timezone.utc).isoformat()
            )
        
        if 'trade_time_ms' in df.columns:
            df['trade_time_utc'] = df['trade_time_ms'].apply(
                lambda x: datetime.fromtimestamp(x / 1000, tz=timezone.utc).isoformat()
            )
        
        return {
            "exchange": exch,
            "symbol": sym,
            "data_type": dt,
            "hour": target_hour,
            "rows_returned": len(df),
            "total_rows": len(table),
            "data": df.to_dict(orient='records')
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Error reading data: {str(e)}")
    
    finally:
        # Удаляем временный файл
        if merged_file and merged_file.exists():
            try:
                merged_file.unlink()
            except Exception as e:
                print(f"Warning: Failed to delete temp file {merged_file}: {e}")


@router.get("/data/hours")
async def list_available_hours(
    exchange: str = Query(..., description="Exchange name"),
    symbol: str = Query(..., description="Symbol")
):
    """
    Список доступных часов для символа.
    
    Returns:
        {
            "exchange": "binance",
            "symbol": "btcusdt",
            "available_hours": ["20250120_10", "20250120_11", ...],
            "total_hours": 5,
            "data_types": {
                "20250120_14": ["trades", "bookticker", "depth"],
                ...
            }
        }
    """
    exch = exchange.lower()
    sym = symbol.lower()
    
    if exch not in ["binance", "gate"]:
        raise HTTPException(400, "Exchange must be 'binance' or 'gate'")
    
    symbol_dir = Path("collected_data") / exch / sym
    
    if not symbol_dir.exists():
        return {
            "exchange": exch,
            "symbol": sym,
            "available_hours": [],
            "total_hours": 0,
            "data_types": {}
        }
    
    hours = []
    data_types_by_hour = {}
    
    for hour_dir in sorted(symbol_dir.iterdir()):
        if not hour_dir.is_dir():
            continue
        
        hour_key = hour_dir.name
        hours.append(hour_key)
        
        # Проверяем какие типы данных есть (смотрим на conn_* файлы)
        available_types = []
        for dt in ["trades", "bookticker", "depth"]:
            conn_files = list(hour_dir.glob(f"conn_*_{dt}.parquet"))
            if conn_files:
                available_types.append(dt)
        
        data_types_by_hour[hour_key] = available_types
    
    return {
        "exchange": exch,
        "symbol": sym,
        "available_hours": hours,
        "total_hours": len(hours),
        "data_types": data_types_by_hour
    }


@router.get("/data/stats")
async def data_statistics(
    request: Request,
    exchange: str = Query(..., description="Exchange name"),
    symbol: str = Query(..., description="Symbol"),
    hour: Optional[str] = Query(None, description="Hour (default: current)")
):
    """
    Детальная статистика по данным за час.
    Создает временные merged файлы для расчета статистики и удаляет их.
    
    Returns:
        {
            "trades": {
                "total_rows": 12345,
                "file_size_kb": 456.78,
                "time_range": {...},
                "price_range": {...},
                "volume_stats": {...}
            },
            ...
        }
    """
    exch = exchange.lower()
    sym = symbol.lower()
    
    if exch not in ["binance", "gate"]:
        raise HTTPException(400, "Exchange must be 'binance' or 'gate'")
    
    # Получаем нужный CloudManager
    if exch == "binance":
        cloud_manager = request.app.state.manager_binance
    else:
        cloud_manager = request.app.state.manager_gate
    
    target_hour = hour if hour else datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    date, hour_part = target_hour.split('_')
    
    data_dir = Path("collected_data") / exch / sym / target_hour
    
    if not data_dir.exists():
        raise HTTPException(404, f"Hour directory not found: {target_hour}")
    
    stats = {}
    merged_files = []
    
    try:
        for dt in ["trades", "bookticker", "depth"]:
            # Создаем временный merged файл
            merged_file = await cloud_manager.merge_parquet_files(
                symbol=sym,
                date=date,
                hour=hour_part,
                data_type=dt
            )
            
            if not merged_file or not merged_file.exists():
                stats[dt] = {"status": "not_found"}
                continue
            
            merged_files.append(merged_file)
            
            try:
                table = pq.read_table(merged_file)
                df = table.to_pandas()
                
                file_size_kb = merged_file.stat().st_size / 1024
                
                stat = {
                    "total_rows": len(df),
                    "file_size_kb": round(file_size_kb, 2),
                    "time_range": {
                        "start": datetime.fromtimestamp(
                            df['timestamp_ms'].min() / 1000, tz=timezone.utc
                        ).isoformat(),
                        "end": datetime.fromtimestamp(
                            df['timestamp_ms'].max() / 1000, tz=timezone.utc
                        ).isoformat()
                    }
                }
                
                if dt == "trades":
                    stat["price_range"] = {
                        "min": float(df['price'].min()),
                        "max": float(df['price'].max())
                    }
                    stat["volume_stats"] = {
                        "total_qty": float(df['qty'].abs().sum()),
                        "buy_volume": float(df[df['qty'] > 0]['qty'].sum()) if (df['qty'] > 0).any() else 0,
                        "sell_volume": float(df[df['qty'] < 0]['qty'].abs().sum()) if (df['qty'] < 0).any() else 0,
                        "avg_trade_size": float(df['qty'].abs().mean())
                    }
                
                elif dt == "bookticker":
                    stat["spread_stats"] = {
                        "avg_spread": float((df['best_ask_price'] - df['best_bid_price']).mean()),
                        "min_spread": float((df['best_ask_price'] - df['best_bid_price']).min()),
                        "max_spread": float((df['best_ask_price'] - df['best_bid_price']).max())
                    }
                
                stats[dt] = stat
                
            except Exception as e:
                stats[dt] = {"status": "error", "error": str(e)}
        
        return {
            "exchange": exch,
            "symbol": sym,
            "hour": target_hour,
            "statistics": stats
        }
    
    finally:
        # Удаляем все временные merged файлы
        for merged_file in merged_files:
            if merged_file and merged_file.exists():
                try:
                    merged_file.unlink()
                except Exception as e:
                    print(f"Warning: Failed to delete temp file {merged_file}: {e}")