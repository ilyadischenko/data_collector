from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from data_service import DataQueryService

router = APIRouter(prefix="/api/data", tags=["Historical Data"])
service = DataQueryService()

@router.get("/fetch")
async def get_historical_data(
    exchange: str = Query(..., description="binance or bybit"),
    symbol: str = Query(..., description="e.g. btcusdt"),
    date_from: str = Query(..., description="YYYYMMDD"),
    date_to: str = Query(..., description="YYYYMMDD"),
    hour_from: int = Query(0, ge=0, le=23, description="Start Hour (0-23)"),
    hour_to: int = Query(23, ge=0, le=23, description="End Hour (0-23)")
):
    """
    Скачать данные с фильтрацией по часам.
    Пример: date_from=20250115, hour_from=10, date_to=20250115, hour_to=11
    Вернет данные за 10:00 и 11:00.
    """
    
    # Валидация, чтобы не пытались скачать год данных за раз
    if date_from != date_to:
        # Если даты разные, просто предупреждение в консоль (или можно ограничить)
        pass
        
    data = await service.fetch_data(
        exchange.lower(), 
        symbol.lower(), 
        date_from, hour_from,
        date_to, hour_to
    )
    
    if not data:
        return {"count": 0, "data": [], "msg": "No data found"}
        
    return {
        "exchange": exchange,
        "symbol": symbol,
        "range": f"{date_from}:{hour_from} - {date_to}:{hour_to}",
        "count": len(data),
        "data": data
    }