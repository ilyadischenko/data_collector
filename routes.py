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
    date_to: str = Query(..., description="YYYYMMDD")
):
    """
    Получить распаршенные данные (Trades + BookTicker) за период.
    Работает в отдельном потоке, не блокирует сбор данных.
    """
    data = await service.fetch_data(
        exchange.lower(), 
        symbol.lower(), 
        date_from, 
        date_to
    )
    
    if not data:
        return {"count": 0, "data": [], "msg": "No data found for this range"}
        
    return {
        "exchange": exchange,
        "symbol": symbol,
        "count": len(data),
        "data": data
    }