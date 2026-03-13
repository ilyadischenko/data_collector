"""
Конфигурация системы сбора данных Binance через CCXT
"""

from dataclasses import dataclass


@dataclass
class BinanceConfig:
    """Конфигурация Binance через CCXT"""
    
    # Лимиты подписок для Binance
    SUBSCRIPTION_LIMIT = 200  # Максимум подписок на одно соединение
    
    # Настройки буферов
    TRADES_BUFFER_SIZE = 50_000
    DEPTH_BUFFER_SIZE = 25_000
    
    # Flush настройки
    FLUSH_INTERVAL_MIN = 5  # Минимальный интервал flush (секунд)
    FLUSH_INTERVAL_MAX = 10  # Максимальный интервал flush (секунд)
    EMERGENCY_FLUSH_THRESHOLD = 500_000  # Принудительный flush при превышении
    
    # Depth настройки
    DEPTH_LIMIT = 20  # Количество уровней стакана
    DEPTH_UPDATE_INTERVAL = 0.1  # 100ms между обновлениями


@dataclass
class StorageConfig:
    """Конфигурация хранения данных"""
    
    # Локальное хранилище
    LOCAL_DIR = "collected_data"
    
    # Облачное хранилище
    CLOUD_PROVIDER = "s3"  # s3, gcs, r2
    BUCKET = "hft-data"
    
    # Компрессия
    PARQUET_COMPRESSION = "zstd"
    PARQUET_COMPRESSION_LEVEL = 12
    GZIP_ENABLED = True
    GZIP_LEVEL = 9
    
    # CloudManager
    HOUR_CHANGE_DELAY = 90  # Секунд задержки после смены часа
    MAX_WORKERS = 2  # ThreadPool для I/O операций


@dataclass
class MonitoringConfig:
    """Конфигурация мониторинга"""
    
    HEALTH_CHECK_INTERVAL = 30  # Секунд
    MEMORY_CHECK_INTERVAL = 30  # Секунд
    
    # Пороги для алертов
    MEMORY_WARNING_PERCENT = 60
    MEMORY_CRITICAL_PERCENT = 80


@dataclass
class SystemConfig:
    """Общая конфигурация системы"""
    
    binance: BinanceConfig
    storage: StorageConfig
    monitoring: MonitoringConfig
    
    @classmethod
    def default(cls) -> 'SystemConfig':
        """Дефолтная конфигурация"""
        return cls(
            binance=BinanceConfig(),
            storage=StorageConfig(),
            monitoring=MonitoringConfig()
        )


# Глобальная конфигурация
CONFIG = SystemConfig.default()