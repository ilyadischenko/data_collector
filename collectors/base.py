
from typing import List


class BaseCollector:
    def start(self) -> None:
        """
        Запускает сбор данных
        """
        raise NotImplementedError("Метод start должен быть реализован в подклассе")

    def add_symbol(self, symbol: str) -> None:
        """
        Добавляет символ в список отслеживаемых
        """
        raise NotImplementedError("Метод add_symbol должен быть реализован в подклассе")
    
    def remove_symbol(self, symbol: str) -> None:
        """
        Удаляет символ из списка отслеживаемых
        """
        raise NotImplementedError("Метод remove_symbol должен быть реализован в подклассе")
    
    def list_symbols(self) -> List[str]:
        """
        Возвращает список отслеживаемых символов
        """
        raise NotImplementedError("Метод list_symbols должен быть реализован в подклассе")
    