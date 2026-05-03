import re
from typing import Dict, Any, Optional
from loguru import logger

# Импортируем наш мозг (Словарь)
from src.core.extraction_dictionary import (
    METRICS_PATTERNS,
    PROPERTY_TYPES,
    EXTRA_FEATURES_PATTERNS,
    SYSTEM_PATTERNS
)

class DataExtractor:
    """
    Универсальный NLP-движок для извлечения данных из сырого текста.
    Использует предварительно скомпилированные регулярные выражения для максимальной производительности (O(1)).
    """
    
    def __init__(self):
        # Компилируем все паттерны при инициализации для ускорения последующих вызовов
        self.compiled_metrics = self._compile_patterns(METRICS_PATTERNS)
        self.compiled_types = self._compile_patterns(PROPERTY_TYPES)
        self.compiled_features = self._compile_patterns(EXTRA_FEATURES_PATTERNS)
        self.compiled_system = self._compile_patterns(SYSTEM_PATTERNS)
        logger.debug("🧠 DataExtractor инициализирован: все Regex-паттерны скомпилированы.")

    @staticmethod
    def _compile_patterns(pattern_dict: Dict[str, list]) -> Dict[str, list]:
        """Вспомогательный метод для компиляции словаря регулярных выражений"""
        compiled = {}
        for key, patterns in pattern_dict.items():
            compiled[key] = [re.compile(p, re.IGNORECASE) for p in patterns]
        return compiled

    @staticmethod
    def _clean_number(text: str) -> Optional[float]:
        """Умная очистка любых числовых значений (цены, площади)"""
        if not text: 
            return None
        raw_num = text.strip()
        # Отрезаем европейские нули на конце (e.g., ,00 или .00)
        if raw_num.endswith(',00') or raw_num.endswith('.00'):
            raw_num = raw_num[:-3]
        # Убираем все разделители
        clean_num = re.sub(r'[.,]', '', raw_num)
        try:
            return float(clean_num)
        except ValueError:
            return None

    def extract_metrics(self, text: str) -> Dict[str, Any]:
        """Извлекает числа (площадь, спальни, год и т.д.)"""
        results = {}
        for key, patterns in self.compiled_metrics.items():
            results[key] = None  # Значение по умолчанию
            for pattern in patterns:
                match = pattern.search(text)
                if match:
                    val_str = match.group(1)
                    
                    # Приводим типы данных согласно нашей Pydantic схеме
                    if key in ["bedrooms", "bathrooms", "year_built"]:
                        clean_val = self._clean_number(val_str)
                        if clean_val is not None:
                            results[key] = int(clean_val)
                    elif key == "levels":
                        # Уровни всегда оставляем строкой (фикс для Pydantic)
                        results[key] = str(val_str).strip()
                    else:
                        # Площади и расстояния (float)
                        results[key] = self._clean_number(val_str)
                        
                    break  # Если нашли паттерн, остальные для этого ключа не проверяем
        return results

    def extract_type(self, text: str) -> Optional[str]:
        """
        Определяет категорию недвижимости (Villa, Apartment, Hotel/Commercial...).

        Strategy: COUNT-BASED scoring (was: first-match-wins).

        Why count-based:
          • A villa description may contain a single mention of "hotel"
            (e.g. "5 minutes from the resort hotel") which would falsely
            classify it under Hotel/Commercial with first-match-wins.
          • Conversely, a hotel listing's body usually mentions "hotel"
            many times (5-10) — count clearly wins over a stray mention.
          • Land/Plot listings would otherwise be eaten by villa
            descriptions that mention "land plot: 3500m²" first.

        Tiebreaker: when scores are equal, the EARLIER category in the
        PROPERTY_TYPES dict wins. This is why Hotel/Commercial is listed
        first — it's the most-specific type and we want it to win on ties.
        """
        scores: Dict[str, int] = {}
        for prop_type, patterns in self.compiled_types.items():
            hits = 0
            for pattern in patterns:
                hits += len(pattern.findall(text))
            if hits > 0:
                scores[prop_type] = hits

        if not scores:
            return None

        # Sort: highest count first; ties broken by insertion order
        # (which is preserved in Python 3.7+ dicts).
        ordered_types = list(self.compiled_types.keys())
        return max(
            scores.items(),
            key=lambda kv: (kv[1], -ordered_types.index(kv[0])),
        )[0]

    def extract_features(self, text: str) -> Dict[str, bool]:
        """Сканирует текст на наличие удобств и возвращает JSON-ready словарь"""
        features = {}
        for feature_key, patterns in self.compiled_features.items():
            for pattern in patterns:
                if pattern.search(text):
                    features[feature_key] = True
                    break
        return features

    def extract_system_data(self, text: str) -> Dict[str, str]:
        """Вытягивает ID и даты обновления"""
        sys_data = {}
        for key, patterns in self.compiled_system.items():
            for pattern in patterns:
                match = pattern.search(text)
                if match:
                    sys_data[key] = match.group(1).strip()
                    break
        return sys_data

    def analyze_full_text(self, text: str) -> Dict[str, Any]:
        """
        🔥 Главный метод (Facade): Прогоняет сырой текст через все движки разом.
        Возвращает полностью структурированный словарь для обновления PropertyTemplate.
        """
        if not text:
            return {}
            
        data = self.extract_metrics(text)
        
        # Добавляем категорию
        category = self.extract_type(text)
        if category:
            data["category"] = category
            
        # Добавляем системные данные (если есть)
        sys_info = self.extract_system_data(text)
        if "site_property_id" in sys_info:
            data["site_property_id"] = sys_info["site_property_id"]
            
        # Заливаем фичи в отдельный ключ
        data["extra_features"] = self.extract_features(text)
        
        return data