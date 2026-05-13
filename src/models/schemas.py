import re
from typing import Optional, List

from loguru import logger
from pydantic import BaseModel, Field, field_validator

class PropertyTemplate(BaseModel): # Это и есть наш "Шаблон"
    # Обязательные технические поля
    site_property_id: str
    source_domain: str
    url: str
    levels: str | None = None
    
    # Поля недвижимости (Все Optional, чтобы скрипт не падал, если чего-то нет)
    price: Optional[int] = None
    size_sqm: Optional[float] = None
    land_size_sqm: Optional[float] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    year_built: Optional[int] = None
    location_raw: Optional[str] = None
    area: Optional[str] = None
    subarea: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    site_last_updated: Optional[str] = None
    status: Optional[str] = None
    images: List[str] = Field(default_factory=list)
    
    #лОКАЦИИ АРИСА
    location_id: Optional[int] = None
    calc_prefecture: Optional[str] = None
    calc_municipality: Optional[str] = None
    calc_area: Optional[str] = None
    
    # Новое поле для любых дополнительных характеристик в виде словаря (например, терраса, бассейн, вид на море и т.д.)
    # Bug #19/#59: use default_factory for mutable defaults; Pydantic v2
    # handles `= {}` correctly (deep-copies on each instance) but
    # default_factory is the explicit/canonical form and matches
    # ai_schemas.py convention. Linters and IDEs are happier with it.
    extra_features: dict = Field(default_factory=dict)

    #Переводим уровни в строку, чтобы избежать проблем с разными форматами----------------------------
    @field_validator('levels', mode='before')
    @classmethod
    def ensure_string(cls, v):
        if v is None: return None
        return str(v) # Автоматически превратит 3 в "3" до сохранения
    #Переводим уровни в строку, чтобы избежать проблем с разными форматами----------------------------

    # ЛОГИКА ФИЛЬТРАЦИИ (Чистим данные прямо при заполнении шаблона)-----------
    @field_validator('price', mode='before')
    @classmethod
    def clean_price(cls, v):
        if not v: return None
        if isinstance(v, (int, float)): return int(v)
        
        # Удаляем символы валют и ПРОБЕЛЫ (чтобы 1 500 000 склеилось в 1500000)
        text = str(v).replace('€', '').replace('£', '').replace('$', '').replace(' ', '')
        
        prices = re.findall(r'[\d.,]+', text)
        
        if not prices: 
            return None
            
        valid_prices = []
        for p in prices:
            p = p.strip()
            
            # 🔥 ИСПРАВЛЕНИЕ: Сначала отсекаем копейки (ровно 1 или 2 цифры после точки/запятой в конце)
            # Например: '1.400.000,00' -> '1.400.000'
            p = re.sub(r'[.,]\d{1,2}$', '', p)
            
            # Теперь безопасно удаляем оставшиеся разделители тысяч
            clean_num_str = re.sub(r'[^\d]', '', p)
            
            if clean_num_str:
                num = int(clean_num_str)
                if num > 1000:
                    valid_prices.append(num)
                    
        if valid_prices:
            # Bug #9: most real-estate listings put the current price first
            # ("€500 000 (was €600 000)" → 500000). Old "was/from" prices
            # follow in parentheses or descriptive text. Pre-fix this returned
            # the LAST number which was usually the OLD price. If you see
            # this warning, the scraper selector may be picking up too much
            # descriptive text — investigate that scraper.
            if len(valid_prices) > 1:
                logger.warning(
                    f"[clean_price] multiple prices in input — taking first. "
                    f"Found: {valid_prices}, raw input: {str(v)[:120]!r}"
                )
            return valid_prices[0]
            
        return None

    @field_validator('size_sqm','land_size_sqm', mode='before')
    def clean_float(cls, v):
        if not v: return None
        if isinstance(v, (int, float)): return float(v)
        # Ищем число в строке '127m2' -> 127.0
        match = re.search(r'(\d+[.,]?\d*)', str(v))
        if match:
            return float(match.group(1).replace(',', '.'))
        return None

    @field_validator('bedrooms', 'bathrooms', 'year_built', mode='before')
    def clean_int(cls, v):
        if not v: return None
        if isinstance(v, int): return v
        # 'Year: 2020' -> 2020
        cleaned = re.sub(r'[^\d]', '', str(v))
        return int(cleaned) if cleaned else None
    
    # метод для очистки координат (если они придут строкой)
    @field_validator('latitude', 'longitude', mode='before')
    def clean_coords(cls, v):
        if not v: return None
        try:
            # Заменяем запятую на точку и убираем лишние символы
            clean_v = str(v).replace(',', '.').strip()
            clean_v = re.sub(r'[^\d.-]', '', clean_v)
            return float(clean_v)
        except ValueError:
            return None
        
        