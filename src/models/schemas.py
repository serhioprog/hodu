import re
from typing import Optional, List
from pydantic import BaseModel, field_validator

class PropertyTemplate(BaseModel): # Это и есть наш "Шаблон"
    # Обязательные технические поля
    site_property_id: str
    source_domain: str
    url: str
    
    # Поля недвижимости (Все Optional, чтобы скрипт не падал, если чего-то нет)
    price: Optional[int] = None
    size_sqm: Optional[float] = None
    land_size_sqm: Optional[float] = None  # Новое: площадь участка
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    year_built: Optional[int] = None
    location_raw: Optional[str] = None
    area: Optional[str] = None             # Новое: Район
    subarea: Optional[str] = None          # Новое: Подрайон
    category: Optional[str] = None         # Новое: Тип (Вилла/Апартаменты)
    levels: Optional[str] = None           # Новое: Уровни/Этажность
    description: Optional[str] = ""        # Новое: Полное описание
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    site_last_updated: Optional[str] = None # Новое: Дата обновления на сайте
    status: str = "ACTIVE"
    images: List[str] = []

    # ЛОГИКА ФИЛЬТРАЦИИ (Чистим данные прямо при заполнении шаблона)-----------
    
    @field_validator('price', mode='before')
    def clean_price(cls, v):
        if not v: return None
        if isinstance(v, int): return v
        
        # Превращаем '1.100.000€ 1.000.000€' в '1.100.000 1.000.000'
        text = str(v).replace('€', '').replace('£', '').replace('$', '')
        
        # Ищем все группы цифр (с точками или запятыми)
        # Например: ['1.100.000', '1.000.000']
        prices = re.findall(r'[\d.,]+', text)
        
        if not prices: 
            return None
            
        # Берем ПОСЛЕДНЮЮ найденную цену
        last_price_str = prices[-1]
        
        # Теперь удаляем точки и запятые, чтобы получить чистое число
        clean_num = re.sub(r'[^\d]', '', last_price_str)
        
        try:
            return int(clean_num)
        except ValueError:
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
            return float(str(v).strip())
        except ValueError:
            return None