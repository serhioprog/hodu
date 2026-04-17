import asyncio
from src.services.geo_matcher import geo_matcher
from loguru import logger

async def test():
    # Координаты одной из вилл из твоего списка
    test_lat, test_lng = 40.0059436, 23.4151339
    
    logger.info(f"🧪 Тестируем поиск для координат: {test_lat}, {test_lng}...")
    result = await geo_matcher.find_best_match(lat=test_lat, lng=test_lng, area_name="Test")
    
    print("\n--- РЕЗУЛЬТАТ ТЕСТА ---")
    print(f"Префектура:  {result['prefecture']}")
    print(f"Муниципалитет: {result['municipality']}")
    print(f"Район:       {result['exact_district']}")
    print("-----------------------\n")

if __name__ == "__main__":
    asyncio.run(test())