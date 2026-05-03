import hashlib
from typing import Optional
from loguru import logger

from src.models.ai_schemas import CanonicalFacts
from src.models.domain import Property

class PropertyCanonicalizer:
    """
    Строит плотную, фактическую строку для эмбеддинга.
    БЕЗ цены (она меняется → иначе пришлось бы пересчитывать эмбеддинг на каждое price-changed).
    Стабильная нормализация → дает стабильный SHA256, что позволяет пропускать повторную векторизацию.
    """

    @staticmethod
    def _norm(val: Optional[str]) -> str:
        return (val or "").strip().lower()

    @staticmethod
    def _num(val: Optional[float | int]) -> str:
        if val is None:
            return "?"
        if isinstance(val, float):
            return f"{val:.1f}"
        return str(val)

    @classmethod
    def build_text(cls, facts: CanonicalFacts) -> str:
        """Canonical text (stable ordering, lowercase, фиксированный формат)."""
        # Только true-фичи, отсортированные по ключу → стабильный порядок
        true_features = sorted(k for k, v in (facts.features or {}).items() if v)

        parts = [
            f"category: {cls._norm(facts.category) or 'unknown'}",
            f"prefecture: {cls._norm(facts.calc_prefecture) or 'unknown'}",
            f"municipality: {cls._norm(facts.calc_municipality) or 'unknown'}",
            f"area: {cls._norm(facts.calc_area) or 'unknown'}",
            f"size_sqm: {cls._num(facts.size_sqm)}",
            f"land_size_sqm: {cls._num(facts.land_size_sqm)}",
            f"bedrooms: {cls._num(facts.bedrooms)}",
            f"bathrooms: {cls._num(facts.bathrooms)}",
            f"year_built: {cls._num(facts.year_built)}",
            f"levels: {cls._norm(facts.levels) or 'unknown'}",
            f"features: {', '.join(true_features) if true_features else 'none'}",
            f"phashes: {','.join(facts.image_phashes) if facts.image_phashes else 'none'}",
        ]
        return " | ".join(parts)

    @classmethod
    def from_property(cls, prop: Property) -> CanonicalFacts:
        # Pydantic ждет только True/False, а скрапер иногда приносит строки ("Forest", "2")
        # Поэтому мы принудительно конвертируем всё в безопасный bool:
        safe_features = {}
        for k, v in (prop.extra_features or {}).items():
            if isinstance(v, bool):
                safe_features[k] = v
            else:
                # Если значение есть (не пустое и не 0) - считаем фичу присутствующей (True)
                safe_features[k] = bool(v and str(v).lower() not in ['false', 'no', '0', 'none'])

        return CanonicalFacts(
            category=prop.category,
            calc_prefecture=prop.calc_prefecture,
            calc_municipality=prop.calc_municipality,
            calc_area=prop.calc_area,
            size_sqm=prop.size_sqm,
            land_size_sqm=prop.land_size_sqm,
            bedrooms=prop.bedrooms,
            bathrooms=prop.bathrooms,
            year_built=prop.year_built,
            levels=prop.levels,
            features=safe_features,
            image_phashes=prop.image_phashes,
        )

    @classmethod
    def hash_text(cls, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @classmethod
    def canonicalize(cls, facts: CanonicalFacts) -> tuple[str, str]:
        """Возвращает (canonical_text, sha256)."""
        text = cls.build_text(facts)
        return text, cls.hash_text(text)