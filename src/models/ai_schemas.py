from typing import Optional, List
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field, ConfigDict

# === Canonical representation ===
class CanonicalFacts(BaseModel):
    """Факты объекта для построения canonical_text и генерации эмбеддинга."""
    category: Optional[str] = None
    calc_prefecture: Optional[str] = None
    calc_municipality: Optional[str] = None
    calc_area: Optional[str] = None
    size_sqm: Optional[float] = None
    land_size_sqm: Optional[float] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    year_built: Optional[int] = None
    levels: Optional[str] = None
    features: dict[str, bool] = Field(default_factory=dict)
    image_phashes: list[str] = Field(default_factory=list)

# === Internal matcher candidate ===
class DuplicateCandidatePair(BaseModel):
    prop_a: UUID
    prop_b: UUID
    similarity: float
    phashes_a: list[str] = Field(default_factory=list)
    phashes_b: list[str] = Field(default_factory=list)

class VisionVerdict(BaseModel):
    """
    GPT-4o Vision verdict on whether two property listings depict the
    same physical property.

    Used by the InternalDuplicateDetector as a tie-breaker for the
    gray zone (text similarity 0.92-0.985 without pHash confirmation).

    The model receives 3 photos from each listing and outputs JSON
    matching this schema.
    """
    is_same: bool = Field(
        description=(
            "True if the photos depict the SAME physical property "
            "(same building/villa/plot, possibly photographed at different "
            "times or from different angles). False if they are clearly "
            "different physical properties (different buildings, different "
            "plots, different architecture)."
        )
    )
    confidence: float = Field(
        ge=0.0, le=1.0,
        description=(
            "Confidence in the verdict, 0.0 to 1.0. "
            "Use < 0.7 if you cannot reliably tell from the photos "
            "(e.g. mostly stock photos, unclear angles, similar but not identical)."
        )
    )
    reason: str = Field(
        description=(
            "Brief explanation (1-2 sentences) of what visual cues "
            "led to the verdict. Mention specific architectural details, "
            "surroundings, or photo content."
        )
    )

# === External API DTO (generic shape, adapter maps provider's JSON here) ===
class ExternalPropertyDTO(BaseModel):
    external_id: str
    external_source: str
    category: Optional[str] = None
    calc_municipality: Optional[str] = None
    calc_area: Optional[str] = None
    size_sqm: Optional[float] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    description: Optional[str] = None
    features: dict[str, bool] = Field(default_factory=dict)
    image_phashes: list[str] = Field(default_factory=list)
    raw: dict = Field(default_factory=dict)

# === Power Object AI output (enforced via OpenAI structured output) ===
class PowerPropertySynthesis(BaseModel):
    """Строгий output gpt-4o. response_format=PowerPropertySynthesis."""
    model_config = ConfigDict(extra="forbid")

    description: str = Field(
        ...,
        description=(
            "Единое факт-ориентированное описание объекта, 2-4 абзаца, "
            "на английском. Без маркетинговых клише ('stunning', 'dream home'), "
            "без упоминания цены, без упоминания источников/агентств."
        ),
    )
    features: dict[str, bool] = Field(
        ...,
        description="Дедуплицированный набор удобств. Только true-значения."
    )


# =====================================================================
# === LLM EXTRACTION (per-scraper amenity extraction from descriptions)
# =====================================================================
class GreekPropertyExtraction(BaseModel):
    """
    Structured extraction from a Greek real estate listing description.

    Used as response_format for OpenAI structured outputs (gpt-4o-mini),
    so the model is GUARANTEED to return valid JSON matching this schema.
    No hallucinated fields, no malformed output, no prompt-injection escape.

    Design principles:
      • Booleans: True if feature is EXPLICITLY present in the text.
                  None if NOT mentioned. Never use False — "absence of
                  evidence is not evidence of absence" for amenities.
      • Numerics: integer only when an exact value is stated. None otherwise.
      • Categoricals: copy the exact value from the description, no
                      paraphrasing.
      • other_features: free-form catch-all for things not in schema.
                        Cap at 10 items × 5 words to limit prompt-injection
                        attack surface.

    All field names are lowercase snake_case to match the rest of
    Property.extra_features convention.
    """
    model_config = ConfigDict(extra="forbid")

    # ─── BOOLEANS: amenities (most common on Halkidiki luxury listings) ──
    sea_view:          Optional[bool] = None
    pool:              Optional[bool] = None
    jacuzzi:           Optional[bool] = None
    sauna:             Optional[bool] = None
    hammam:            Optional[bool] = None
    solarium:          Optional[bool] = None
    fireplace:         Optional[bool] = None
    home_cinema:       Optional[bool] = None
    air_conditioning:  Optional[bool] = None
    heating:           Optional[bool] = None
    alarm_system:      Optional[bool] = None
    elevator:          Optional[bool] = None
    garden:            Optional[bool] = None
    bbq:               Optional[bool] = None
    parking:           Optional[bool] = None
    furnished:         Optional[bool] = None
    beachfront:        Optional[bool] = None
    guest_house:       Optional[bool] = None
    wifi:              Optional[bool] = None

    # ─── NUMERIC VALUES (when stated explicitly) ────────────────────────
    floors:              Optional[int] = Field(None, description="Number of floors / storeys")
    distance_from_sea_m: Optional[int] = Field(None, description="Meters from sea (only if stated)")
    pool_size_sqm:       Optional[int] = Field(None, description="Swimming pool surface in sqm")
    parking_spots:       Optional[int] = Field(None, description="Number of parking spaces")

    # ─── HOTEL / COMMERCIAL specifics ───────────────────────────────────
    rooms_count:      Optional[int] = Field(None, description="Total rooms (hotel listings)")
    beds_count:       Optional[int] = Field(None, description="Total beds (hotel listings)")
    buildings_count:  Optional[int] = Field(None, description="Multi-building plots")

    # ─── CATEGORICAL (open-ended but constrained) ───────────────────────
    energy_class: Optional[str] = Field(
        None,
        description="EU energy efficiency rating: A, B, C, D, E, F, G. Only if stated.",
    )
    condition: Optional[str] = Field(
        None,
        description=(
            "Condition stated in listing: e.g. 'Furnished', 'Unfurnished', "
            "'Renovated', 'New', 'Needs renovation'. Only if stated."
        ),
    )
    orientation: Optional[str] = Field(
        None,
        description="Compass orientation (e.g. 'South', 'South-East'). Only if stated.",
    )

    # ─── FREE-FORM CATCH-ALL ────────────────────────────────────────────
    other_features: List[str] = Field(
        default_factory=list,
        description=(
            "Notable features not in the schema. Each item is a short noun "
            "phrase (max 5 words). Maximum 10 items total. Examples: "
            "'wine cellar', 'helipad', 'tennis court', 'private cinema room'."
        ),
    )

    def to_extra_features(self) -> dict[str, bool | int | str]:
        """
        Flatten this structured extraction into the flat dict format expected
        by Property.extra_features (snake_case keys, primitive values).

        - Booleans: only included if True (we don't store False — absence
          is implied).
        - Numerics: included if not None.
        - Categoricals: included if not None.
        - other_features: each item becomes a key with True value
          (e.g. 'wine_cellar' -> True), normalised to snake_case.
        """
        result: dict = {}

        # Booleans (only True ones)
        bool_fields = [
            "sea_view", "pool", "jacuzzi", "sauna", "hammam", "solarium",
            "fireplace", "home_cinema", "air_conditioning", "heating",
            "alarm_system", "elevator", "garden", "bbq", "parking",
            "furnished", "beachfront", "guest_house", "wifi",
        ]
        for f in bool_fields:
            v = getattr(self, f, None)
            if v is True:
                result[f] = True

        # Numerics
        for f in ("floors", "distance_from_sea_m", "pool_size_sqm",
                  "parking_spots", "rooms_count", "beds_count",
                  "buildings_count"):
            v = getattr(self, f, None)
            if v is not None:
                result[f] = int(v)

        # Categoricals
        for f in ("energy_class", "condition", "orientation"):
            v = getattr(self, f, None)
            if v:
                result[f] = str(v).strip()

        # other_features → snake_case keys with True
        for raw in (self.other_features or [])[:10]:  # hard cap 10
            if not raw:
                continue
            key = (
                str(raw)
                .strip()
                .lower()
                .replace("-", " ")
                .replace("/", " ")
                .replace(",", " ")
            )
            # Collapse whitespace, snake_case
            key = "_".join(key.split())[:50]  # cap key length
            if key and key not in result:
                result[key] = True

        return result