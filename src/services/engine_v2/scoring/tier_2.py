"""
Tier 2 ML scoring backend for engine v2.

Per RESEARCH.md §12.5.10 + Day 3 Phase B architect approval.

Wraps a trained HistGradientBoostingClassifier (Day 3: raw, no
calibration — see train_tier_2.py for the N=72 calibration-collapse
rationale) and emits ScoringBackend Protocol verdicts based on raw
class probabilities + asymmetric thresholds per spec §2.4.

Validation only requires `predict_proba` to exist (HistGB has it
natively); no calibration-wrapper check. Future versions (Day 6+)
may re-introduce calibration when N grows.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import joblib
import numpy as np
from loguru import logger

from src.models.domain import Property

from src.services.engine_v2.config.thresholds import (
    T2_PROB_DIFFERENT_THRESHOLD,
    T2_PROB_DUPLICATE_THRESHOLD,
)

from ..dedup_report import EngineVerdict
from ..features import PairFeatures
from .feature_extraction import FEATURE_NAMES, extract_features


class Tier2MLBackend:
    """ML-based scoring backend for Tier 2.

    Loads a pre-trained CalibratedClassifierCV from disk; emits
    verdicts based on calibrated probabilities + asymmetric thresholds
    (spec §2.4 — high bar for DUPLICATE).

    Stateless after init — model + class indices captured in __init__.
    """

    def __init__(self, model_path: Path) -> None:
        """Load model bundle from joblib pickle.

        Bundle structure (set by training/train_tier_2.py):
            {
                'model': CalibratedClassifierCV,
                'feature_names': list[str],
                'classes': list[str],   # e.g. ['different', 'duplicate']
                'training_metadata': dict,
            }

        Raises:
            FileNotFoundError if model file missing.
            ValueError on validation failure (predict_proba missing,
            feature drift, or unexpected class labels).
        """
        if not model_path.exists():
            raise FileNotFoundError(
                f"Tier 2 model file not found: {model_path}. "
                "Train via experiments/new_engine_v2/training/train_tier_2.py"
            )

        bundle: dict[str, Any] = joblib.load(model_path)
        self._model = bundle["model"]
        self._feature_names_at_train = list(bundle["feature_names"])
        self._classes = list(bundle["classes"])

        if not hasattr(self._model, "predict_proba"):
            raise ValueError(
                "Tier 2 model lacks predict_proba; not a calibrated classifier"
            )
        if self._feature_names_at_train != FEATURE_NAMES:
            raise ValueError(
                f"Feature mismatch: model trained on "
                f"{self._feature_names_at_train}, current "
                f"FEATURE_NAMES={FEATURE_NAMES}"
            )
        if "duplicate" not in self._classes or "different" not in self._classes:
            raise ValueError(
                f"Tier 2 model classes={self._classes} must include "
                f"'duplicate' and 'different'"
            )

        self._idx_duplicate = self._classes.index("duplicate")
        self._idx_different = self._classes.index("different")

        logger.info(
            "[t2_ml] loaded model from {p}, classes={c}, features={n}",
            p=str(model_path), c=self._classes, n=len(FEATURE_NAMES),
        )

    async def score(
        self,
        features: PairFeatures,
        prop_a: Property,
        prop_b: Property,
    ) -> EngineVerdict:
        """Score one pair via ML model.

        Steps:
          1. Extract 13 ML features from Property pair (Day 3 C-1)
          2. Predict calibrated probabilities
          3. Apply asymmetric thresholds (T2_PROB_DUPLICATE/DIFFERENT)
          4. Return EngineVerdict with tier_emitted=2, cost_usd=0.0

        Note: features arg (PairFeatures) is not used — ML uses raw
        Property fields via extract_features. PairFeatures kept in
        signature for Protocol uniformity.
        """
        t0 = time.perf_counter()

        # 1. Extract 13 ML features
        feats = extract_features(prop_a, prop_b)

        # 2. Build numpy X matrix in FEATURE_NAMES order
        X = np.array(
            [[feats[name] for name in FEATURE_NAMES]],
            dtype=np.float32,
        )

        # 3. Predict probabilities
        probs = self._model.predict_proba(X)[0]
        prob_duplicate = float(probs[self._idx_duplicate])
        prob_different = float(probs[self._idx_different])

        # 4. Apply asymmetric thresholds
        if prob_duplicate >= T2_PROB_DUPLICATE_THRESHOLD:
            verdict = "duplicate"
            confidence = prob_duplicate
        elif prob_different >= T2_PROB_DIFFERENT_THRESHOLD:
            verdict = "different"
            confidence = prob_different
        else:
            verdict = "uncertain"
            confidence = max(prob_duplicate, prob_different)

        reasoning = (
            f"T2 ML: prob_dup={prob_duplicate:.3f}, "
            f"prob_diff={prob_different:.3f}, "
            f"emb_cos={feats['embedding_cosine_sim']:.3f}, "
            f"phash_min={feats['phash_min_hamming']:.0f}, "
            f"same_municipality={feats['same_calc_municipality']:.0f}"
        )

        return EngineVerdict(
            verdict=verdict,                       # type: ignore[arg-type]
            confidence=confidence,
            reasoning=reasoning,
            tier_emitted=2,                        # int per Day 1 EngineVerdict schema
            cost_usd=0.0,                          # local model = no API cost
            latency_ms=(time.perf_counter() - t0) * 1000,
        )
