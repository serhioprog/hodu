"""
Classical ML scorer (Phase 3 of Pass 4 bake-off).

LogisticRegression with class_weight="balanced" over engineered features.
Trained on 72 train pairs only; thresholds tuned via 5-fold CV.

Spec hard rules (cross-source, canonical, year_diff, feedback) are
applied BEFORE the ML scorer — same Tier 0 prelude as rule_based and
llm_tier — so the ML model only sees pairs that pass the hard rules.
This keeps the bake-off apples-to-apples.

Lifecycle:
  - At first invocation, score_pair() raises if model not yet trained.
  - bake_off.py calls `await prepare(session)` once before evaluate().
  - prepare() loads train pairs, fetches features, fits model, runs CV,
    tunes high/low thresholds, prints feature importances.

No DB writes. No LLM. CPU-only inference (~0.1 ms/pair after fit).
"""
from __future__ import annotations

import time
from typing import Any

import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.services.engine_v2.config import thresholds as T
from src.services.engine_v2.evaluation import LabeledPair, PairResult, load_train
from src.services.engine_v2.features import PairFeatures, fetch_pair_with_features


# =============================================================
# Feature engineering
# =============================================================

FEATURE_NAMES = [
    "cosine_sim",
    "gps_distance_m",
    "year_diff",
    "price_diff_pct",
    "size_diff_pct",
    "shared_phash_count",
    "same_canonical_category",
    "bedrooms_match_state",        # -1 unknown, 0 differ, 1 match
    "same_municipality_state",     # -1 unknown, 0 differ, 1 match
    "same_calc_area_state",        # -1 unknown, 0 differ, 1 match
    "src_gl_greek",                # one-hot
    "src_gl_real",
    "src_greek_real",
    "src_same_source",             # should be ~0 after Tier 0 same-source filter
]


def _tri_state(b: bool | None) -> int:
    if b is None:
        return -1
    return 1 if b else 0


def features_to_row(f: PairFeatures) -> list[float]:
    """Convert PairFeatures to a numeric row matching FEATURE_NAMES."""
    a, b = sorted([f.a_source, f.b_source])
    src_gl_greek = int(a == "glrealestate.gr" and b == "greekexclusiveproperties.com")
    src_gl_real = int(a == "glrealestate.gr" and b == "realestatecenter.gr")
    src_greek_real = int(
        a == "greekexclusiveproperties.com" and b == "realestatecenter.gr"
    )
    src_same_source = int(f.a_source == f.b_source)
    return [
        f.cosine_sim if f.cosine_sim is not None else float("nan"),
        f.gps_distance_m if f.gps_distance_m is not None else float("nan"),
        float(f.year_diff) if f.year_diff is not None else float("nan"),
        f.price_diff_pct if f.price_diff_pct is not None else float("nan"),
        f.size_diff_pct if f.size_diff_pct is not None else float("nan"),
        float(f.shared_phash_count),
        int(f.same_canonical_category),
        _tri_state(f.bedrooms_match),
        _tri_state(f.same_municipality),
        _tri_state(f.same_calc_area),
        src_gl_greek, src_gl_real, src_greek_real, src_same_source,
    ]


# =============================================================
# Hard-rule prelude (mirrors rule_based / llm_tier)
# =============================================================

def _hard_rule_verdict(f: PairFeatures) -> tuple[str, str] | None:
    if f.pair_in_feedback:
        return ("different", "hard: pair in ai_duplicate_feedbacks (spec §3.4)")
    if not f.cross_source:
        return ("different", f"hard: same source_domain ({f.a_source})")
    if (
        f.canonical_category_a != f.canonical_category_b
        and not f.canonical_category_a.startswith("unknown")
        and not f.canonical_category_b.startswith("unknown")
    ):
        return (
            "different",
            f"hard: canonical category mismatch "
            f"({f.canonical_category_a} vs {f.canonical_category_b})",
        )
    if f.year_diff is not None and f.year_diff > T.YEAR_DIFF_DETERMINISTIC_DIFFERENT:
        return ("different", f"hard: year_built diff {f.year_diff} > 5")
    return None


# =============================================================
# Lifecycle: model singleton + prepare()
# =============================================================

_MODEL: Pipeline | None = None
_THRESHOLD_HIGH: float = T.ML_UNCERTAIN_BAND_HIGH   # default 0.75
_THRESHOLD_LOW: float = T.ML_UNCERTAIN_BAND_LOW     # default 0.35


def _build_pipeline() -> Pipeline:
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(
            class_weight="balanced",
            random_state=T.ML_RANDOM_SEED,
            max_iter=1000,
        )),
    ])


def _tune_thresholds_via_cv(X: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    """
    5-fold CV: out-of-fold probas, then grid-search (low, high) thresholds
    to maximise coverage subject to per-side precision >= floor.
    """
    skf = StratifiedKFold(n_splits=T.ML_CV_FOLDS, shuffle=True, random_state=T.ML_RANDOM_SEED)
    oof_probas = np.zeros(len(y))
    for train_idx, val_idx in skf.split(X, y):
        fold = _build_pipeline()
        fold.fit(X[train_idx], y[train_idx])
        oof_probas[val_idx] = fold.predict_proba(X[val_idx])[:, 1]

    # Grid-search thresholds. We want:
    #   high: minimum cutoff for DUPLICATE such that precision >= 0.95
    #         (spec §6 floor) — among out-of-fold probas
    #   low:  maximum cutoff for DIFFERENT such that 1-precision-on-different >= 0.95
    # Coverage = pairs not in [low, high] band (i.e., engine emits hard verdict).
    best = {"high": T.ML_UNCERTAIN_BAND_HIGH, "low": T.ML_UNCERTAIN_BAND_LOW,
            "coverage": 0.0, "p_dup": 0.0, "p_diff": 0.0}
    grid = np.arange(0.05, 1.0, 0.05)
    for high in grid:
        if high < 0.5:
            continue
        for low in grid:
            if low > high:
                continue
            pred_dup = oof_probas >= high
            pred_diff = oof_probas < low
            tp = int(((y == 1) & pred_dup).sum())
            fp = int(((y == 0) & pred_dup).sum())
            tn = int(((y == 0) & pred_diff).sum())
            fn_neg = int(((y == 1) & pred_diff).sum())
            p_dup = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            p_diff = tn / (tn + fn_neg) if (tn + fn_neg) > 0 else 0.0
            coverage = (pred_dup.sum() + pred_diff.sum()) / len(y)
            # Require precision floor on duplicate side per spec §6 floor (0.95)
            if p_dup < T.SPEC_PRECISION_FLOOR:
                continue
            # Prefer wider coverage; tie-break on higher dup precision then F1ish
            if coverage > best["coverage"] or (
                coverage == best["coverage"] and p_dup > best["p_dup"]
            ):
                best = {"high": float(high), "low": float(low),
                        "coverage": coverage, "p_dup": p_dup, "p_diff": p_diff}
    return best["low"], best["high"]


async def prepare(session) -> dict[str, Any]:
    """Fit the LogisticRegression model on the 72 train pairs and tune
    decision thresholds via 5-fold CV. Idempotent."""
    global _MODEL, _THRESHOLD_HIGH, _THRESHOLD_LOW

    print("[classical_ml] Loading train pairs and fetching features...")
    train_pairs = load_train()
    X_rows: list[list[float]] = []
    y_vals: list[int] = []
    skipped = 0
    for p in train_pairs:
        f = await fetch_pair_with_features(session, p.property_a_id, p.property_b_id)
        if f is None:
            skipped += 1
            continue
        X_rows.append(features_to_row(f))
        y_vals.append(1 if p.ground_truth == "duplicate" else 0)
    X = np.array(X_rows, dtype=float)
    y = np.array(y_vals, dtype=int)
    print(f"[classical_ml] Train: n={len(y)}  duplicates={int(y.sum())}  "
          f"different={int((y == 0).sum())}  skipped={skipped}")

    pipeline = _build_pipeline()
    pipeline.fit(X, y)
    _MODEL = pipeline

    # Feature importance (positive coef = duplicate-leaning, after standardization)
    coefs = pipeline.named_steps["clf"].coef_[0]
    importance = sorted(zip(FEATURE_NAMES, coefs), key=lambda kv: -abs(kv[1]))
    print("[classical_ml] Feature coefficients (standardized; +duplicate, -different):")
    for name, coef in importance:
        print(f"  {name:30s}  {coef:+.3f}")

    # CV-tuned thresholds
    low_t, high_t = _tune_thresholds_via_cv(X, y)
    _THRESHOLD_LOW = low_t
    _THRESHOLD_HIGH = high_t
    print(f"[classical_ml] Tuned thresholds via 5-fold CV: "
          f"low={low_t:.3f}  high={high_t:.3f}  "
          f"(default were {T.ML_UNCERTAIN_BAND_LOW}/{T.ML_UNCERTAIN_BAND_HIGH})")

    return {
        "n_train": len(y),
        "n_train_duplicates": int(y.sum()),
        "feature_importance": [(n, float(c)) for n, c in importance],
        "threshold_low": low_t,
        "threshold_high": high_t,
    }


# =============================================================
# Scorer
# =============================================================

async def score_pair(pair: LabeledPair, features: PairFeatures) -> PairResult:
    if _MODEL is None:
        raise RuntimeError(
            "classical_ml.score_pair called before prepare(); "
            "bake_off must invoke prepare(session) before evaluate()"
        )
    t0 = time.perf_counter()

    # Tier 0 hard rules (uniform with rule_based / llm_tier)
    hard = _hard_rule_verdict(features)
    if hard is not None:
        verdict, reason = hard
        return PairResult(
            pair_id=pair.id,
            pair_a_id=pair.property_a_id, pair_b_id=pair.property_b_id,
            category=pair.category, ground_truth=pair.ground_truth,
            predicted=verdict, confidence=1.0, cost_usd=0.0,
            latency_ms=(time.perf_counter() - t0) * 1000,
            reasoning=reason,
        )

    # ML prediction
    row = np.array(features_to_row(features), dtype=float).reshape(1, -1)
    proba = float(_MODEL.predict_proba(row)[0, 1])

    if proba >= _THRESHOLD_HIGH:
        verdict = "duplicate"
        confidence = proba
    elif proba < _THRESHOLD_LOW:
        verdict = "different"
        confidence = 1.0 - proba
    else:
        verdict = "uncertain"
        confidence = abs(proba - 0.5) * 2.0  # in [0, 1)

    return PairResult(
        pair_id=pair.id,
        pair_a_id=pair.property_a_id, pair_b_id=pair.property_b_id,
        category=pair.category, ground_truth=pair.ground_truth,
        predicted=verdict, confidence=confidence, cost_usd=0.0,
        latency_ms=(time.perf_counter() - t0) * 1000,
        reasoning=f"ML proba={proba:.3f} thresholds=[{_THRESHOLD_LOW:.2f}, {_THRESHOLD_HIGH:.2f}]",
    )
