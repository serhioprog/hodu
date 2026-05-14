"""
Single source of truth for engine v2 numeric thresholds.

Every constant here has a comment explaining:
  - what decision it controls,
  - where the value came from (spec §, data audit, Pass-3 measurement,
    or 'tuning candidate' for Phase 1 starting points), and
  - the safety direction (which way is conservative).

If you change a value, update the comment and re-run the bake-off.
NO magic numbers in scoring code — everything imports from here.

Imported by: src/engine.py, src/scoring/*.py.
"""
from __future__ import annotations

# =============================================================
# TIER 0 deterministic filters (spec §6.1, locked Pass 3)
# =============================================================
# These are HARD rules with empirical justification. Do not relax
# without architect approval and re-running the blocking analysis.

YEAR_DIFF_DETERMINISTIC_DIFFERENT: int = 5
"""
Spec §2.3 (revised 2026-05-06): year_built diff > N triggers
deterministic DIFFERENT.
- N=5 sits at the empirically-observed bimodal break in the labeled
  test set (sorted diffs: [0,0,0,0,1,1,2,4,4,5,19,38] — gap from 5 to 19).
- See research/CANDIDATE_GENERATION.md §3.7.2 for the histogram.
- Conservative direction: lower N filters more pairs, may lose recall.
- Higher N admits more pairs, may admit data-quality outliers.
"""

YEAR_DIFF_MISMERGE_FLAG: int = 10
"""
Spec §2.3 (revised): if a pair WITHIN an existing APPROVED cluster has
year_diff > N, flag the cluster as "possible data-quality issue" per
spec §11. Report-only; engine does NOT auto-dissolve.
"""


# =============================================================
# TIER 1 blocking (spec §6.1, locked Pass 3)
# =============================================================
# Same calc_municipality is the locked Tier-1 narrowing filter.
# No GPS fallback (rejected at Pass 3 — premature optimization at N=564).

REQUIRE_SAME_MUNICIPALITY: bool = True
"""
Spec §6.1 / Pass 3 lock: pairs must share calc_municipality to enter
the candidate pool. Coverage is 98.58% on active properties; safe
without NULL fallback.
"""


# =============================================================
# RULE-BASED scoring (Phase 1, tuning candidates)
# =============================================================
# These start as conservative defaults and may be adjusted on the
# train set via 5-fold CV. They are NOT to be tuned on the holdout
# (spec §5.4).
#
# COSINE DECISION MATRIX (used by all 3 cosine-aware architectures)
# ------------------------------------------------------------------
# Three thresholds interact: COSINE_HIGH_DUPLICATE (0.92),
# COSINE_LOW_DIFFERENT (0.50), LLM_PREFILTER_COSINE_HIGH_SKIP (0.95),
# LLM_PREFILTER_COSINE_LOW_SKIP (0.40). Behavior across architectures:
#
#   Cosine band      Rule verdict   LLM pre-filter    Hybrid action
#   [0.95, 1.00]     DUPLICATE      skip (no LLM)     rule emits
#   [0.92, 0.95)     DUPLICATE      call LLM          rule emits
#   [0.50, 0.92)     UNCERTAIN      call LLM          escalate to ML/LLM
#   [0.40, 0.50)     DIFFERENT      call LLM          rule emits
#   [0.00, 0.40)     DIFFERENT      skip (no LLM)     rule emits
#
# - The 0.92 / 0.50 thresholds split rule-based DUPLICATE / UNCERTAIN
#   / DIFFERENT.
# - The 0.95 / 0.40 thresholds gate LLM cost: skip the LLM call when
#   the rule layer is confident enough that the LLM is unlikely to
#   change the verdict.
# - In hybrid mode, rule-confident bands emit directly; only the
#   middle band [0.50, 0.92) escalates to ML and (if still uncertain)
#   to LLM.
# - If you change any of these four thresholds, re-walk this matrix
#   and re-run the train bake-off. Architectures use the same numbers.

COSINE_HIGH_DUPLICATE: float = 0.92
"""
Rule-based: cosine_sim >= this AND other signals agree -> DUPLICATE.
- Starting point informed by Pass 2 truly_uncertain finder band
  (architect-confirmed: cosine in [0.80, 0.92] is the squishy middle
  where engine should NOT emit DUPLICATE without strong corroboration).
- Conservative direction: higher threshold = fewer DUPLICATE verdicts
  = better precision, worse recall.
- Will be tuned on train CV in Phase 1.
"""

COSINE_LOW_DIFFERENT: float = 0.50
"""
Rule-based: cosine_sim < this AND other signals agree -> DIFFERENT.
- Starting point: Pass 1 cross-source-pair distribution shows
  unrelated property pairs cluster around cosine ~0.4-0.5; values
  above 0.5 indicate correlated text (overlapping vocabulary even
  for non-duplicates).
- Conservative direction: lower threshold = fewer DIFFERENT verdicts
  via this signal alone = more UNCERTAIN escalations (recall-friendly).
"""

PRICE_RATIO_DUPLICATE_MAX: float = 1.30
"""
Rule-based: price_ratio = max(a,b)/min(a,b) <= this contributes to
DUPLICATE evidence. 30% margin allows for source-disagreement on
price (haggling, currency rounding, agent commission inclusion).
- Train n=17 duplicates: distribution appears centered around ratio
  1.05 with most observed pairs under 1.20 (per
  duplicate_pair_diagnostics.json). Threshold 1.30 chosen as
  conservative starting point; may tune on train 5-fold CV.
- Sample size honest: with n=17 train positives we cannot claim a
  population-level distribution; this is a starting heuristic.
"""

PRICE_RATIO_DIFFERENT_MIN: float = 3.00
"""
Rule-based: price_ratio > this -> DIFFERENT signal (200%+ price gap
suggests fundamentally different objects).
- Conservative direction: higher threshold = fewer DIFFERENT
  verdicts, more UNCERTAIN.
"""

GPS_SAME_BUILDING_M: float = 100.0
"""
Rule-based: GPS distance <= this AND different bedroom/size signals
-> "different units same building" indicator -> DIFFERENT.
- Pass 2 edge_same_building finder used 50m for tight building
  matches; 100m here as soft same-complex indicator.
"""

GPS_DIFFERENT_KM: float = 50.0
"""
Rule-based: GPS distance > this km -> DIFFERENT signal (different
parts of the prefecture, very unlikely same property).
- Tier 1 same-municipality already implicit; this is for safety.
"""

SIZE_DIFF_PCT_DIFFERENT: float = 50.0
"""
Rule-based: size_diff_pct > this -> DIFFERENT signal. 50% size
disagreement between sources for the same property is implausible.
"""


# =============================================================
# LLM-TIER (Phase 2)
# =============================================================

LLM_MODEL: str = "gpt-4o-mini"
"""
Architect-locked Pass 4: only gpt-4o-mini, no other cloud models,
no local LLM. Cheap, function-calling capable, sufficient for
real-estate text comparison.
"""

LLM_MAX_OUTPUT_TOKENS: int = 500
"""
Verdict + reasoning + confidence fits in <500 tokens easily.
Higher cap wastes cost on rambling.
"""

LLM_TEMPERATURE: float = 0.0
"""
Determinism: same input -> same output. Required for reproducible
test-set evaluation.
"""

LLM_PREFILTER_COSINE_HIGH_SKIP: float = 0.95
"""
LLM-tier: skip LLM call if cosine >= this (rule-based already
DUPLICATE-confident; LLM unlikely to disagree, save cost).
"""

LLM_PREFILTER_COSINE_LOW_SKIP: float = 0.40
"""
LLM-tier: skip LLM call if cosine < this (rule-based already
DIFFERENT-confident; LLM unlikely to flip).
"""

LLM_DEFER_RUNNING_COST_USD: float = 1.00
"""
Architect-defined Pass 4 cost cap: pause and discuss before adding
LLM cost above this cumulative threshold across iterations.
Tracked via src.services.cost_tracker.
"""


# =============================================================
# CLASSICAL ML (Phase 3)
# =============================================================

ML_RANDOM_SEED: int = 42
"""
Determinism for sklearn fit + cross-validation splits.
"""

ML_DECISION_THRESHOLD: float = 0.50
"""
Classical-ML: predict_proba >= this -> DUPLICATE; else DIFFERENT.
- Starting point. May be tuned on train cross-validation, NEVER on
  holdout.
- Asymmetric loss (false-positive worse than false-negative per spec
  §2.4) suggests raising threshold to push toward UNCERTAIN.
"""

ML_UNCERTAIN_BAND_LOW: float = 0.35
ML_UNCERTAIN_BAND_HIGH: float = 0.75
"""
Classical-ML: predict_proba in [LOW, HIGH] -> UNCERTAIN.
- Engine prefers UNCERTAIN over wrong-DUPLICATE (spec §2.4).
- Wider band escalates more pairs to admin (recall-friendly).
- Tuned on train CV only.
"""

ML_CV_FOLDS: int = 5
"""
5-fold cross-validation for hyperparameter tuning on the 72-pair
train set. Architect-confirmed Pass 4 plan.
"""


# =============================================================
# HYBRID (Phase 4)
# =============================================================

HYBRID_TIER1_RULE_CONFIDENT_HIGH: float = 0.95
"""
Hybrid: rule-based signal-agreement score >= this -> DUPLICATE
without going to ML/LLM. (Defined within rule_based.py — number of
DUPLICATE-leaning signals divided by total signals.)
- Starting point. Tuned via train CV.
"""

HYBRID_TIER1_RULE_CONFIDENT_LOW: float = 0.05
"""
Hybrid: rule-based signal-agreement score <= this -> DIFFERENT
without going to ML/LLM.
"""

HYBRID_TIER2_ML_DEFER_TO_LLM_LOW: float = 0.40
HYBRID_TIER2_ML_DEFER_TO_LLM_HIGH: float = 0.85
"""
Hybrid: classical-ML predict_proba in [LOW, HIGH] -> escalate to LLM.
Outside this band, ML is confident enough to verdict alone.
- Tuned via 5-fold CV on train; never on holdout.
"""


# =============================================================
# SPEC §6 SUCCESS CRITERIA (read-only, for assertions)
# =============================================================

SPEC_PRECISION_TARGET: float = 0.99
SPEC_RECALL_TARGET: float = 0.90
SPEC_PRECISION_FLOOR: float = 0.95
"""
Spec §6: precision >= 99% target, 95% acceptable floor for initial
release if recall is high. Engine code asserts winners against these.
"""

SPEC_LATENCY_PER_1000_PAIRS_SEC: float = 600.0  # 10 minutes
SPEC_COST_PER_SCRAPE_USD: float = 1.00
"""
Spec §6: budgets. Bake-off measures latency in pair-scoring stage
(blocking is sub-second per Pass 3). Cost includes only OpenAI spend
(local-LLM ruled out for Pass 4).
"""


# =============================================================
# RETURNING-LISTING DETECTION (Pass 3 deferred decision)
# =============================================================

INCLUDE_RECENTLY_DELISTED_DAYS: int = 0
"""
Pass 4 decision: keep strict-active filter (Option B from
research/CANDIDATE_GENERATION.md §3.7.1). Set to 0 = is_active=true
required; properties with last_seen_at older than 0 days don't enter
the candidate pool.
- Premature optimization without labeled re-listing data.
- Documented in research/RISK_REGISTER.md ('is_active filter assumption').
- Revisit when admin reports missed re-list duplicates in feedback.
"""


# =============================================================
# Tier 2 ML thresholds (Day 3 Phase B Decision 5)
# =============================================================

T2_PROB_DUPLICATE_THRESHOLD: float = 0.92
"""
Asymmetric high bar for DUPLICATE per spec §2.4 (false merges most
harmful). Tier 2 emits DUPLICATE only when raw HistGB prob_duplicate
>= this value; otherwise cascades to Tier 3.

Day 3 reality (N=72 train pairs, no calibration): max raw P(duplicate)
observed in CV = 0.77 < 0.92. Tier 2 effectively NEVER emits DUPLICATE
at this threshold — by design, all DUPLICATE verdicts cascade to the
Tier 3 LLM (arbiter for positives). This maximises spec §2.4 protection
(zero T2 false-positive risk). Day 4 source-pair calibration + Day 6
retraining on larger N may make this threshold reachable.
"""

T2_PROB_DIFFERENT_THRESHOLD: float = 0.80
"""
Tier 2 emits DIFFERENT when raw HistGB prob_different >= this value;
otherwise cascades to Tier 3.

Day 3: lowered from 0.85 -> 0.80 to extract some DIFFERENT cascade
savings on confident-non-duplicate pairs (raw HistGB outputs cluster
more conservatively than calibrated probabilities). Diagnostic CV
expects ~30-40% of pairs to hit this threshold.
"""


# =============================================================
# ENGINE VERSION (Pass 6 Day 2)
# =============================================================

ENGINE_VERSION: str = "v2.0.0-day3"
"""
Engine v2 version string written to engine_pair_cache.engine_version.
Bump invalidates entire cache (cache miss on engine_version mismatch).

Day 3: introduces Tier 2 ML + Tier 3 LLM scoring backends. Cache rows
from prior versions are automatically invalidated by cache.py on next
read (engine_version mismatch -> miss).

Convention:
  v2.0.0-day2  - Day 2 deliverables (Tier 0+1 cascade, stub T2/T3)
  v2.0.0-day3  - Day 3 deliverables (real ML T2, real LLM T3)
  v2.0.0-day4  - Day 4 deliverables (adaptive weights from DB)
  v2.0.0       - Pass 6 production release after Day 6
  v2.1.0+      - first post-prod minor bump (calibration changes,
                 threshold updates, scoring algorithm refinements)

When to bump:
  - Tier 0/1 logic change (rules added/removed/reordered)
  - Tier 2/3 backend swap (model artifact change, prompt change)
  - Threshold change in this file affecting scoring output
  - Default signal weights change in tier_1.py DEFAULT_SIGNAL_WEIGHTS
"""
