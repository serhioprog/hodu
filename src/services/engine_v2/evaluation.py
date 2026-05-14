"""
Evaluation framework for engine v2 architecture bake-off.

Loads train/holdout pairs from labeled_pairs.json, runs a scorer
function on each pair, computes precision/recall/F1/cost/latency with
per-category breakdown.

Usage (from a scoring script):

    from src.services.engine_v2.evaluation import (
        load_train, load_holdout, evaluate, write_results,
    )

    async def my_scorer(pair, features) -> PairResult: ...

    async with async_session_maker() as session:
        m = await evaluate(session, my_scorer, load_train())
    write_results("results/bake_off_train.json", "rule_based", m)

`load_holdout()` is reserved for the single-shot final check after the
winner is picked on train. Spec §5.4: holdout is sacred.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal

from sqlalchemy.ext.asyncio import AsyncSession

from .features import PairFeatures, fetch_pair_with_features

_HERE = Path(__file__).resolve().parent  # src/
LABELED_PATH = _HERE.parent / "test_set" / "labeled_pairs.json"
RESULTS_DIR = _HERE.parent / "results"

Verdict = Literal["duplicate", "different", "uncertain"]
VALID_VERDICTS: frozenset[str] = frozenset({"duplicate", "different", "uncertain"})


# =============================================================
# Data classes
# =============================================================

@dataclass
class LabeledPair:
    """One pair from the labeled test set."""
    id: str
    property_a_id: str
    property_b_id: str
    ground_truth: Verdict
    category: str
    reasoning: str
    edge_case_tags: list[str]
    provenance: dict[str, Any]


@dataclass
class PairResult:
    """Output of scoring a single pair."""
    pair_id: str
    pair_a_id: str
    pair_b_id: str
    category: str
    ground_truth: Verdict
    predicted: Verdict
    confidence: float | None
    cost_usd: float
    latency_ms: float
    reasoning: str | None = None
    correct: bool = False  # filled by _compute_metrics


@dataclass
class Metrics:
    """Aggregate metrics across a set of pairs."""
    architecture: str
    total: int
    n_duplicate_truth: int
    n_different_truth: int
    n_uncertain_truth: int

    # Confusion (treating uncertain-predicted separately):
    true_positive: int
    false_positive: int
    true_negative: int
    false_negative: int
    uncertain_predicted: int

    precision: float
    recall: float
    f1: float
    coverage: float
    """Fraction of pairs where engine emitted a hard verdict
    (TP+FP+TN+FN)/total. UNCERTAIN-predicted = abstention."""

    total_cost_usd: float
    total_latency_ms: float
    avg_latency_ms_per_pair: float
    latency_per_1000_pairs_sec: float

    by_category: dict[str, dict[str, Any]] = field(default_factory=dict)
    pair_results: list[PairResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "architecture": self.architecture,
            "total": self.total,
            "truth_counts": {
                "duplicate": self.n_duplicate_truth,
                "different": self.n_different_truth,
                "uncertain": self.n_uncertain_truth,
            },
            "confusion": {
                "true_positive": self.true_positive,
                "false_positive": self.false_positive,
                "true_negative": self.true_negative,
                "false_negative": self.false_negative,
                "uncertain_predicted": self.uncertain_predicted,
            },
            "metrics": {
                "precision": round(self.precision, 4),
                "recall": round(self.recall, 4),
                "f1": round(self.f1, 4),
                "coverage": round(self.coverage, 4),
            },
            "cost_usd": round(self.total_cost_usd, 6),
            "latency": {
                "total_ms": round(self.total_latency_ms, 1),
                "avg_ms_per_pair": round(self.avg_latency_ms_per_pair, 1),
                "per_1000_pairs_sec": round(self.latency_per_1000_pairs_sec, 2),
            },
            "by_category": self.by_category,
            "pair_results": [
                {
                    "pair_id": r.pair_id,
                    "a": r.pair_a_id, "b": r.pair_b_id,
                    "category": r.category,
                    "ground_truth": r.ground_truth,
                    "predicted": r.predicted,
                    "correct": r.correct,
                    "confidence": r.confidence,
                    "cost_usd": round(r.cost_usd, 6),
                    "latency_ms": round(r.latency_ms, 1),
                    "reasoning": r.reasoning,
                }
                for r in self.pair_results
            ],
        }


# =============================================================
# Loaders
# =============================================================

def _load_labeled_doc() -> dict[str, Any]:
    if not LABELED_PATH.exists():
        raise FileNotFoundError(f"{LABELED_PATH} not found — finalize the test set first")
    with LABELED_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_split(split: str) -> list[LabeledPair]:
    d = _load_labeled_doc()
    split_ids = set(d["splits"].get(split, []))
    out: list[LabeledPair] = []
    for p in d["pairs"]:
        if p["id"] in split_ids:
            out.append(LabeledPair(
                id=p["id"],
                property_a_id=p["property_a_id"],
                property_b_id=p["property_b_id"],
                ground_truth=p["ground_truth"],
                category=p["category"],
                reasoning=p.get("reasoning") or "",
                edge_case_tags=p.get("edge_case_tags") or [],
                provenance=p.get("provenance") or {},
            ))
    return sorted(out, key=lambda x: x.id)


def load_train() -> list[LabeledPair]:
    """72 train pairs, sorted by test-NNN id."""
    return _load_split("train")


def load_holdout() -> list[LabeledPair]:
    """28 holdout pairs. SPEC §5.4: single-shot evaluation only — do not tune on this."""
    return _load_split("holdout")


# =============================================================
# Evaluator
# =============================================================

ScorerFn = Callable[[LabeledPair, PairFeatures], Awaitable[PairResult]]


async def evaluate(
    session: AsyncSession,
    scorer_fn: ScorerFn,
    pairs: list[LabeledPair],
    *,
    architecture: str,
) -> Metrics:
    """
    Run `scorer_fn` against each pair, return aggregate Metrics.

    The scorer is responsible for setting `predicted`, `confidence`,
    `cost_usd`, and (optionally) `reasoning`. Latency is measured by
    this evaluator if scorer leaves `latency_ms == 0`.
    """
    results: list[PairResult] = []
    for pair in pairs:
        t0 = time.perf_counter()
        feats = await fetch_pair_with_features(
            session, pair.property_a_id, pair.property_b_id
        )
        if feats is None:
            results.append(PairResult(
                pair_id=pair.id,
                pair_a_id=pair.property_a_id,
                pair_b_id=pair.property_b_id,
                category=pair.category,
                ground_truth=pair.ground_truth,
                predicted="uncertain",
                confidence=None,
                cost_usd=0.0,
                latency_ms=(time.perf_counter() - t0) * 1000,
                reasoning="property data missing or no embedding",
            ))
            continue
        result = await scorer_fn(pair, feats)
        if result.predicted not in VALID_VERDICTS:
            raise ValueError(
                f"scorer returned invalid verdict {result.predicted!r} for pair {pair.id}"
            )
        if result.latency_ms == 0:
            result.latency_ms = (time.perf_counter() - t0) * 1000
        results.append(result)

    return _compute_metrics(architecture, results)


# Sidetable: per-results-id, count of UNCERTAIN-predicted-on-truth-duplicate.
# Stashed during _compute_metrics so the display layer can show it without
# changing the Metrics dataclass shape (keeps JSON output schema stable).
_UNC_ON_TRUTH_DUP_COUNTS: dict[int, int] = {}


def _compute_metrics(architecture: str, results: list[PairResult]) -> Metrics:
    n = len(results)
    truth_counts = {"duplicate": 0, "different": 0, "uncertain": 0}
    for r in results:
        truth_counts[r.ground_truth] += 1

    tp = fp = tn = fn = unc_pred = 0
    unc_on_truth_dup = 0
    for r in results:
        gt, pr = r.ground_truth, r.predicted
        if pr == "uncertain":
            unc_pred += 1
            if gt == "duplicate":
                unc_on_truth_dup += 1
            r.correct = (gt == "uncertain")
            continue
        if pr == "duplicate" and gt == "duplicate":
            tp += 1
            r.correct = True
        elif pr == "duplicate" and gt == "different":
            fp += 1
        elif pr == "different" and gt == "different":
            tn += 1
            r.correct = True
        elif pr == "different" and gt == "duplicate":
            fn += 1
        else:
            r.correct = False

    # Spec §6 recall: TP / total_truth_duplicate.
    # FN = predicted-DIFFERENT on truth-duplicate (real failure, hurts recall).
    # UNCERTAIN-on-truth-duplicate = abstention (acceptable per spec §6 wording
    # "the rest become UNCERTAIN, which is acceptable — admin reviews"), but
    # also doesn't count as "found", so it lowers strict recall.
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    n_dup_truth = truth_counts["duplicate"]
    recall = tp / n_dup_truth if n_dup_truth > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    hard_verdicts = tp + fp + tn + fn
    coverage = hard_verdicts / n if n > 0 else 0.0
    # Stash the extra count for display.
    _UNC_ON_TRUTH_DUP_COUNTS[id(results)] = unc_on_truth_dup

    total_cost = sum(r.cost_usd for r in results)
    total_lat = sum(r.latency_ms for r in results)
    avg_lat = total_lat / n if n > 0 else 0.0
    per_1k_sec = (avg_lat * 1000) / 1000  # ms per pair * 1000 pairs / 1000 ms/sec

    # Per-category breakdown
    by_cat: dict[str, dict[str, Any]] = {}
    cats = sorted({r.category for r in results})
    for cat in cats:
        cat_results = [r for r in results if r.category == cat]
        cat_tp = sum(1 for r in cat_results if r.predicted == "duplicate" and r.ground_truth == "duplicate")
        cat_fp = sum(1 for r in cat_results if r.predicted == "duplicate" and r.ground_truth == "different")
        cat_tn = sum(1 for r in cat_results if r.predicted == "different" and r.ground_truth == "different")
        cat_fn = sum(1 for r in cat_results if r.predicted == "different" and r.ground_truth == "duplicate")
        cat_unc = sum(1 for r in cat_results if r.predicted == "uncertain")
        cat_correct = sum(1 for r in cat_results if r.correct)
        by_cat[cat] = {
            "n": len(cat_results),
            "true_positive": cat_tp,
            "false_positive": cat_fp,
            "true_negative": cat_tn,
            "false_negative": cat_fn,
            "uncertain_predicted": cat_unc,
            "correct": cat_correct,
            "accuracy": round(cat_correct / len(cat_results), 4) if cat_results else 0.0,
        }

    return Metrics(
        architecture=architecture,
        total=n,
        n_duplicate_truth=truth_counts["duplicate"],
        n_different_truth=truth_counts["different"],
        n_uncertain_truth=truth_counts["uncertain"],
        true_positive=tp,
        false_positive=fp,
        true_negative=tn,
        false_negative=fn,
        uncertain_predicted=unc_pred,
        precision=precision,
        recall=recall,
        f1=f1,
        coverage=coverage,
        total_cost_usd=total_cost,
        total_latency_ms=total_lat,
        avg_latency_ms_per_pair=avg_lat,
        latency_per_1000_pairs_sec=per_1k_sec,
        by_category=by_cat,
        pair_results=results,
    )


# =============================================================
# Result IO
# =============================================================

def write_results(filename: str, architecture: str, metrics: Metrics) -> Path:
    """
    Append metrics for one architecture to a results JSON, keyed by name.
    Atomic write via .tmp + rename.
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / filename
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            doc = json.load(f)
    else:
        doc = {"architectures": {}}
    doc["architectures"][architecture] = metrics.to_dict()
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)
    tmp.replace(path)
    return path


def _fmt_precision(tp: int, fp: int) -> str:
    den = tp + fp
    if den == 0:
        return f"n/a (0 predicted-duplicate)"
    return f"{tp/den:.4f} ({tp}/{den} TP, {fp}/{den} FP)"


def _fmt_recall(tp: int, fn: int, n_truth_dup: int, unc_on_dup: int = 0) -> str:
    """Spec §6 recall: TP / total_truth_duplicate. FN is the failure mode;
    UNCERTAIN-on-truth-dup is acceptable abstention per spec but still
    lowers strict recall (it doesn't count as 'found').
    """
    if n_truth_dup == 0:
        return "n/a (0 truth-duplicate)"
    return (
        f"{tp/n_truth_dup:.4f} ({tp}/{n_truth_dup} dup caught, "
        f"{fn} FN, {unc_on_dup} UNCERTAIN-on-dup)"
    )


def _fmt_coverage(tp: int, fp: int, tn: int, fn: int, unc: int, total: int) -> str:
    if total == 0:
        return "n/a"
    hard = tp + fp + tn + fn
    return f"{hard/total:.4f} ({hard}/{total} hard verdicts, {unc}/{total} UNCERTAIN)"


def _fmt_cat_precision(tp: int, fp: int) -> str:
    den = tp + fp
    if den == 0:
        return "n/a       "
    return f"{tp/den:.2f} ({tp}/{den})"


def _fmt_cat_recall(tp: int, fn: int, n_truth_dup: int) -> str:
    """Spec §6 recall: TP / total_truth_duplicate (per category)."""
    if n_truth_dup == 0:
        return "n/a       "
    return f"{tp/n_truth_dup:.2f} ({tp}/{n_truth_dup})"


def print_metrics_summary(metrics: Metrics) -> None:
    """
    One-pager metrics dump with sample-size context.
    Per architect Pass-4 reporting requirements (2026-05-07):
    - precision/recall printed with raw counts (numerator/denominator)
    - coverage printed alongside (UNCERTAIN as abstention is visible)
    - per-category n shown; categories with n<5 marked *info* only
    """
    m = metrics
    print(f"\n=== {m.architecture}  (n={m.total}) ===")
    print(f"  truth:    duplicate={m.n_duplicate_truth}  "
          f"different={m.n_different_truth}  uncertain={m.n_uncertain_truth}")
    print()
    unc_on_dup = sum(
        1 for r in m.pair_results
        if r.predicted == "uncertain" and r.ground_truth == "duplicate"
    )
    print(f"  precision  {_fmt_precision(m.true_positive, m.false_positive)}")
    print(f"  recall     {_fmt_recall(m.true_positive, m.false_negative, m.n_duplicate_truth, unc_on_dup)}")
    print(f"  coverage   {_fmt_coverage(m.true_positive, m.false_positive, m.true_negative, m.false_negative, m.uncertain_predicted, m.total)}")
    print(f"  f1         {m.f1:.4f}")
    print()
    print(f"  cost=${m.total_cost_usd:.4f}  "
          f"avg={m.avg_latency_ms_per_pair:.1f} ms/pair  "
          f"per-1000-pairs={m.latency_per_1000_pairs_sec:.1f}s")
    print()
    print(f"  by category (n<5 = informational, not selection criterion):")
    print(f"    {'category':30s}  {'n':>2}  {'precision':<14}  {'recall':<14}  unc")
    print(f"    {'-' * 30}  {'-' * 2}  {'-' * 14}  {'-' * 14}  ---")
    for cat in sorted(m.by_category):
        c = m.by_category[cat]
        n = c["n"]
        marker = " *info*" if n < 5 else ""
        p_str = _fmt_cat_precision(c["true_positive"], c["false_positive"])
        # Per-category truth-duplicate count for strict recall denominator
        n_cat_dup = c["true_positive"] + c["false_negative"] + sum(
            1 for r in m.pair_results
            if r.category == cat and r.predicted == "uncertain" and r.ground_truth == "duplicate"
        )
        r_str = _fmt_cat_recall(c["true_positive"], c["false_negative"], n_cat_dup)
        print(f"    {cat:30s}  {n:>2}  {p_str:<14}  {r_str:<14}  {c['uncertain_predicted']:>2}{marker}")
