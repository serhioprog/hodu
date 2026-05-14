"""
HybridEngine — engine v2 public API per RESEARCH.md §12.5.10.

Three production methods:
  run_full_dedup(session)  -> DedupReport       (Day 5)
  check_pair(session, a, b) -> EngineVerdict    (Day 5; spec §11 short-circuit)
  find_duplicates_for(session, p) -> list[EngineVerdict]  (Day 5)

Plus one Day-1 internal method:
  score_pair(features) -> EngineVerdict  (pure scoring cascade T0->T1->T2->T3)

Day 1 status:
  - score_pair: FULLY IMPLEMENTED (cascade through T0, T1, then stub T2/T3)
  - run_full_dedup / check_pair / find_duplicates_for: NotImplementedError
    stubs with docstrings explaining the Day-N dependency.

Day 2-5 will fill in: cluster construction (§12.5.9), engine_pair_cache
(§12.5.7), spec §11 verdict_locked DB lookup, mismerge_flags emission
(§12.5.11), blocking-pipeline integration (spec §6.1).
"""
from __future__ import annotations

import time
from uuid import UUID

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.domain import Property
from src.services.cost_tracker import cost_tracker

from .blocking import get_candidate_pairs
from .cache import get_cached_verdict, set_cached_verdict
from .cluster_construction import DSUClusterBuilder, ScoredPair
from .dedup_report import DedupReport, EngineVerdict
from .features import PairFeatures, fetch_pair_with_features
from .scoring.pluggable import (
    ScoringBackend,
    StubTier2Backend,
    StubTier3Backend,
)
from .scoring.tier_0 import Tier0Filter
from .scoring.tier_1 import Tier1Scorer
from .writer import write_engine_v2_prediction


class HybridEngine:
    """4-tier hybrid duplicate-detection engine.

    Composition:
      Tier 0 - Tier0Filter (deterministic hard rules, spec §3.4 / §3.1 / §2.3)
      Tier 1 - Tier1Scorer (signal-agreement scoring, spec §6.1)
      Tier 2 - ScoringBackend (classical ML; Day 1 = stub)
      Tier 3 - ScoringBackend (LLM; Day 1 = stub)

    Tiers 0 and 1 are the deterministic core (not pluggable). Tiers 2
    and 3 are dependency-injected via the ScoringBackend Protocol.
    """

    def __init__(
        self,
        tier2_backend: ScoringBackend,
        tier3_backend: ScoringBackend,
    ) -> None:
        self._tier0 = Tier0Filter()
        self._tier1 = Tier1Scorer()
        self._tier2 = tier2_backend
        self._tier3 = tier3_backend
        logger.debug(
            "[HybridEngine] init: T2={t2}, T3={t3}",
            t2=type(tier2_backend).__name__,
            t3=type(tier3_backend).__name__,
        )

    @classmethod
    def build_with_stubs(cls) -> "HybridEngine":
        """Day-1 factory using stub Tier 2 + Tier 3 backends.

        Production path (Day 5+) will use a different factory that
        injects ClassicalMLBackend + OpenAIBackend + cache + cost_tracker
        per env-var configuration (RESEARCH.md §12.5.1).

        This factory is intended for tests and Day-1 validation only.
        """
        return cls(
            tier2_backend=StubTier2Backend(),
            tier3_backend=StubTier3Backend(),
        )

    @classmethod
    def build_default(cls) -> "HybridEngine":
        """Production factory — real Tier 2 ML + Tier 3 LLM backends.

        Tier 2 model loaded from
        experiments/new_engine_v2/models/tier_2_v1.pkl (Pass 6 Day 3
        training output).
        Tier 3 OpenAI client init reads OPENAI_API_KEY via pydantic
        settings.

        Used by daily_sync._run_mdm_pipeline() when feature flag
        settings.USE_NEW_DUPLICATE_ENGINE is True (Pass 6 Sprint C).

        Imports are local to defer engine v2 module loading until the
        flag is enabled — keeps the cold-start path of unrelated
        callers free of Tier 2/3 init cost.
        """
        from pathlib import Path

        from .scoring.tier_2 import Tier2MLBackend
        from .scoring.tier_3 import Tier3LLMBackend

        model_path = (
            Path(__file__).resolve().parent / "models" / "tier_2_v1.pkl"
        )
        return cls(
            tier2_backend=Tier2MLBackend(model_path),
            tier3_backend=Tier3LLMBackend(),
        )

    # ------------------------------------------------------------------
    # Day-1 fully-implemented method
    # ------------------------------------------------------------------

    async def score_pair(
        self,
        features: PairFeatures,
        prop_a: Property,
        prop_b: Property,
    ) -> EngineVerdict:
        """Pure pair-scoring cascade: T0 -> T1 -> T2 -> T3.

        First tier that emits a non-UNCERTAIN verdict wins. UNCERTAIN
        cascades to the next tier. After T3, if still UNCERTAIN, the
        T3 verdict is returned (admin review per spec §2.4).

        Day 3 Path B widening: Property pair passed to T2/T3 backends
        for ML feature extraction / LLM description reading. T0/T1
        use only PairFeatures (their interface is unchanged).

        No DB writes, no cache. Used by:
          - tests (Day 1 validation)
          - check_pair internals (Day 5)
          - find_duplicates_for internals (Day 5)
        """
        # Tier 0 - deterministic hard rules
        t0_result = self._tier0.evaluate(features)
        if t0_result is not None:
            return t0_result

        # Tier 1 - signal-agreement scoring
        t1_result = self._tier1.score(features)
        if t1_result.verdict != "uncertain":
            return t1_result

        # Tier 2 - classical ML
        t2_result = await self._tier2.score(features, prop_a, prop_b)
        if t2_result.verdict != "uncertain":
            return t2_result

        # Tier 3 - LLM
        # If still UNCERTAIN, return T3 verdict (admin review).
        return await self._tier3.score(features, prop_a, prop_b)

    # ------------------------------------------------------------------
    # Day-5 stubs - public API per RESEARCH.md §12.5.10
    # ------------------------------------------------------------------

    async def run_full_dedup(self, session: AsyncSession) -> DedupReport:
        """Full re-scan over the active-property pool (Phase 1 shadow).

        Phase 1 boundary: writes engine_v2_predictions + engine_pair_cache
        only. Does NOT write property_clusters (old engine still owns it
        until Phase 3 cut-over). Mismerge flag emission deferred per
        Pass 6 priority list.

        DSUClusterBuilder runs for metrics aggregation (cluster proposals,
        bridge blocks, approved disagreements) — output appears in
        DedupReport new shadow-mode fields + logger but is not persisted.

        Caller manages transaction lifecycle (no commit inside).
        """
        t_start = time.perf_counter()
        cost_before = await cost_tracker.daily_snapshot()

        # Step 1: blocking
        candidate_pairs = await get_candidate_pairs(session)

        # Step 2: score loop with cache (option iii — explicit tracking)
        scored_pairs: list[ScoredPair] = []
        tier_counts: dict[int, int] = {0: 0, 1: 0, 2: 0, 3: 0}
        cached_count = 0
        skipped_count = 0
        errors_count = 0

        for a_id, b_id in candidate_pairs:
            try:
                features = await fetch_pair_with_features(
                    session, str(a_id), str(b_id),
                )
                if features is None:
                    skipped_count += 1
                    continue
                prop_a = await session.get(Property, a_id)
                prop_b = await session.get(Property, b_id)
                if prop_a is None or prop_b is None:
                    skipped_count += 1
                    continue

                if not prop_a.content_hash or not prop_b.content_hash:
                    skipped_count += 1
                    continue

                cached = await get_cached_verdict(
                    session, a_id, b_id,
                    prop_a.content_hash, prop_b.content_hash,
                )
                if cached is not None:
                    verdict = cached
                    cached_count += 1
                else:
                    verdict = await self.score_pair(features, prop_a, prop_b)
                    await set_cached_verdict(
                        session, a_id, b_id,
                        prop_a.content_hash, prop_b.content_hash,
                        verdict,
                    )

                # Order matters for atomicity — prediction write first.
                # If it fails, scored_pairs/tier_counts don't drift.
                await write_engine_v2_prediction(session, a_id, b_id, verdict)

                if verdict.tier_emitted in tier_counts:
                    tier_counts[verdict.tier_emitted] += 1

                scored_pairs.append(ScoredPair(
                    prop_a_id=a_id,
                    prop_b_id=b_id,
                    verdict=verdict.verdict,
                    confidence=verdict.confidence,
                ))

            except Exception as e:
                logger.error(
                    "[run_full_dedup] pair {a}/{b} failed: {err}",
                    a=str(a_id)[:8], b=str(b_id)[:8], err=str(e),
                )
                errors_count += 1
                continue

        # Step 3: cluster construction (metrics only in shadow mode)
        builder = DSUClusterBuilder(scored_pairs)
        cluster_result = await builder.build(session)

        # Step 3.5: Persist engine v2 clusters (engine_version='2')
        # Sprint 7 Phase B — engine v2 now writes its own PENDING clusters
        # to property_clusters + cluster_v2_members junction. Engine 1
        # continues to use Property.cluster_id FK exclusively.
        from .writer import write_cluster_build_result
        writer_report = await write_cluster_build_result(session, cluster_result)
        logger.info(
            "[run_full_dedup] writer: {n_new} new + {n_attach} attached, "
            "{n_props} props, {n_flags} mismerge flags",
            n_new=writer_report.new_clusters_created,
            n_attach=writer_report.attachments_updated,
            n_props=writer_report.properties_attached,
            n_flags=writer_report.mismerge_flags_emitted,
        )

        # Step 4: aggregate + return
        cost_after = await cost_tracker.daily_snapshot()
        elapsed_ms = int((time.perf_counter() - t_start) * 1000)
        cost_delta = cost_after.llm.cost_usd - cost_before.llm.cost_usd

        n_new = sum(
            1 for c in cluster_result.new_clusters if not c.is_attachment
        )
        n_attached = sum(
            1 for c in cluster_result.new_clusters if c.is_attachment
        )

        logger.info(
            "[run_full_dedup] DONE: {n_scored} scored ({n_cached} cached, "
            "{n_skipped} skipped, {n_errors} errors), "
            "{n_new} new + {n_attached} attached clusters, "
            "{n_bridge} bridges, {n_dis} disagreements, "
            "cost=${c:.6f}, elapsed={t}ms",
            n_scored=len(scored_pairs), n_cached=cached_count,
            n_skipped=skipped_count, n_errors=errors_count,
            n_new=n_new, n_attached=n_attached,
            n_bridge=len(cluster_result.bridge_blocks),
            n_dis=len(cluster_result.approved_disagreements),
            c=cost_delta, t=elapsed_ms,
        )

        return DedupReport(
            clusters_created=0,                       # shadow mode — no writes
            clusters_updated=0,
            clusters_unchanged=0,
            pairs_scored=len(scored_pairs),
            pairs_cached=cached_count,
            mismerge_flags_emitted=0,                 # deferred
            cost_usd=cost_delta,
            latency_ms=elapsed_ms,
            by_tier=tier_counts,
            uncertain_count=sum(
                1 for s in scored_pairs if s.verdict == "uncertain"
            ),
            new_clusters_proposed=n_new,
            attached_clusters_count=n_attached,
            bridge_blocks=len(cluster_result.bridge_blocks),
            approved_disagreements=len(cluster_result.approved_disagreements),
            errors_count=errors_count,
        )

    async def check_pair(
        self,
        session: AsyncSession,
        a_id: UUID,
        b_id: UUID,
    ) -> EngineVerdict:
        """Score a single pair on demand.

        Used by admin-UI manual-merge validation. Bypasses cache;
        never writes.

        Spec §11 short-circuit: if the pair is in a verdict_locked=True
        cluster, returns synthetic verdict with tier_emitted=-1
        without entering the scoring pipeline.

        Day-5 territory - depends on:
          - verdict_locked DB lookup (spec §11 sentinel handling)
          - fetch_pair_with_features integration
        """
        raise NotImplementedError(
            "Day 5 - depends on verdict_locked DB lookup (spec §11 sentinel) "
            "and fetch_pair_with_features integration"
        )

    async def find_duplicates_for(
        self,
        session: AsyncSession,
        property_id: UUID,
    ) -> list[EngineVerdict]:
        """Incremental: find duplicates for a single new property.

        Used by post-scrape new-property hook. Applies blocking
        pipeline (§6.1) seeded by property_id, scores each candidate
        through T0-T3, uses cache. Returns DUPLICATE + UNCERTAIN only
        (DIFFERENT verdicts omitted - caller has no actionable use).

        Day-5 territory - depends on:
          - blocking pipeline integration (spec §6.1)
          - §12.5.7 cache
        """
        raise NotImplementedError(
            "Day 5 - depends on blocking pipeline integration (spec §6.1) "
            "and §12.5.7 cache"
        )
