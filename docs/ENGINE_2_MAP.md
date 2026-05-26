# Engine 2 Map — `HybridEngine` (engine v2)

> Full lifecycle map of the newer 4-tier hybrid duplicate-detection engine for
> the hodu MDM platform. Runs in parallel/shadow alongside Engine 1; writes its
> own clusters (`engine_version='2'`) via the `cluster_v2_members` junction.
>
> Primary package: `src/services/engine_v2/`
> Entry: `src/services/engine_v2/engine.py::HybridEngine.run_full_dedup` (line 168)
> Orchestrated by: `src/tasks/daily_sync.py::_run_mdm_pipeline` (line 781), gated
> by `settings.USE_NEW_DUPLICATE_ENGINE`.

---

## Part A — Plain-language explanation

### What is this engine trying to accomplish?

Same business goal as Engine 1: collapse the same physical property listed
across many broker sites into one canonical cluster, so the platform can publish
a single master record. Engine 2 is a **ground-up redesign** built during the
"Pass 6 / new_engine_v2" research project. Where Engine 1 is a hand-tuned SQL +
cosine + pHash + Vision funnel, Engine 2 is a **principled 4-tier scoring
cascade** with an explicit cost/precision philosophy:

> *"Prefer UNCERTAIN over a wrong DUPLICATE. False positives are far more
> harmful than false negatives."* (`tier_3.py` system prompt, line 87)

Engine 2 separates two concerns that Engine 1 fuses:
1. **Pair scoring** — for any two listings, emit `duplicate` / `different` /
   `uncertain` with a calibrated confidence and a tier provenance.
2. **Cluster construction** — assemble pair verdicts into clusters via a DSU
   that *respects admin-approved clusters as immovable* and *flags*
   (never auto-dissolves) conflicts for human review.

### When does it run? What triggers it?

Inside the nightly `daily_sync()` → `_run_mdm_pipeline()` (`daily_sync.py:762`),
**after** Engine 1, on a **separate DB session**, and **only if**
`settings.USE_NEW_DUPLICATE_ENGINE` is True (default **False**,
`config.py:104`). The whole Engine-2 block is wrapped in try/except so a shadow
failure "MUST NOT block production pipeline" (`daily_sync.py:803-806`).

It is constructed by `HybridEngine.build_default()` (`engine.py:91`), which wires
in the **real** Tier-2 ML model and Tier-3 LLM backend.

### What inputs does it take?

`run_full_dedup(session)` reads the `properties` table (already scraped +
embedded by the time it runs). Per pair it materialises a rich `PairFeatures`
struct (cosine, GPS distance, price/size/year ratios, bedroom match,
municipality/area match, shared-pHash count, both descriptions, and a
"previously-rejected-by-admin" flag) from a single SQL query
(`features.py:209`). Tiers 2 and 3 additionally read the raw `Property` objects.

### What decisions does it make, and on what signals?

For each candidate pair, Engine 2 runs a **first-tier-to-emit-wins cascade**
(`engine.py:124-162`):

- **Tier 0 — deterministic hard rules** (`tier_0.py`): if the pair was rejected
  by an admin, or is same-source, or has mismatched canonical categories, or
  year-built differs by > 5 → instant `different` (confidence 1.0).
- **Tier 1 — weighted signal agreement** (`tier_1.py`): adds/subtracts weights
  for cosine bands, price ratio, "same building different units", GPS distance,
  size disagreement, bedrooms, area, shared photos, close year. Score ≥ +2 →
  `duplicate`; ≤ −2 → `different`; otherwise `uncertain` and cascade down.
- **Tier 2 — classical ML** (`tier_2.py`): a `HistGradientBoostingClassifier`
  reads 13 numeric features and emits probabilities. By design the DUPLICATE bar
  (0.92) is essentially unreachable with the current tiny training set, so Tier 2
  mostly emits confident `different` or cascades.
- **Tier 3 — LLM** (`tier_3.py`): gpt-4o-mini reads both descriptions + computed
  signals and returns a JSON verdict + confidence + reasoning. This is the final
  arbiter; if still uncertain, the pair goes to admin review.

A **per-pair cache** (`engine_pair_cache`) short-circuits the whole cascade when
the same pair was scored before and neither property's content has changed —
this is what keeps the (expensive) Tier-3 LLM cost bounded across runs.

Cluster construction (`cluster_construction.py`) then:
- pre-seeds the DSU with admin-approved clusters as forced unions,
- applies `duplicate` verdicts as unions,
- **blocks** any union that would bridge two distinct approved clusters (or
  attach a new property to multiple approved clusters) and emits a
  **mismerge flag** instead,
- materialises multi-member components into clusters (reusing approved IDs for
  attachments, fresh UUIDs for new clusters).

### What outputs does it produce / what changes in the DB?

- **`engine_pair_cache`** — one cache row per scored pair (verdict, confidence,
  tier, cost). Read-first/write-back.
- **`engine_v2_predictions`** — append-only shadow log, one row per pair per run
  (for drift analysis & daily diff vs Engine 1).
- **`property_clusters`** with `engine_version='2'`, status PENDING, plus
  **`cluster_v2_members`** junction rows — the new engine's own clusters.
- **`mismerge_flags`** — `multi_cluster_bridge` and `engine_t0_disagrees` rows
  for admin review (never auto-acted-on).

It **never** writes `Property.cluster_id` (that's Engine 1's), and never touches
admin fields (`verdict_locked*`, `power_generated_at`, `mismerge_flags.admin_*`).

### Failure modes / when does it skip work?

- **Flag off:** the entire engine is skipped (`daily_sync.py:781`).
- **Per-pair failure:** caught, counted in `errors_count`, the transaction is
  rolled back to clear `InFailedSQLTransactionError`, and the loop continues
  (`engine.py:261-278`).
- **Missing embedding / content_hash / property:** pair skipped (`skipped_count`).
- **LLM API failure:** Tier 3 returns `uncertain` with confidence 0.0 after 3
  retries (`tier_3.py:270-287`); never crashes the run.
- **Whole-engine failure:** caught by the orchestrator's try/except; logged with
  traceback; production pipeline continues.

### Edge cases it is designed to handle

- **Admin-approved clusters are sacred** (spec §11): the DSU pre-seeds them and
  refuses to merge or split them; conflicts become flags, not actions
  (`cluster_construction.py:304-373`, `_apply_verdicts`).
- **Multi-cluster bridges:** a new listing that looks like a duplicate of members
  in *two* approved clusters is attached to the highest-confidence "winner" and
  the losers get flagged (`cluster_construction.py:546-572`).
- **Transitive bridges:** even chains of unclustered pairs that would *indirectly*
  merge two approved clusters are detected and blocked (`_find_approved_in_component`,
  Phase 3, lines 574–606).
- **Cache invalidation:** stale on engine-version bump or content-hash change
  (`cache.py:101-114`).
- **Mid-run durability:** commits every 100 pairs so a crash loses at most one
  batch, not hours of LLM work (`engine.py:201, 251-259`).
- **Determinism:** canonical pair ordering, `min()` anchors, and an EPSILON
  tiebreaker on bridge winners (`cluster_construction.py:702-725`).

---

## Part B — Technical deep-dive

### B.0 Entry point & orchestration

```
daily_sync()                                       daily_sync.py:888
└── _run_mdm_pipeline()                            daily_sync.py:762
    ├── (Engine 1 first, shared session)           lines 770-775
    └── if settings.USE_NEW_DUPLICATE_ENGINE:      line 781
        try:
          from ...engine_v2.engine import HybridEngine     line 783
          engine_v2 = HybridEngine.build_default()         line 784
          async with async_session_maker() as v2_session:  line 785  ← SEPARATE session
              shadow_report = await engine_v2.run_full_dedup(v2_session)  line 786
              await v2_session.commit()                    line 787
        except Exception:
          logger.exception("[MDM] engine v2 shadow run FAILED")  line 806
```

`HybridEngine.build_default()` (`engine.py:91-118`):
- loads Tier-2 model from `src/services/engine_v2/models/tier_2_v1.pkl`
  (`engine.py:112-114`),
- constructs `Tier2MLBackend(model_path)` (`tier_2.py:37`) and
  `Tier3LLMBackend()` (`tier_3.py:222`),
- `__init__` (`engine.py:60-73`) also builds the non-pluggable
  `Tier0Filter()` and `Tier1Scorer()`.

(There is also `build_with_stubs()` at `engine.py:75-88` using
`StubTier2Backend`/`StubTier3Backend` from `pluggable.py` — tests/Day-1 only;
not the production path.)

### B.1 Complete call graph

```
HybridEngine.run_full_dedup(session)                      engine.py:168
│
├─ cost_tracker.daily_snapshot()  (cost_before)           engine.py:183
│
├─ get_candidate_pairs(session)                           blocking.py:26
│     └─ SELECT p1.id,p2.id  (cross-source, same-muni)    blocking.py:44-56
│
├─ for (a_id,b_id) in candidate_pairs:                    engine.py:203
│   ├─ fetch_pair_with_features(session,a,b)              features.py:209
│   │     └─ extract_features(a_dict,b_dict,...)           features.py:86
│   │           └─ to_canonical(category)                 canonical.py:56
│   │           └─ haversine_m(...)                        features.py:27
│   ├─ session.get(Property, a_id) / (Property, b_id)     engine.py:211-212
│   ├─ get_cached_verdict(session,a,b,ha,hb)              cache.py:73
│   │     └─ _make_pair_key / _canonicalize_hashes        cache.py:45 / 55
│   ├─ (cache miss) score_pair(features,prop_a,prop_b)    engine.py:124
│   │     ├─ Tier0Filter.evaluate(features)               tier_0.py:40
│   │     ├─ Tier1Scorer.score(features)                  tier_1.py:70
│   │     │     └─ _compute_signals(features)             tier_1.py:102
│   │     ├─ Tier2MLBackend.score(features,a,b)           tier_2.py:98
│   │     │     └─ feature_extraction.extract_features    feature_extraction.py:59
│   │     │     └─ model.predict_proba(X)                 tier_2.py:128
│   │     └─ Tier3LLMBackend.score(features,a,b)          tier_3.py:242
│   │           ├─ feature_extraction.extract_features    feature_extraction.py:59
│   │           ├─ _build_user_prompt(a,b,feats)          tier_3.py:140
│   │           ├─ AsyncOpenAI.chat.completions.create    tier_3.py:259
│   │           ├─ _parse_response(content)               tier_3.py:185
│   │           └─ cost_tracker.record_llm(...)           tier_3.py:324
│   ├─ set_cached_verdict(session,a,b,ha,hb,verdict)      cache.py:142
│   ├─ write_engine_v2_prediction(session,a,b,verdict)    writer.py:387
│   └─ (every 100) session.commit()                       engine.py:252
│
├─ DSUClusterBuilder(scored_pairs).build(session)         cluster_construction.py:264
│     ├─ _load_approved_clusters(session)                 cluster_construction.py:304
│     ├─ _seed_dsu()                                       cluster_construction.py:379
│     ├─ _apply_verdicts(dsu)                              cluster_construction.py:462
│     │     └─ _pick_bridge_winner(...)                    cluster_construction.py:702
│     │     └─ _find_approved_in_component(...)            cluster_construction.py:436
│     └─ _materialize(dsu)                                 cluster_construction.py:622
│
├─ write_cluster_build_result(session, cluster_result)    writer.py:80
│     ├─ _create_pending_cluster(session, cluster)        writer.py:144
│     │     └─ INSERT property_clusters + cluster_v2_members
│     ├─ _emit_bridge_flag(session, bridge)               writer.py:301
│     └─ _emit_disagreement_flag(session, disagreement)   writer.py:428
│
├─ session.commit()                                       engine.py:301
├─ cost_tracker.daily_snapshot()  (cost_after)            engine.py:312
└─ return DedupReport(...)                                 engine.py:337
```

### B.2 Step-by-step trace

#### Step 1 — blocking (`get_candidate_pairs`, `blocking.py:26`)

A single SQL self-join (`blocking.py:44-56`):
```sql
SELECT p1.id, p2.id
FROM properties p1 JOIN properties p2 ON p1.id < p2.id
WHERE p1.is_active AND p2.is_active
  AND p1.source_domain <> p2.source_domain
  AND p1.calc_municipality IS NOT NULL AND p2.calc_municipality IS NOT NULL
  AND p1.calc_municipality = p2.calc_municipality
ORDER BY p1.id, p2.id
```
- **Filters:** both active, cross-source, same non-null municipality.
- **Canonical ordering** `p1.id < p2.id` → each pair once; `ORDER BY` →
  deterministic.
- **Output:** `list[tuple[UUID, UUID]]`.
- **Deliberately simpler than Engine 1's `_PAIR_SQL`:** no cosine/price/size
  filter here — those become per-pair Python logic in the cascade (module
  docstring lines 4–11). ANN top-K blocking is deferred until the pool exceeds
  ~10K (lines 9–12). This makes blocking O(n²) within a municipality (see B.6).

#### Step 2 — per-pair scoring loop (`engine.py:203-278`)

For each `(a_id, b_id)`:

1. **Features:** `fetch_pair_with_features(session, str(a_id), str(b_id))`
   (`features.py:209`). One SQL roundtrip (lines 218–248) that:
   - pulls both properties' fields + `1 - (a.embedding <=> b.embedding)` cosine
     (line 239),
   - computes `pair_in_feedback` via an `EXISTS` against `ai_duplicate_feedbacks`
     (lines 240–244),
   - returns **None** if either side is missing or has no embedding (filter at
     line 247) → `skipped_count += 1` (`engine.py:209`).
   - `extract_features` (`features.py:86`) then derives canonical categories
     (`to_canonical`, `canonical.py:56`), GPS distance (`haversine_m`,
     `features.py:27`), price ratio/diff%, size diff%, year diff, bedroom match,
     municipality/area equality, shared-pHash count, and carries both raw
     descriptions.
2. **Load ORM objects:** `session.get(Property, a_id)` / `b_id` (lines 211–212);
   skip if either None.
3. **Content-hash guard:** skip if either `content_hash` is falsy (lines 217–219).
4. **Cache read:** `get_cached_verdict(session, a_id, b_id, prop_a.content_hash,
   prop_b.content_hash)` (`cache.py:73`). On hit → reuse verdict,
   `cached_count += 1`. Miss conditions (`cache.py:97-114`): no row,
   `engine_version != T.ENGINE_VERSION`, content-hash mismatch, or expired TTL.
5. **Cache miss → score:** `await self.score_pair(features, prop_a, prop_b)`
   (`engine.py:229`), then `set_cached_verdict(...)` (`cache.py:142`, upsert via
   `ON CONFLICT (pair_key) DO UPDATE`).
6. **Prediction write:** `write_engine_v2_prediction(session, a_id, b_id, verdict)`
   (`writer.py:387`) — INSERT into `engine_v2_predictions`. Comment at
   `engine.py:236-238` notes prediction write is ordered first for atomicity.
7. **Accumulate:** `tier_counts[verdict.tier_emitted] += 1`; append a `ScoredPair`
   (`cluster_construction.py:128`) to `scored_pairs`.
8. **Checkpoint commit:** every `COMMIT_EVERY_N_PAIRS = 100` (line 201) →
   `session.commit()` (line 252) so writes are durable incrementally.
9. **Error path** (lines 261–278): log, `errors_count += 1`,
   `await session.rollback()` to clear the aborted-transaction state (the
   documented "May 14 cascade" fix), reset batch counter, `continue`.

After the loop, a final commit flushes the partial batch (lines 281–286).

#### Step 2a — the scoring cascade (`score_pair`, `engine.py:124-162`)

Pure, no DB/cache. "First tier that emits a non-UNCERTAIN verdict wins."

**Tier 0** — `Tier0Filter.evaluate(features)` (`tier_0.py:40`). Returns a
`different` verdict (confidence 1.0, `tier_emitted=0`) if **any**:
- `features.pair_in_feedback` (admin rejected, spec §3.4) — line 53
- `not features.cross_source` (same domain, spec §3.1) — line 60
- canonical category mismatch, neither "unknown" (spec §2.3) — lines 66–70
- `year_diff > T.YEAR_DIFF_DETERMINISTIC_DIFFERENT` (= 5) — lines 78–81

Returns `None` → cascade continues.

**Tier 1** — `Tier1Scorer.score(features)` (`tier_1.py:70`). `_compute_signals`
(lines 102–183) sums weighted contributions from `DEFAULT_SIGNAL_WEIGHTS`
(lines 41–56):
- cosine ≥0.95 → +2.0; ≥0.92 → +1.0; <0.40 → −2.0; <0.50 → −1.0
- price_ratio ≤1.30 → +1.0; ≥3.0 → −2.0
- same-building-different-units (GPS ≤100m + bedroom mismatch + price diff >20%) → −2.0
- GPS >50km → −1.0; size diff >50% → −1.0
- bedrooms match ±0.5; same calc_area +0.5; shared pHash +1.0; year_diff ≤1 +0.5

Verdict: score ≥ +2.0 → `duplicate`; ≤ −2.0 → `different`; else `uncertain`
(lines 83–88). Confidence = `min(1.0, |score|/6.0)` (line 90). `tier_emitted=1`.
If not uncertain, `score_pair` returns here (`engine.py:152-153`).

**Tier 2** — `Tier2MLBackend.score(features, prop_a, prop_b)` (`tier_2.py:98`):
- Extracts 13 ML features from the raw Properties (`feature_extraction.py:59`):
  price_log_ratio, size/land ratios, bedrooms/bathrooms/year diffs, distance_km,
  embedding_cosine_sim, phash_min_hamming, phash_close_count (≤10 Hamming),
  same municipality/area/category-canonical (lines 26–53). NULLs → `math.nan`
  (HistGB handles natively).
- Builds an `X` matrix in `FEATURE_NAMES` order (`tier_2.py:122-125`),
  `model.predict_proba(X)` (line 128).
- Asymmetric thresholds (`thresholds.py`): `prob_duplicate ≥ 0.92`
  (`T2_PROB_DUPLICATE_THRESHOLD`) → `duplicate`; else `prob_different ≥ 0.80`
  (`T2_PROB_DIFFERENT_THRESHOLD`) → `different`; else `uncertain`
  (`tier_2.py:133-141`). `tier_emitted=2`, `cost_usd=0.0` (local model).
- **Important reality** (`thresholds.py:297-309`): with the N=72 uncalibrated
  training set, max observed P(duplicate) ≈ 0.77 < 0.92, so Tier 2 **never**
  emits `duplicate` — all positives cascade to Tier 3 by design (max spec §2.4
  protection). It *can* emit `different`.

**Tier 3** — `Tier3LLMBackend.score(features, prop_a, prop_b)` (`tier_3.py:242`):
- Re-extracts the 13 features (for the signal block), builds a per-pair user
  prompt (`_build_user_prompt`, line 140) embedding both descriptions (truncated
  to 600 chars) + computed signals.
- Calls `gpt-4o-mini`, `response_format={"type":"json_object"}`,
  `temperature=0.0`, `max_tokens=300` (lines 259–268), with up to
  `MAX_RETRIES=3` exponential-backoff retries on `APITimeout/RateLimit/APIConnection`
  (lines 270–294). `BadRequestError` → no retry (lines 295–308).
- `_parse_response` (line 185) defensively parses JSON → falls back to
  `uncertain`/0.0 on any malformation; validates verdict ∈ {duplicate, different,
  uncertain}; clamps confidence to [0,1].
- Computes cost from `response.usage` tokens (lines 318–323) and calls
  `cost_tracker.record_llm(...)` (line 324). `tier_emitted=3`.
- This is the final arbiter — if still `uncertain`, that verdict propagates to
  admin review.

#### Step 3 — cluster construction (`DSUClusterBuilder.build`, `cluster_construction.py:264`)

Input: `scored_pairs: list[ScoredPair]` (verdict + confidence + the two IDs).

1. **`_load_approved_clusters(session)`** (line 304): loads all
   `property_clusters` where `status=APPROVED OR verdict_locked=True` (lines
   323–328), bulk-loads their members via `Property.cluster_id` (lines 337–340),
   and builds `self._approved: {cluster_id: _ApprovedCluster}` +
   `self._member_to_cluster: {property_id: cluster_id}` (lines 348–366). Approved
   clusters with 0 members are skipped defensively (lines 350–357).
   **Note:** it reads `Property.cluster_id` — i.e. Engine 1's membership — so the
   "approved clusters" it protects are largely Engine-1-owned (see Part C).
2. **`_seed_dsu()`** (line 379): for each approved cluster, add all members and
   union them under the first member as anchor (O(N) unions/cluster, line 405–409).
   Enforces spec §11 — approved clusters become immovable single components.
3. **`_apply_verdicts(dsu)`** (line 462): three phases over `duplicate` pairs
   only (non-duplicate verdicts are no-ops):
   - **Phase 1 (classify, lines 494–532):** for each duplicate pair, look up each
     side's approved cluster. **Case 1** both in same approved cluster → skip.
     **Case 2** both in *different* approved clusters → `ApprovedDisagreement`
     (lines 507–515). **Case 3** one approved + one unclustered → record an
     *intent* (`prop → {cluster: [confidences]}`) (lines 517–529). **Case 4**
     both unclustered → defer to Phase 3.
   - **Phase 2 (resolve intents, lines 534–572):** single-cluster intent → union
     the property to that cluster's anchor (`min(member_ids)`, FIX B, line 542).
     Multi-cluster intent → `_pick_bridge_winner` (line 702: highest mean
     confidence, EPSILON-guarded tie → earliest `created_at`), union to winner,
     emit a `BridgeBlockEvent` for the losers (lines 559–564).
   - **Phase 3 (unclustered-unclustered, lines 574–606):** for each deferred
     pair, `_find_approved_in_component` (line 436) checks whether the two
     properties already sit in components anchored by *different* approved
     clusters. If so → transitive bridge → `ApprovedDisagreement`, do **not**
     union (lines 582–603). Else → safe `dsu.union` (line 606).
4. **`_materialize(dsu)`** (line 622): walk components. Singletons skipped
   (line 645). Component containing an approved member → **attachment**
   (`is_attachment=True`, reuse approved cluster_id, `ai_score=None`, lines
   656–664). Otherwise → **new cluster** (fresh `uuid4()`, `ai_score = mean(pair
   confidences)` over the component's recorded duplicate pairs, lines 666–693).

Output: `ClusterBuildResult(new_clusters, bridge_blocks, approved_disagreements)`
(line 202). This step is **pure / read-only** — no DB writes.

#### Step 3.5 — persist clusters (`write_cluster_build_result`, `writer.py:80`)

Called at `engine.py:297`. Three loops (lines 105–127):

1. **New clusters** (lines 105–116): for each `ProposedCluster`,
   - if `is_attachment` → **skipped** in the Sprint-7-Phase-B MVP (lines 106–112;
     attachment to existing v2-approved clusters is Sprint 8 work).
   - else → `_create_pending_cluster` (line 144): INSERT a `PropertyCluster`
     (`status=PENDING`, `engine_version='2'`, `member_count=len(members)`,
     `ai_score`, `phash_matches=None`, `notes="engine_v2: created at …"`, lines
     173–183), `flush()` (line 191, needed before the FK-bearing junction insert),
     then INSERT `cluster_v2_members` rows via raw SQL `unnest` with
     `ON CONFLICT DO NOTHING` (lines 197–208).
2. **Bridge blocks** (lines 118–121): `_emit_bridge_flag` (line 301) — one
   `mismerge_flags` row **per loser cluster**, `flag_type='multi_cluster_bridge'`,
   anchored on the loser cluster's `min(member.id)`, canonical pair ordering,
   `ON CONFLICT (cluster_id,pair_a_id,pair_b_id,flag_type) DO NOTHING` (idempotent).
3. **Approved disagreements** (lines 123–127): `_emit_disagreement_flag`
   (line 428) — one `mismerge_flags` row, `flag_type='engine_t0_disagrees'`,
   `cluster_id=min(cluster_a,cluster_b)`, canonical pair ordering, idempotent.

`_update_attachment` (line 218) exists and is fully implemented (UPDATE
`cluster_v2_members`-equivalent via `Property.cluster_id`… actually via
`Property` update + `member_count` bump + notes append, lines 240–298) **but is
not called** by the MVP path. Note its body updates `Property.cluster_id` (line
267–271), which would be an Engine-1 column — see Part C.

#### Step 4 — report aggregation (`engine.py:311-355`)

- `cost_after - cost_before` from `cost_tracker.daily_snapshot()` → `cost_delta`
  (line 314).
- Count `n_new` vs `n_attached` from `cluster_result.new_clusters` (lines 316–321).
- Build and return a `DedupReport` (`dedup_report.py:47`) with
  `clusters_created=0` (shadow-mode semantics, line 338), `pairs_scored`,
  `pairs_cached`, `by_tier`, `uncertain_count`, `new_clusters_proposed`,
  `attached_clusters_count`, `bridge_blocks`, `approved_disagreements`,
  `errors_count`, `cost_usd`, `latency_ms`.

The orchestrator logs this report (`daily_sync.py:788-802`).

### B.3 Database tables

| Table | Access | Key columns |
|-------|--------|-------------|
| `properties` | read | `id, embedding, source_domain, category, price, size_sqm, land_size_sqm, bedrooms, bathrooms, year_built, calc_municipality, calc_area, latitude, longitude, image_phashes, description, content_hash, is_active, cluster_id` |
| `ai_duplicate_feedbacks` | read (EXISTS) | `prop_a_id, prop_b_id` (`features.py:240-244`) |
| `engine_pair_cache` | read + upsert | `pair_key, engine_version, a_content_hash, b_content_hash, verdict, confidence, reasoning, tier_emitted, cost_usd, scored_at, expires_at` (`domain.py:520`, migration 011) |
| `engine_v2_predictions` | insert (append) | `pair_key, a_id, b_id, verdict, confidence, reasoning, tier_emitted, cost_usd, scored_at` (`domain.py:603`, migration 013) |
| `property_clusters` | read (approved) + insert (PENDING v2) | `id, status, member_count, engine_version, ai_score, phash_matches, notes, created_at, verdict_locked` (`domain.py:83`) |
| `cluster_v2_members` | insert | `cluster_id, property_id, added_at` (`domain.py:66`, migration 015) |
| `mismerge_flags` | insert (idempotent) | `cluster_id, pair_a_id, pair_b_id, flag_type, flag_reason, detected_at` (`domain.py:553`, migration 012) |

Membership mechanism: **`cluster_v2_members` junction** (migration 015). Engine 2
reads Engine 1's `Property.cluster_id` to learn approved clusters but writes its
own membership only to the junction. It **never** writes `Property.cluster_id`
(except the dormant `_update_attachment`, see Part C).

### B.4 External services & configuration

External calls:
- **pgvector** (`a.embedding <=> b.embedding` cosine in `features.py:239`;
  also recomputed in NumPy inside Tier-2 features, `feature_extraction.py:160`).
- **OpenAI gpt-4o-mini** (Tier 3, `tier_3.py:259`) with `cost_tracker.record_llm`.
- **local sklearn HistGradientBoostingClassifier** (Tier 2, loaded from
  `models/tier_2_v1.pkl` via `joblib`, `tier_2.py:69`).
- **cost_tracker.daily_snapshot()** for cost deltas (`engine.py:183, 312`).

Configuration:
- **Feature flag:** `settings.USE_NEW_DUPLICATE_ENGINE` (`config.py:104`,
  default False).
- **Tier-3 OpenAI key:** `settings.OPENAI_API_KEY` (`tier_3.py:239`).
- **All scoring thresholds** live in
  `src/services/engine_v2/config/thresholds.py` — *not* in `settings`/`.env`:

| Constant | Value | Controls |
|----------|-------|----------|
| `YEAR_DIFF_DETERMINISTIC_DIFFERENT` | 5 | Tier 0 hard reject (`thresholds.py:23`) |
| `COSINE_HIGH_DUPLICATE` | 0.92 | Tier 1 cosine dup band (`:87`) |
| `COSINE_LOW_DIFFERENT` | 0.50 | Tier 1 cosine diff band (`:98`) |
| `LLM_PREFILTER_COSINE_LOW_SKIP` | 0.40 | Tier 1 strong-diff band (`:181`) |
| `PRICE_RATIO_DUPLICATE_MAX` | 1.30 | Tier 1 price-dup (`:109`) |
| `PRICE_RATIO_DIFFERENT_MIN` | 3.00 | Tier 1 price-diff (`:122`) |
| `GPS_SAME_BUILDING_M` | 100.0 | Tier 1 same-building (`:130`) |
| `GPS_DIFFERENT_KM` | 50.0 | Tier 1 far-apart (`:138`) |
| `SIZE_DIFF_PCT_DIFFERENT` | 50.0 | Tier 1 size-diff (`:145`) |
| `T2_PROB_DUPLICATE_THRESHOLD` | 0.92 | Tier 2 dup bar (`:297`) |
| `T2_PROB_DIFFERENT_THRESHOLD` | 0.80 | Tier 2 diff bar (`:311`) |
| `ENGINE_VERSION` | `"v2.0.0-day3"` | cache invalidation key (`:327`) |

Tier-3 module constants (`tier_3.py`): `MODEL="gpt-4o-mini"` (:44),
`MAX_RETRIES=3` (:45), `DESCRIPTION_TRUNCATE_CHARS=600` (:47),
`MAX_OUTPUT_TOKENS=300` (:48), token pricing (:51–52). Tier-1 thresholds
(`tier_1.py`): `DUPLICATE_THRESHOLD=2.0`, `DIFFERENT_THRESHOLD=-2.0`,
`MAX_SCORE_NORMALIZER=6.0` (:30–36), `DEFAULT_SIGNAL_WEIGHTS` (:41–56).
Tier-2 ML feature threshold `PHASH_CLOSE_THRESHOLD=10` (`feature_extraction.py:56`).

### B.5 Concurrency model

- **Async, logically sequential.** The scoring loop processes pairs one at a time
  (`engine.py:203`); the only concurrency is OpenAI client internals.
- **Multiple transaction boundaries.** Unlike Engine 1's single commit, Engine 2
  commits **every 100 pairs** (line 252), once after the loop (line 282), and
  once after the writer (line 301). The caller's `v2_session.commit()`
  (`daily_sync.py:787`) is a "no-op safety net" (comment lines 298–300).
- **Rollback for recovery.** On a per-pair exception it rolls back the current
  uncommitted batch (line 274) to clear `InFailedSQLTransactionError`, then
  resumes — SQLAlchemy auto-begins the next transaction on the next `execute`.
- **Separate session from Engine 1** (`daily_sync.py:785`): Engine 2 failures
  cannot poison Engine 1's transaction.
- **No locks.** Cache upserts use `ON CONFLICT DO UPDATE` (last-writer-wins,
  `cache.py:175`); flag inserts use `ON CONFLICT DO NOTHING` (idempotent). Safe
  under the single-nightly-process assumption.

### B.6 Performance characteristics

- **Blocking (`get_candidate_pairs`):** O(n²) self-join *within each
  municipality* (no cosine/price prefilter at the SQL level, `blocking.py:44`).
  Acceptable at the current pool size; the module docstring (lines 9–12) flags
  ANN top-K as deferred to Pass 5.5+ beyond ~10K properties. This is the primary
  scaling cliff.
- **Per-pair features:** one SQL roundtrip each (`features.py:209`) — N roundtrips
  total. Could be batched.
- **Cascade cost:** T0/T1 are pure-Python and ~free. T2 is a single local
  `predict_proba` (cheap). T3 is a network LLM call — the cost/latency driver,
  but gated by (a) the cache, (b) only pairs that reach T3 uncertain. The cache
  is what makes nightly re-runs cheap (only changed/new pairs hit T3).
- **DSU construction:** near-linear (path compression + union by rank,
  `cluster_construction.py:70-109`). `_materialize`'s per-component pair-conf
  lookup is O(members²) per component (lines 668–673) — fine for small clusters.
- **`_find_approved_in_component`** (line 436) iterates approved members per
  call — O(approved · deferred_pairs) in Phase 3; mitigated when approved/total
  ratio is low (docstring lines 446–451).

### B.7 Error-handling pattern

- **Per-pair isolation** (`engine.py:261-278`): every pair is wrapped; failures
  are counted and the loop continues after a rollback. This is the engine's
  signature robustness feature.
- **Tier-3 LLM resilience:** retries with backoff, then a graceful `uncertain`
  verdict on exhaustion or `BadRequestError` (`tier_3.py:270-308`); JSON parse
  failures fall back to `uncertain` (`_parse_response`, line 185).
- **Tier-2 init validation:** raises on missing model file, missing
  `predict_proba`, feature-name drift, or unexpected class labels
  (`tier_2.py:63-88`) — fail-fast at construction.
- **Writer FK ordering:** `flush()` before junction insert prevents FK violations
  (`writer.py:185-191`).
- **Whole-engine guard:** the orchestrator's try/except (`daily_sync.py:782-806`)
  ensures any uncaught failure is logged but never blocks production.

---

## Part B-addendum — Reference tables & worked examples

### B.8 `PairFeatures` field reference

Produced by `extract_features` (`features.py:86-185`); consumed by Tier 0/1.
(Tiers 2/3 derive their own features from raw Properties instead.)

| Field | Type | Derivation |
|-------|------|-----------|
| `a_id`, `b_id` | str | property ids |
| `a_source`, `b_source`, `cross_source` | str/bool | `source_domain`; `cross_source = a≠b` |
| `canonical_category_a/_b`, `same_canonical_category` | str/bool | `to_canonical(category)` (`canonical.py:56`) |
| `cosine_sim` | float\|None | `1 - (embedding <=> embedding)` from SQL |
| `gps_distance_m` | float\|None | `haversine_m(lat/lng)` (`features.py:27`) |
| `price_a/_b`, `price_ratio`, `price_diff_pct` | int/float\|None | `max/min`, `|a-b|/max·100` |
| `size_a/_b`, `size_diff_pct` | float\|None | `|a-b|/max·100` |
| `year_a/_b`, `year_diff` | int\|None | `|a-b|` |
| `bedrooms_match` | bool\|None | `a == b` when both present |
| `same_municipality`, `same_calc_area` | bool\|None | equality when both present |
| `shared_phash_count` | int | `len(set(a) & set(b))` (exact-equal hashes) |
| `description_a/_b` | str\|None | raw text (Tier-3 only) |
| `pair_in_feedback` | bool | `EXISTS` in `ai_duplicate_feedbacks` |

### B.9 Tier-1 signal weights (`tier_1.py:41-56`)

| Signal | Weight | Condition |
|--------|--------|-----------|
| `cosine_strong_dup` | +2.0 | cosine ≥ 0.95 |
| `cosine_dup` | +1.0 | 0.92 ≤ cosine < 0.95 |
| `cosine_diff` | −1.0 | 0.40 ≤ cosine < 0.50 |
| `cosine_low_diff` | −2.0 | cosine < 0.40 |
| `price_ratio_dup` | +1.0 | ratio ≤ 1.30 |
| `price_ratio_diff` | −2.0 | ratio ≥ 3.0 |
| `same_building_diff_units` | −2.0 | GPS ≤ 100m + bedroom mismatch + price diff > 20% |
| `gps_far` | −1.0 | > 50 km apart |
| `size_diff` | −1.0 | size diff > 50% |
| `bedrooms_match` / `bedrooms_differ` | +0.5 / −0.5 | bedrooms equal / differ |
| `same_calc_area` | +0.5 | same calc_area |
| `shared_phash` | +1.0 | ≥1 shared pHash |
| `year_close` | +0.5 | year_diff ≤ 1 |

Verdict boundary ±2.0; confidence = `min(1, |score|/6.0)`.

### B.10 Tier-2 ML features (`feature_extraction.py:26-53`)

13 features, fixed order (must match training). NULL → `math.nan` for numeric,
`0.0` for categorical.

| # | Feature | Formula |
|---|---------|---------|
| 1 | `price_log_ratio` | `log(max/min)` of price |
| 2 | `size_sqm_ratio` | `max/min` of size_sqm |
| 3 | `land_size_ratio` | `max/min` of land_size_sqm |
| 4 | `bedrooms_diff` | `|a-b|` |
| 5 | `bathrooms_diff` | `|a-b|` |
| 6 | `year_built_diff` | `|a-b|` |
| 7 | `distance_km` | Haversine km |
| 8 | `embedding_cosine_sim` | NumPy dot of 1536-dim vectors |
| 9 | `phash_min_hamming` | min Hamming over all photo pairs (64 if none) |
| 10 | `phash_close_count` | count of photo pairs with Hamming ≤ 10 |
| 11 | `same_calc_municipality` | 1.0 / 0.0 |
| 12 | `same_calc_area` | 1.0 / 0.0 |
| 13 | `same_category_canonical` | 1.0 if equal-and-known else 0.0 |

### B.11 `EngineVerdict` & `DedupReport` reference

`EngineVerdict` (frozen, `dedup_report.py:27-44`): `verdict`
(`duplicate|different|uncertain`), `confidence` [0,1], `reasoning`,
`tier_emitted` (0|1|2|3, or −1 sentinel for the unimplemented verdict_locked
short-circuit), `cost_usd`, `latency_ms`.

`DedupReport` (mutable, `dedup_report.py:47-92`): cluster counts (0 in shadow),
`pairs_scored`, `pairs_cached`, `by_tier` histogram, `uncertain_count`, plus
shadow-mode fields `new_clusters_proposed`, `attached_clusters_count`,
`bridge_blocks`, `approved_disagreements`, `errors_count`, `cost_usd`,
`latency_ms`.

### B.12 Cache invalidation matrix (`cache.py:97-114`)

| Condition | Result |
|-----------|--------|
| no row for `pair_key` | miss |
| `row.engine_version != T.ENGINE_VERSION` ("v2.0.0-day3") | miss (whole-version invalidation) |
| `row.a_content_hash`/`b_content_hash` ≠ current canonical | miss (property content changed) |
| `row.expires_at <= now` | miss (TTL — default NULL = never) |
| else | **hit** → reconstruct `EngineVerdict` (`latency_ms=0`) |

Explicit invalidation: `invalidate_pair_cache(a,b)` (line 197) and
`invalidate_property_cache(p)` (line 217, LIKE-matches both pair-key positions).

### B.13 DUPLICATE-verdict → DSU case matrix (`_apply_verdicts`, `cluster_construction.py:494-606`)

| a in approved? | b in approved? | same cluster? | Action |
|----------------|----------------|---------------|--------|
| yes | yes | yes | skip (already seeded) — line 503 |
| yes | yes | no | `ApprovedDisagreement` flag — lines 507–515 |
| yes | no | — | record attach intent → Phase 2 — lines 517–521 |
| no | yes | — | record attach intent → Phase 2 — lines 524–528 |
| no | no | — | defer to Phase 3 (transitive check) — line 532 |

Phase 2 intent resolution: 1 candidate cluster → union to its `min()` anchor;
≥2 → `_pick_bridge_winner` (highest mean conf, EPSILON tie → earliest
`created_at`), union to winner, `BridgeBlockEvent` per loser. Phase 3: if the
two unclustered props already sit under *different* approved roots → transitive
`ApprovedDisagreement`, no union; else safe union.

### B.14 Worked example — a clean new duplicate pair

A (`kw-greece.gr`, villa, €480k, 150 m², year 2019, desc "seafront villa…") and
B (`clever-estate.gr`, villa, €495k, 148 m², year 2020), neither yet clustered:

1. **Blocking** keeps the pair (cross-source, same municipality "Kassandra").
2. **Features:** cosine 0.973, price_ratio 1.03, size diff 1.3%, year_diff 1,
   same municipality, 3 shared pHashes.
3. **Cache:** miss (first time).
4. **Cascade:** T0 — categories both "villa" (canonical equal), year_diff 1 ≤ 5,
   cross-source, not in feedback → no fire, cascade. T1 — cosine 0.973 ≥0.95
   (+2.0), price_ratio ≤1.30 (+1.0), shared pHash (+1.0), year_close (+0.5),
   same calc_area maybe (+0.5) → score ≈ +5.0 ≥ +2.0 → **`duplicate`**,
   confidence `min(1, 5/6) = 0.83`, `tier_emitted=1`. T2/T3 never invoked.
5. **Writes:** `set_cached_verdict` upserts the cache row;
   `write_engine_v2_prediction` logs the shadow row.
6. **DSU:** neither in an approved cluster → Phase 3 safe union {A,B}.
7. **Materialize:** multi-member, no approved member → new cluster, fresh UUID,
   `ai_score = 0.83`.
8. **Writer:** `_create_pending_cluster` inserts `property_clusters`
   (`engine_version='2'`, PENDING) + two `cluster_v2_members` rows.

### B.15 Worked example — the uncertain pair that reaches the LLM

A and B as above but cosine is 0.88 (mid band), price_ratio 1.6, year_diff 2,
1 shared pHash:

1–3. Same blocking/feature/cache-miss.
4. **T0:** no fire. **T1:** cosine 0.88 is in the dead `[0.50, 0.92)` band
   (no contribution, `tier_1.py:127`), price_ratio 1.6 neither ≤1.30 nor ≥3.0
   (no contribution), shared pHash (+1.0), bedrooms match (+0.5) → score ≈ +1.5,
   `|1.5| < 2.0` → **`uncertain`**, cascade. **T2:** HistGB `predict_proba`
   yields, say, `prob_dup=0.6` (< 0.92) and `prob_diff=0.4` (< 0.80) →
   **`uncertain`**, cascade. **T3:** gpt-4o-mini reads both descriptions +
   signals, returns e.g. `{"verdict":"duplicate","confidence":0.86,...}` →
   `tier_emitted=3`, cost recorded.
5. **DSU/writer:** duplicate → union → new PENDING v2 cluster (if T3 had said
   `uncertain`, the pair would contribute no DSU union and surface as a
   prediction row only, awaiting admin).

### B.16 Worked example — admin-cluster bridge block

Property C (newly scraped) scores `duplicate` against a member of approved
cluster **X** *and* against a member of approved cluster **Y** (admin had
deliberately kept X and Y separate):

1. `_load_approved_clusters` has X and Y seeded as immovable components.
2. Phase 1 records intent `C → {X:[0.9], Y:[0.82]}`.
3. Phase 2: two candidate clusters → `_pick_bridge_winner` picks **X**
   (mean 0.9 > 0.82). C is unioned into X. A `BridgeBlockEvent(new=C,
   winner=X, losers=[(Y, 0.82)])` is emitted.
4. Writer: `_emit_bridge_flag` writes one `mismerge_flags` row
   (`flag_type='multi_cluster_bridge'`, `cluster_id=Y`) for admin review. X and
   Y are **never** merged automatically (spec §11).

---

## Part C — Critique

### Strengths

1. **Principled, layered scoring with explicit cost discipline.** The
   T0→T1→T2→T3 cascade (`engine.py:124-162`) escalates only what's uncertain, and
   the asymmetric thresholds everywhere encode the "false-positive is worse"
   policy (`thresholds.py:297-309`, `tier_3.py:87-94`).
2. **Cache-first design genuinely bounds LLM spend.** `get_cached_verdict` with
   content-hash + engine-version invalidation (`cache.py:73-114`) means a nightly
   re-run only pays the LLM for new/changed pairs. The cache is the single
   biggest cost lever and it's done well.
3. **Spec §11 admin-authority handling is sophisticated.** The DSU pre-seeds
   approved clusters and the three-phase `_apply_verdicts` blocks both direct and
   *transitive* bridges, flagging rather than acting
   (`cluster_construction.py:462-616`). This is a meaningfully harder problem than
   Engine 1's locked-cluster check and it's handled carefully.
4. **Strong determinism engineering.** Canonical pair keys (`cache.py:45`),
   `min()` anchors (FIX B), EPSILON bridge-winner tiebreak (FIX A,
   `cluster_construction.py:718-725`), and ordered blocking all make output
   reproducible across runs.
5. **Per-pair error isolation + batched commits** (`engine.py:201, 261-278`):
   a single bad pair or a mid-run crash costs at most one 100-pair batch, not the
   whole run — directly addressing the documented "May 14 cascade".
6. **Clean separation of concerns:** features / scoring tiers / DSU / writer are
   independently testable; tiers 2–3 are dependency-injected via a Protocol
   (`pluggable.py:26`), so backend swaps need no engine changes.

### Weaknesses

1. **Tier 2 is effectively inert for positives (HIGH-as-designed, but risky).**
   With N=72 uncalibrated training data, `T2_PROB_DUPLICATE_THRESHOLD=0.92` is
   unreachable (`thresholds.py:302-308`), so *every* duplicate decision funnels
   to the gpt-4o-mini LLM. The "ML tier" currently buys only some `different`
   short-circuits. This concentrates both cost and correctness risk on Tier 3.
2. **`ENGINE_VERSION="v2.0.0-day3"`** (`thresholds.py:327`) is a "day3" research
   tag still in the production cache key. It's honest but signals the engine was
   promoted mid-development; any threshold tweak that *should* bump the version
   (per the convention at lines 344–349) silently won't, risking stale cache
   reuse.
3. **Approved-cluster protection reads the wrong engine's membership (HIGH).**
   `_load_approved_clusters` loads members via `Property.cluster_id`
   (`cluster_construction.py:337-340`), which is **Engine 1's** FK. Engine 2's own
   approved clusters (members in `cluster_v2_members`) are invisible to its own
   bridge/seed logic. So Engine 2 protects Engine-1 clusters but not its own —
   and `_update_attachment` is disabled (`writer.py:106-112`), so v2 clusters are
   rebuilt fresh every run with new UUIDs.
4. **`_update_attachment` updates `Property.cluster_id`** (`writer.py:267-271`) —
   an Engine-1-owned column — despite the engine's stated contract that it writes
   membership only to the junction. It is currently dormant (never called), so
   it's a latent cross-engine bug rather than an active one, but it directly
   contradicts the writer module docstring (lines 19–25).
5. **Fresh UUIDs every run for new clusters** (`_materialize`, line 689) combined
   with the disabled attachment path means a PENDING v2 cluster that an admin
   hasn't yet approved gets a *new* `id` on the next nightly run — duplicating
   `engine_v2_predictions`/cluster churn and making the admin worklist unstable
   for v2 (mitigated only because Engine 1 no longer wipes them, per the
   `_delete_orphan_clusters` fix in Engine 1).
6. **Blocking is O(n²) per municipality** with no cosine prefilter at SQL level
   (`blocking.py:44-56`), so the candidate set (and the number of feature SQL
   roundtrips + cache lookups) grows quadratically in dense municipalities. The
   docstring acknowledges this is deferred work.
7. **N feature-fetch roundtrips** (`features.py:209` called once per pair) and
   **N cache roundtrips** are unbatched; on a large candidate set the DB
   round-trip count, not the LLM, may dominate latency before Tier 3 is even
   reached.
8. **Tier 2 recomputes cosine in NumPy** (`feature_extraction.py:160-180`) rather
   than reusing the pgvector value already fetched in `PairFeatures.cosine_sim`,
   loading the full 1536-dim embedding into Python per pair — wasteful.

### Suggested improvements (ranked)

| Impact | Suggestion | Rationale (anchor) |
|--------|-----------|--------------------|
| **HIGH** | Make `_load_approved_clusters` also load v2-owned approved clusters from `cluster_v2_members` (union with the `Property.cluster_id` source), and enable `_update_attachment`. Otherwise Engine 2 cannot protect/grow its own clusters. | `cluster_construction.py:337-340` reads only `Property.cluster_id`; `writer.py:106-112` disables attachment. |
| **HIGH** | Either retrain/calibrate Tier 2 so its DUPLICATE bar is reachable, or explicitly document Tier 2 as a "DIFFERENT-only fast filter" and drop the dead DUPLICATE branch to reduce cost concentration on Tier 3. | `thresholds.py:302-308`; `tier_2.py:133`. |
| **HIGH** | Fix `_update_attachment` to write `cluster_v2_members`, not `Property.cluster_id`, before it is ever enabled. | `writer.py:267-271` vs module contract `writer.py:19-25`. |
| **MED** | Add a cheap SQL prefilter to blocking (cosine band / price band, as Engine 1 does) to cut the O(n²) candidate explosion. | `blocking.py:44-56`; contrast Engine 1 `_PAIR_SQL`. |
| **MED** | Batch `fetch_pair_with_features` and `get_cached_verdict` (e.g. fetch all features for a p1's candidates in one query) to cut N round-trips. | `features.py:209`, `engine.py:205-221`. |
| **MED** | Reuse `features.cosine_sim` in Tier 2 instead of reloading embeddings and recomputing in NumPy. | `feature_extraction.py:160-180`. |
| **LOW** | Make `ENGINE_VERSION` reflect production (`v2.0.0`+) and add a CI check that threshold/weight changes bump it. | `thresholds.py:327-349`. |
| **LOW** | Stabilise PENDING v2 cluster IDs across runs (content-addressed cluster id, or attachment) so the admin worklist doesn't churn. | `cluster_construction.py:689`. |

### Inconsistencies between engines (Engine 2 perspective)

1. **Membership model (intentional).** Engine 1 = `Property.cluster_id`;
   Engine 2 = `cluster_v2_members` (migration 015). By design, but it leaks:
   Engine 2's *own* approval-protection still reads the Engine-1 column
   (`cluster_construction.py:337`), which is almost certainly **accidental** and
   means v2 clusters aren't protected by v2's own DSU.
2. **Category handling (accidental gap in Engine 1).** Engine 2 canonicalises
   categories and hard-rejects mismatches at Tier 0 (`tier_0.py:65-75`,
   `canonical.py`); Engine 1 only checks `category IS NOT NULL` and never compares
   categories (`internal_duplicate_detector.py:58`). Engine 1 can therefore merge
   a Villa with an Apartment; Engine 2 cannot. Same problem, different rigor.
3. **Year handling (divergent, intentional).** Engine 2 has an explicit
   `year_diff > 5` deterministic reject (`tier_0.py:78`) plus a soft signal;
   Engine 1 has no year logic at all. Engine 2's choice is data-justified
   (`thresholds.py:24-31`); Engine 1's omission is an unaddressed gap.
4. **Expensive-arbiter modality (divergent).** Engine 1's arbiter is **GPT-4o
   Vision over photos** (`vision_tiebreaker.py`); Engine 2's is **gpt-4o-mini over
   descriptions** (`tier_3.py`). They use different evidence and could disagree on
   the same pair. Neither consults the other. Likely intentional, never
   reconciled.
5. **`ai_score` semantics collide (accidental).** Both engines write the same
   `property_clusters.ai_score` column (`domain.py:121`): Engine 1 stores **max
   edge cosine** (`internal_duplicate_detector.py:462`), Engine 2 stores **mean of
   pair confidences** (`cluster_construction.py:676`). Any UI rendering `ai_score`
   without checking `engine_version` shows incomparable numbers.
6. **Cluster status policy (convergent by accident).** Both engines now create
   only PENDING clusters (Engine 1 line 574; Engine 2 `writer.py:176`), so both
   require human approval — but for different reasons (Engine 1 by Sprint-7 policy
   reversal, Engine 2 by spec §2.4 from the start).
7. **Error isolation (divergent, see Engine 1 critique).** Engine 2 isolates every
   pair and commits in batches; Engine 1 does neither. The newer/experimental
   engine is strictly more robust than production — backwards from a risk
   standpoint.

---

*End of Engine 2 map.*
