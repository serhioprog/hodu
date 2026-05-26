# Engine 1 Map — `InternalDuplicateDetector`

> Full lifecycle map of the original (v1) duplicate-detection engine for the
> hodu MDM platform. Production-tested, owns `property_clusters` via the
> `Property.cluster_id` foreign key.
>
> Primary source file: `src/services/internal_duplicate_detector.py`
> Orchestrated by: `src/tasks/daily_sync.py::_run_mdm_pipeline`
>
> **Sprint 8 V1 refactor (2026-05-26):** 11 fixes applied. See Part C for
> the before/after weakness map. Most notably:
>
> - V1-Robust-1: per-pair try/except + SAVEPOINT per component
> - V1-Defense: detector.run() wrapped in daily_sync
> - V1-Precision-1: category canonicalization at classification time
> - V1-Recall-1: pending edges now union in DSU
> - V1-QualityGateMetric: observability for dropped properties
> - V1-AdminAuthority: Task E REPLACED — APPROVED clusters sacrosanct,
>   new evidence creates cluster_member_proposals records
> - V1-Cleanup, V1-EngineVersionExplicit: hygiene

---

## Part A — Plain-language explanation

### What is this engine trying to accomplish?

The hodu platform scrapes property listings from ~14 Halkidiki real-estate
broker websites. The same physical villa, apartment, or plot of land is very
often listed on several of those sites at once, each with its own photos,
description, and price. The business problem is **master data management**: we
want exactly **one canonical record per physical property**, not 14 noisy
copies.

Engine 1 is the component that decides _"are these two listings, scraped from
two different websites, actually the same real-world property?"_ When it
decides yes, it groups them into a **cluster**. A cluster is the raw material
from which a later pipeline phase synthesises a single "PowerObject" master
record. Engine 1's job stops at proposing clusters and surfacing them for human
review — it deliberately does **not** auto-publish merges (see "decisions"
below).

### When does it run? What triggers it?

Engine 1 runs once per **nightly daily-sync cycle**. The orchestrator is
`daily_sync()` (`src/tasks/daily_sync.py`), which runs three phases in sequence:

1. **Scraping** (`_run_scrapers`) — collect/diff/revive/deep-fetch listings per
   domain.
2. **MDM** (`_run_mdm_pipeline`) — embed properties, then run the duplicate
   detectors. **Engine 1 lives here.**
3. **PowerObject generation** (`_run_power_generation`) — external uniqueness
   check + GPT-4o synthesis of master records.

Within phase 2, Engine 1 runs **first**, _before_ Engine 2. This ordering is
deliberate: Engine 2's Tier-3 LLM scoring "can take hours" and the surrounding
comment explains Engine 1 runs first "so that a long-running engine v2 phase
can never starve production clustering." Engine 1 is the production engine;
Engine 2 is shadow/parallel.

There is no independent scheduler inside the engine — it is purely a function
called by the orchestrator. (A scheduler/cron sits above `daily_sync` but is
out of scope for this map.)

### What inputs does it take?

The only argument to `InternalDuplicateDetector.run()` is an open async DB
session. All real input comes from the `properties` table, which by the time
Engine 1 runs has already been:

- **Freshly scraped** (phase 1), so statuses, prices, and descriptions are
  current.
- **Re-embedded** (`EmbeddingService.refresh_property_embeddings`), so every
  active property has an up-to-date 1536-dim OpenAI text embedding and a
  `content_hash` fingerprint.

Engine 1 considers a property _eligible_ only if it has an embedding, a
content hash, a known municipality and category, at least bedrooms-or-land,
and a description ≥ 50 characters (the SQL "Quality Gate", `_PAIR_SQL`
`eligible` CTE).

### What decisions does it make, and on what signals?

Engine 1 is a **multi-level funnel**. For each candidate pair of listings it
asks a sequence of cheaper-to-more-expensive questions:

1. **Quality + hard pre-filter (SQL):** Are both eligible? Cross-source? Price
   within 30%? Size within 15%? Not already rejected by an admin? Not already
   in the same cluster? (One big SQL query does all of this.)
2. **Embedding similarity (pgvector):** How semantically similar are the two
   canonical descriptions (cosine over the HNSW index)?
3. **Category canonicalization (V1-Precision-1):** Do both sides canonicalize
   to the same broad family (RESIDENTIAL_DETACHED, RESIDENTIAL_FLATS, COMPLEX,
   LAND, HOTEL, COMMERCIAL)? Villa/House/Maisonette OK; Villa↔Apartment blocked.
4. **Photo fingerprints (pHash):** Do the two listings share actual photos
   (perceptual-hash matches), ignoring generic stock/beach/logo images?
5. **Vision tie-break (GPT-4o, optional):** For ambiguous "gray-zone" pairs,
   _look at the photos_ and decide if it's the same building.

The verdict per pair is one of: `merge_approved` (the two belong together),
`merge_pending` (looks plausible but not certain — needs review), or
`reject_vision` (Vision said different — record so we never re-ask).

Both `merge_approved` AND `merge_pending` edges are unioned in DSU into
connected components (V1-Recall-1); each multi-member component becomes a
cluster. Pure-pending components surface for admin review rather than being
silently dropped.

### What outputs does it produce / what changes in the DB?

After a run, Engine 1 has:

- **Created or updated `property_clusters` rows** (always `engine_version='1'`,
  set explicitly — V1-EngineVersionExplicit), each with a `member_count`,
  `ai_score` (max edge similarity), and `phash_matches` (max photo overlap).
  **All new clusters are created in `PENDING` status** — a human must approve
  them.
- **Set `Property.cluster_id`** on each clustered property (this FK is Engine
  1's exclusive membership mechanism).
- **Written `ai_duplicate_feedbacks` rows** for pairs Vision confidently
  rejected, so they are never re-proposed.
- **Written `cluster_member_proposals` rows** (V1-AdminAuthority, Sprint 8)
  for new candidate members detected for locked+APPROVED clusters — instead of
  reverting those clusters.
- **Detached SOLD properties** from their clusters.
- **Deleted orphan clusters** (engine-1-owned clusters that lost all members).
- **NEVER reverts APPROVED clusters** anymore (Sprint 7 Task E replaced by
  proposal creation — V1-AdminAuthority).

### Failure modes / when does it skip work?

- **Vision disabled** (`VISION_TIEBREAKER_ENABLED=False`, the default): gray-zone
  pairs simply stay `merge_pending`, get unioned in DSU, and become PENDING
  clusters for admin review.
- **Vision budget exhausted:** only the top `VISION_MAX_PAIRS_PER_RUN` (50)
  pairs by similarity get a Vision call; the rest stay pending.
- **Vision low confidence / API failure:** the pair stays pending; never crashes.
- **No candidate pairs:** nothing happens; commit is a no-op.
- **Locked clusters** (`verdict_locked=True`): Engine 1 must never overwrite an
  admin's manual verdict. PENDING locked → metadata refresh only. APPROVED
  locked + new candidate → proposal record (V1-AdminAuthority); cluster
  unchanged.
- **Malformed row in `_classify_pairs`** (V1-Robust-1): isolated via try/except,
  logged, counted in `stats["classify_errors"]`, run continues.
- **Component persist failure** (V1-Robust-1): isolated via SAVEPOINT
  (`async with session.begin_nested()`). On exception, savepoint rolls back;
  other components survive; counted in `stats["component_errors"]`.
- **Whole-run failure** (V1-Defense): wrapped in try/except inside
  `_run_mdm_pipeline`. logger.exception captures traceback; session is rolled
  back; downstream phases (Engine 2, PowerObject) continue.

### Edge cases it is designed to handle

- **Stock photos:** beach/sunset/logo images that recur across unrelated
  listings are filtered so they don't create false photo matches.
- **Slug renames & revival:** handled upstream in `_run_scrapers`; Engine 1
  benefits because revived properties get `content_hash` cleared and re-embedded.
- **Category mismatches** (V1-Precision-1): "Villa" and "Apartment" are
  different physical types — never clustered together even if cosine is high.
  Synonyms (Villa/House/Maisonette/Bungalow) all map to RESIDENTIAL_DETACHED
  and can cluster cross-site.
- **Gray-zone pairs** (V1-Recall-1): pure pending-edge components now persist
  as PENDING clusters with `gray_zone_clusters` stat tracking — admin sees them
  for review rather than the engine silently dropping them as singletons.
- **Admin authority** (V1-AdminAuthority): locked APPROVED clusters are
  sacrosanct. New candidates create `cluster_member_proposals` records; admin
  decides APPROVE (add to cluster) or REJECT (blacklist pairs in
  `ai_duplicate_feedbacks`). Cluster status unchanged either way. PowerObject
  preserved.
- **Cross-engine contamination:** the orphan-delete is explicitly scoped to
  `engine_version='1'` so it never destroys Engine 2's clusters.

---

## Part B — Technical deep-dive

### B.0 Entry point & orchestration

daily_sync() src/tasks/daily_sync.py
└── \_run_mdm_pipeline() src/tasks/daily_sync.py
├── EmbeddingService()
├── InternalDuplicateDetector()
├── async with async_session_maker() ← ONE shared session
│ ├── embedder.refresh_property_embeddings(session)
│ └── try: ← V1-Defense (Sprint 8)
│ │ await detector.run(session) ← ENGINE 1 ENTRY
│ except Exception:
│ logger.exception(...)
│ await session.rollback() ← graceful degradation
└── if settings.USE_NEW_DUPLICATE_ENGINE: ← Engine 2 (separate session)

The engine class is `InternalDuplicateDetector`
(`src/services/internal_duplicate_detector.py`). It is instantiated with no
arguments; its only state is a lazily-created `VisionTiebreaker` (`self._vision`).

The entry method is **`async def run(self, session) -> Dict[str, int]`**. It
returns a stats dict and commits its own work inside `session`. The caller's
`async with` block owns the session lifecycle, and V1-Defense (Sprint 8)
wraps the `await detector.run(session)` call so an Engine 1 crash never
blocks Engine 2 or PowerObject generation.

### B.0a The funnel levels at a glance

The module docstring names the funnel "Партия 5". Mapping the conceptual
levels to code:

| Level | Name                | Mechanism                                               | Where                                          |
| ----- | ------------------- | ------------------------------------------------------- | ---------------------------------------------- |
| -1    | Quality Gate metric | `_QUALITY_GATE_SQL` aggregate count                     | `_measure_quality_gate` (V1-QualityGateMetric) |
| 0     | Quality Gate        | SQL `eligible` CTE                                      | `_PAIR_SQL` eligible CTE                       |
| 1     | Hard pre-filter     | feedback blacklist, price/size bands, same-cluster skip | `_PAIR_SQL` LATERAL                            |
| 2     | Embedding           | pgvector cosine `> SIM_REJECT`, top-K per p1            | `_PAIR_SQL`                                    |
| 2.5   | Category canonical  | `_canonicalize_category()` Python filter                | `_classify_pairs` (V1-Precision-1)             |
| 3     | Smart pHash         | stock-filtered photo matching                           | `_classify_pairs` + `phash_service.py`         |
| 4     | Vision tie-break    | GPT-4o photos for gray-zone                             | `_apply_vision_tiebreaker`                     |
| 5     | DSU components      | union approved AND pending edges                        | `run()` DSU loop (V1-Recall-1)                 |
| 6     | Persist             | write clusters OR proposals + metrics                   | `_persist_component` (V1-AdminAuthority)       |

Levels 0–2 are fused into a single SQL statement so PostgreSQL does the heavy
filtering before any Python sees a row. Level 2.5 (category) is a cheap Python
filter applied per row before the more expensive pHash compute.

### B.1 Complete call graph

InternalDuplicateDetector.run(session)
│
├─ \_release_sold_clusters(session)
│ └─ UPDATE properties SET cluster_id=NULL WHERE status=SOLD
│
├─ \_measure_quality_gate(session) ← V1-QualityGateMetric
│ └─ executes QUALITY_GATE_SQL → qg\* stats
│
├─ \_build_stock_phashes(session)
│ └─ executes \_STOCK_PHASH_SQL
│
├─ session.execute(\_PAIR_SQL, {sim_reject, per_p1_limit})
│
├─ \_classify_pairs(rows, stock_phashes) → (edges, errors) ← V1-Robust-1
│ ├─ \_canonicalize_category(...) per side ← V1-Precision-1
│ ├─ PHashService.count_matching(...)
│ └─ try/except per row (classify_errors counter)
│
├─ if VISION_TIEBREAKER_ENABLED and pending>0:
│ └─ \_apply_vision_tiebreaker(session, edges, stats)
│ ├─ self.\_vision.decide_pair(session, a, b)
│ │ └─ AsyncOpenAI.beta.chat.completions.parse(model="gpt-4o", ...)
│ └─ \_record_vision_reject(session, edge)
│
├─ \_DSU() add(both)/union(approved AND pending)/components() ← V1-Recall-1
│
├─ for each component: ← V1-Robust-1 SAVEPOINT
│ try:
│ async with session.begin_nested():
│ \_persist_component(session, ids, edges)
│ ├─ SELECT Property WHERE id IN (...)
│ ├─ SELECT PropertyCluster (locked?)
│ ├─ if locked + APPROVED + new_members: ← V1-AdminAuthority
│ │ INSERT INTO cluster_member_proposals (raw SQL, ON CONFLICT)
│ │ refresh ai_score / phash_matches on cluster
│ │ DO NOT touch status/verdict_locked/PowerObject
│ ├─ if locked (PENDING or no new):
│ │ reassign cluster_id, recompute member_count
│ └─ else (new/unlocked):
│ create PropertyCluster(engine_version='1', ...) ← V1-EngineVersionExplicit
│ assign cluster_id, recompute member_count
│ except Exception:
│ stats["component_errors"] += 1 (savepoint auto-rolled back)
│
├─ session.flush()
├─ \_delete_orphan_clusters(session)
│ └─ DELETE property_clusters WHERE engine_version='1' AND ...
└─ session.commit()

External services touched: **pgvector** (HNSW cosine in `_PAIR_SQL` /
`_STOCK_PHASH_SQL`), **OpenAI GPT-4o Vision** (only if Vision enabled),
and the cost tracker. OpenAI text-embedding is invoked **before** Engine 1
by `EmbeddingService`, not by Engine 1 itself.

### B.2 Step-by-step trace

#### Step 0 — stats init & SOLD release

`run()` initialises a stats dict with keys for: cluster lifecycle counts
(`approved_singleton_skipped`, `pending`, `locked_preserved`,
`orphans_removed`, `sold_cleaned`), Vision counts (`vision_resolved`,
`vision_skipped`, `vision_feedback_added`), error counters (`classify_errors`,
`component_errors` — V1-Robust-1), recall (`gray_zone_clusters` — V1-Recall-1),
admin authority (`proposals_created` — V1-AdminAuthority), and Quality Gate
observability (`qg_*` — V1-QualityGateMetric, populated in Step 1).

`_release_sold_clusters(session)` runs:

```sql
UPDATE properties SET cluster_id = NULL
WHERE cluster_id IS NOT NULL AND status = 'SOLD';
```

- **Input:** all properties currently in a cluster with status SOLD.
- **Output:** `cluster_id` nulled; `rowcount` returned as `sold_cleaned`.
- **Why:** SOLD listings should not anchor a live cluster. (Note Bug #46:
  this filters **only** SOLD; DELISTED detachment happens via the revival
  flow in `daily_sync`, not here.)

#### Step 1 — Quality Gate measurement (V1-QualityGateMetric)

`_measure_quality_gate(session)` runs `_QUALITY_GATE_SQL`, a single aggregate
query counting:

- `qg_active`: total properties in scrapeable statuses (ACTIVE/NEW/PRICE_CHANGED/DELISTED)
- `qg_eligible`: subset passing all Quality Gate criteria
- `qg_dropped` = qg_active - qg_eligible
- Drop reasons (may overlap): `qg_drop_no_embedding`, `qg_drop_no_content_hash`,
  `qg_drop_no_municipality`, `qg_drop_no_category`, `qg_drop_no_bed_or_land`,
  `qg_drop_short_desc`

These are added directly to `stats` and logged. Surfaces silent recall loss
that the production query (`_PAIR_SQL`) silently filters away.

#### Step 2 — stock-photo pHash set

`_build_stock_phashes(session)` runs `_STOCK_PHASH_SQL`:

```sql
SELECT phash FROM (
  SELECT unnest(image_phashes) AS phash, COUNT(DISTINCT id) AS prop_count
  FROM properties
  WHERE image_phashes IS NOT NULL AND array_length(image_phashes,1) > 0
  GROUP BY 1
) hash_counts
WHERE prop_count > :min_count
```

- **Param:** `min_count = settings.PHASH_STOCK_MIN_PROPS` (= 5).
- **Output:** a `Set[str]` of perceptual hashes that appear on **more than 5
  distinct properties** — treated as stock/template imagery and ignored in all
  later photo comparisons.

#### Step 3 — candidate-pair generation (`_PAIR_SQL`)

`_PAIR_SQL` is the heart of the funnel's Levels 0–2. Structure:

**CTE `eligible` (Quality Gate, Level 0):**

```sql
SELECT id, embedding, image_phashes, source_domain, cluster_id,
       price, size_sqm, description, content_hash, category
FROM properties
WHERE embedding IS NOT NULL
  AND content_hash IS NOT NULL
  AND status IN ('ACTIVE','NEW','PRICE_CHANGED','DELISTED')
  AND calc_municipality IS NOT NULL
  AND category IS NOT NULL
  AND (bedrooms IS NOT NULL OR land_size_sqm IS NOT NULL)
  AND LENGTH(COALESCE(description,'')) >= 50
```

Note DELISTED is **included** so that re-appearing listings can be matched.
`category` column added in Sprint 8 (V1-Precision-1) so it propagates through
the LATERAL projection to be available for canonical comparison in
`_classify_pairs`.

**Self-join with LATERAL (Level 1 hard pre-filter):**
For each `p1`, find `p2` such that:

- `p2.id > p1.id` (canonical ordering, dedupes pairs)
- `p2.source_domain != p1.source_domain` (cross-source only)
- `1 - (p1.embedding <=> p2.embedding) > :sim_reject` (cosine over pgvector)
- price within 30% (or either NULL)
- size within 15% (or either NULL)
- pair **not** in `ai_duplicate_feedbacks` (admin blacklist)
- pair **not** already in the **same** cluster (pairs in _different_ clusters
  are intentionally re-evaluated)
- `ORDER BY p1.embedding <=> p2_inner.embedding LIMIT :per_p1_limit` — top-K
  nearest per `p1`.

**Output columns:** `a_id, b_id, a_phashes, b_phashes, a_category, b_category,
similarity` ordered by similarity DESC.

#### Step 4 — pre-Vision classification (`_classify_pairs`)

Returns a tuple `(edges, error_count)` (V1-Robust-1). The loop body is wrapped
in try/except — a single malformed row is logged and skipped via `classify_errors`
counter, not allowed to abort the whole run.

For each SQL row:

1. **Category prefilter (V1-Precision-1, Level 2.5).** Apply
   `_canonicalize_category()` to both sides. The dict `_CATEGORY_CANONICAL`
   maps ~30 raw category strings (case-insensitive) to ~6 broad families:
   - `RESIDENTIAL_DETACHED`: villa, house, detached house, maisonette,
     bungalow, etc.
   - `RESIDENTIAL_FLATS`: apartment, studio, apartment house
   - `COMPLEX`: complex, apartment complex, residential building, building
   - `LAND`: land, plot, agricultural land, site
   - `HOTEL`: hotel, ξενοδοχείο, hotel/commercial
   - `COMMERCIAL`: business, commercial property
   - `None` (junk): `&nbsp;`, other, investment

   If either side resolves to None or the two families don't match → skip pair,
   increment `category_skipped` counter. Logged at INFO at end of method.

2. **pHash matching:** `phash_matches = PHashService.count_matching(...)` with
   stock filter.

3. **Verdict logic:**
   - `similarity > settings.SIM_AUTO_MERGE` (0.985) → `merge_approved`
   - else if `phash_matches >= settings.PHASH_MIN_MATCHES` (2) → `merge_approved`
     (the **pHash bypass** — strong photo evidence overrides the cosine gray zone)
   - else → `merge_pending`

After Level 3+2.5 the funnel partitions all surviving pairs into
`merge_approved` (auto-confident) and `merge_pending` (gray zone).

#### Step 5 — Vision tie-breaker (`_apply_vision_tiebreaker`)

Gated by `settings.VISION_TIEBREAKER_ENABLED` (default **False**) **and** at
least one pending edge. Unchanged in Sprint 8 — same Vision behavior.

- Collect all `merge_pending` edges; sort by `(-similarity, a_id, b_id)` for
  deterministic budget allocation.
- Take the top `settings.VISION_MAX_PAIRS_PER_RUN` (= 50).
- For each, call `self._vision.decide_pair(session, a_id, b_id)`.

Vision verdicts:

- `verdict is None` → `vision_skipped += 1`, edge unchanged.
- `confidence < settings.VISION_CONFIDENCE_THRESHOLD` (0.8) →
  `vision_skipped += 1`, stays pending.
- `is_same=True` → edge verdict becomes `merge_approved`.
- `is_same=False` → verdict becomes `reject_vision`, and `_record_vision_reject`
  writes a feedback row.

#### Step 6 — DSU union (V1-Recall-1)

A local `_DSU` is built. For each edge **except** `reject_vision` (which is
excluded entirely):

- `dsu.add(a_id)`, `dsu.add(b_id)`.
- **if `merge_approved` OR `merge_pending`: `dsu.union(a_id, b_id)`**
  (V1-Recall-1 change — previously only approved unioned).
- the edge is stored in `edges_by_pair[(a_id,b_id)]`.

Result: pure-pending components are now unioned and persisted as PENDING
clusters for admin review (instead of being dropped as singletons). Tracked
via `gray_zone_clusters` stat when ALL edges in a component are pending.

`dsu.components()` returns `{root: [member_ids]}`.

#### Step 7 — persist each component (`_persist_component`) — V1-AdminAuthority

For each component's `member_ids`, wrapped in **SAVEPOINT**
(`async with session.begin_nested()`) per V1-Robust-1. Exceptions roll back
the savepoint only; other components and earlier session work survive. Errors
counted in `stats["component_errors"]`.

**Singleton (len==1):** skipped entirely; `approved_singleton_skipped += 1`.

**Multi-member:**

1. Load the `Property` objects; bail if < 2 survive.
2. Compute component-level metadata:
   - `max_sim` = max edge similarity → `ai_score`
   - `max_phash` = max photo matches → `phash_matches`
   - `any_pending` = any edge still `merge_pending`
   - `all_pending` (V1-Recall-1) = all edges are pending → `gray_zone_clusters` = 1
3. Find a **locked** cluster among existing member clusters.

**Locked-cluster, APPROVED status + new_members path — V1-AdminAuthority (NEW BEHAVIOR):**

Sprint 7 Task E (revert to PENDING + wipe verdict_locked + DELETE PowerObject)
is REMOVED. Replaced by:

- Query existing cluster members (those whose `cluster_id == locked.id`).
- For each `new_member`:
  - Collect per-pair evidence: edges from new_member to ACTUAL existing
    cluster members (not to other new_members).
  - If no direct evidence (indirect via other new_members) → skip; revisit
    in a future run.
  - INSERT into `cluster_member_proposals` via raw SQL with
    `ON CONFLICT (cluster_id, property_id) WHERE status='PENDING' DO NOTHING`
    — idempotent re-runs.
  - `proposals_created` counter incremented.
- **DO NOT** reassign `new_member.cluster_id` (stays NULL or wherever).
- **DO NOT** touch `locked_cluster.status`, `verdict_locked*`, `notes`.
- **DO NOT** delete PowerObject.
- **DO** refresh `ai_score` / `phash_matches` on cluster only if new max
  exceeds existing (new evidence merits update).

**Locked-cluster, PENDING status OR no new_members path:**

Existing behavior (`locked_preserved`):

- Reassign all `props` to locked cluster.
- Recompute `member_count` via `COUNT(*)` (Bug #66 fix).
- Update `ai_score`, `phash_matches`, `updated_at`.
- `locked_preserved += 1`.

**New / unlocked path:**

- `new_status = ClusterStatus.PENDING` — **always** (Sprint 7 decision).
- Try to reuse an existing **unlocked** cluster among members.
- Else create a fresh `PropertyCluster(status=PENDING, member_count=len(props),
ai_score=max_sim, phash_matches=max_phash, engine_version='1')` —
  `engine_version='1'` set explicitly per V1-EngineVersionExplicit (no longer
  relying on column server-default).
- Assign `cluster_id` on all props, flush, recompute `member_count` via
  `COUNT(*)`.
- `pending += 1`.

#### Step 8 — flush, orphan delete, commit

- `await session.flush()` — **critical** so the raw-SQL orphan delete sees
  in-memory `cluster_id` assignments.
- `_delete_orphan_clusters(session)`:

```sql
  DELETE FROM property_clusters c
  WHERE c.engine_version = '1'
    AND c.verdict_locked = false
    AND NOT EXISTS (SELECT 1 FROM properties p WHERE p.cluster_id = c.id)
  RETURNING c.id
```

The `engine_version = '1'` filter is the cross-engine bug fix — without it,
Engine 2's clusters (which track membership via the `cluster_v2_members`
junction) always look orphaned and were being deleted every night.

- `await session.commit()` and return `stats`.

### B.3 Database tables

| Table                      | Access                                                                | Columns used by Engine 1                                                                                                                                                          |
| -------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `properties`               | read + write `cluster_id`, `status`                                   | `id, embedding (Vector 1536), image_phashes, source_domain, cluster_id, price, size_sqm, description, content_hash, status, calc_municipality, category, bedrooms, land_size_sqm` |
| `property_clusters`        | read + insert + update + delete                                       | `id, status, member_count, engine_version, verdict_locked, verdict_locked_at, verdict_locked_by, ai_score, phash_matches, notes, created_at, updated_at`                          |
| `ai_duplicate_feedbacks`   | read (filter) + insert (vision reject)                                | `prop_a_id, prop_b_id, hash_a, hash_b, feedback_source`                                                                                                                           |
| `cluster_member_proposals` | insert (V1-AdminAuthority, Sprint 8)                                  | `cluster_id, property_id, ai_score, phash_matches, evidence_pairs (JSONB), status`                                                                                                |
| `power_properties`         | NO LONGER touched by Engine 1 (V1-AdminAuthority removed the DELETE)  | —                                                                                                                                                                                 |
| `media`                    | read (Vision only)                                                    | `property_id, image_url, local_file_path, created_at`                                                                                                                             |
| `agents`                   | NO LONGER touched (V1-AdminAuthority removed the revert-audit lookup) | —                                                                                                                                                                                 |

Membership mechanism: **`Property.cluster_id` FK** with `ondelete=SET NULL`.
Engine 1 never touches `cluster_v2_members`.

### B.4 External services & configuration

External calls:

- **pgvector HNSW cosine** via `embedding <=> embedding` in `_PAIR_SQL`,
  `_STOCK_PHASH_SQL`.
- **OpenAI GPT-4o Vision** (`vision_tiebreaker.py`) — only when Vision enabled.
- **cost_tracker** (`record_vision`) — Vision cost accounting.

Configuration consumed (all from `src/core/config.py::settings`, never from
`.env` directly):

| Setting                          | Value           | Used at                      |
| -------------------------------- | --------------- | ---------------------------- |
| `SIM_REJECT`                     | 0.920           | `_PAIR_SQL` floor            |
| `SIM_AUTO_MERGE`                 | 0.985           | `_classify_pairs` auto-merge |
| `MAX_PAIRS_PER_PROPERTY`         | 50              | LATERAL top-K                |
| `PHASH_MIN_MATCHES`              | 2               | pHash bypass                 |
| `PHASH_STOCK_MIN_PROPS`          | 5               | stock filter                 |
| `VISION_TIEBREAKER_ENABLED`      | False           | Vision gate                  |
| `VISION_MAX_PAIRS_PER_RUN`       | 50              | Vision budget                |
| `VISION_CONFIDENCE_THRESHOLD`    | 0.8             | Vision authority             |
| `PHashService.HAMMING_THRESHOLD` | 6 (class const) | photo match tolerance        |

Module-level constants (Sprint 8 additions):

- `_CATEGORY_CANONICAL` — V1-Precision-1 category mapping dict (~30 entries).
- `_QUALITY_GATE_SQL` — V1-QualityGateMetric observability query.

### B.5 Concurrency model

- **Fully async** (`async def`, `AsyncSession`), but **logically sequential** —
  Engine 1 processes pairs and components serially. The only concurrency is
  inside `VisionTiebreaker` (`asyncio.gather` over image encoding).
- **Single transaction with per-component SAVEPOINTs.** The entire `run()`
  executes inside one session and ends with **one `commit()`**. Per-component
  persist now uses `async with session.begin_nested()` (V1-Robust-1) so a
  single bad component doesn't poison the outer transaction.
- **No explicit locks.** Concurrency safety relies on the daily-sync being a
  single nightly process.
- The session is **shared** with `EmbeddingService.refresh_property_embeddings`,
  which commits per batch _before_ Engine 1 starts.

### B.6 Performance characteristics

- **Embedding refresh** (pre-step): O(changed properties), batched 100/call.
- **Quality Gate metric** (V1-QualityGateMetric): O(active properties) single
  aggregate scan with FILTER clauses; sub-second on ~2k rows.
- **`_PAIR_SQL`:** self-join bounded by `MAX_PAIRS_PER_PROPERTY` per p1, so
  candidate count ≈ O(n·50); cosine filter + HNSW keep nearest-neighbour search
  sub-linear per probe.
- **`_classify_pairs`:** O(pairs · photos_a · photos_b) for `count_matching`,
  bounded by photos-per-property. Category canonicalization is O(1) dict
  lookup per side.
- **Vision:** O(min(pending, 50)) sequential OpenAI calls.
- **`_persist_component`:** O(components), each doing a few small queries +
  one `COUNT(*)` per cluster. SAVEPOINT overhead is negligible (per-component
  nested transaction).
- **Bottlenecks:** (1) the `_PAIR_SQL` self-join; (2) serial Vision calls;
  (3) the `COUNT(*)` re-query per component (V1-CountBatch deferred).

### B.7 Error-handling pattern

Major improvement in Sprint 8:

- **Per-pair try/except in `_classify_pairs`** (V1-Robust-1): the loop body
  is wrapped; malformed row is logged + counted in `classify_errors`; run
  continues. Mirrors Engine 2's pattern.
- **Per-component SAVEPOINT in run()'s persist loop** (V1-Robust-1):
  `async with session.begin_nested()` wraps each `_persist_component` call.
  Exception → savepoint rolls back, other components proceed, error counted
  in `component_errors`.
- **Engine 1 wrapped in `_run_mdm_pipeline`** (V1-Defense): try/except +
  rollback + logger.exception. Engine 1 failure no longer blocks Engine 2
  or PowerObject phases.
- **Vision is defensive:** `decide_pair` swallows all exceptions and returns
  None, so Vision failures degrade gracefully.
- **Transaction-level safety:** the single trailing `commit()` means a
  catastrophic crash (outside savepoint scope) leaves the DB unchanged.

---

## Part B-addendum — Reference tables & worked examples

### B.8 The `stats` dictionary (run output) — Sprint 8 state

`run()` returns a `Dict[str, int]`. Active keys after Sprint 8:

| Key                                                                   | Meaning                                                           | When incremented                                   |
| --------------------------------------------------------------------- | ----------------------------------------------------------------- | -------------------------------------------------- |
| `approved_singleton_skipped`                                          | single-member components not persisted                            | per-component                                      |
| `pending`                                                             | new/updated clusters left PENDING for review                      | per-component (new/unlocked path)                  |
| `locked_preserved`                                                    | locked cluster touched but only metadata refreshed                | per-component (PENDING locked, or APPROVED+no new) |
| `proposals_created`                                                   | candidate-member proposals created (V1-AdminAuthority)            | per-component (APPROVED locked + new members)      |
| `gray_zone_clusters`                                                  | components persisted only due to pending-edge union (V1-Recall-1) | per-component if `all_pending`                     |
| `orphans_removed`                                                     | engine-1 clusters deleted for having no members                   | post-flush orphan sweep                            |
| `sold_cleaned`                                                        | SOLD properties detached from clusters                            | Step 0                                             |
| `vision_resolved`                                                     | pairs where Vision returned confident verdict                     | Vision step                                        |
| `vision_skipped`                                                      | Vision returned None or low confidence                            | Vision step                                        |
| `vision_feedback_added`                                               | reject_vision rows written to feedback                            | Vision step                                        |
| `classify_errors`                                                     | per-pair classification failures (V1-Robust-1)                    | `_classify_pairs` exception                        |
| `component_errors`                                                    | per-component persist failures (V1-Robust-1)                      | run() SAVEPOINT exception                          |
| `qg_active`, `qg_eligible`, `qg_dropped`, `qg_drop_no_*` (6 sub-keys) | Quality Gate observability (V1-QualityGateMetric)                 | Step 1                                             |

**Removed in Sprint 8** (formerly present, now dead and intentionally absent):

- `approved_merged` — auto-approve removed in Sprint 7; key cleaned up in
  V1-Cleanup.
- `reverted_to_pending` — Task E replaced by proposals (V1-AdminAuthority);
  key removed in V1-AdminAuthority-1d.

### B.8a The `_DSU` implementation — Sprint 8 change

Engine 1 ships its **own** disjoint-set-union, distinct from Engine 2's.
It is deliberately minimal:

- `__slots__ = ("_parent",)` — only a parent map, **no union-by-rank**.
- `add(x)` — idempotent insert.
- `find(x)` — iterative root walk + path compression.
- `union(a, b)` — attach `find(a)`'s root under `find(b)`'s root unconditionally.
- `components()` — `defaultdict(list)` grouping every node under its root.

**Sprint 8 change (V1-Recall-1):** the DSU union loop in `run()` now calls
`union` for BOTH `merge_approved` AND `merge_pending` edges. `reject_vision`
edges are still excluded entirely. Result: gray-zone-only components now form
2+ member clusters that get persisted as PENDING for admin review.

### B.9 The `_EdgeMeta` dataclass

Defined as a module-level frozen dataclass. One per classified candidate pair.
Unchanged in Sprint 8 (fields and types preserved):

| Field           | Type          | Source                                                             |
| --------------- | ------------- | ------------------------------------------------------------------ |
| `a_id`, `b_id`  | str           | SQL row (canonical `a_id < b_id` from `_PAIR_SQL` `p2.id > p1.id`) |
| `similarity`    | float         | `1 - (embedding <=> embedding)` cosine                             |
| `phash_matches` | int           | `PHashService.count_matching` (stock-filtered)                     |
| `verdict`       | str           | `merge_approved` \| `merge_pending` \| `reject_vision`             |
| `vision_trace`  | Optional[str] | human-readable Vision rationale (debug/logs)                       |

### B.10 Cluster state transitions driven by Engine 1 — Sprint 8

                (new multi-member component, no locked cluster)

(none) ────────────────────────────────────────────────────────▶ PENDING (engine_version='1')
│
admin approves elsewhere (sets verdict_locked=True) ───────────────────┤
▼
APPROVED + verdict_locked
│
│
┌─ Sprint 7 Task E (REMOVED in Sprint 8 V1-AdminAuthority) │
│ │
│ new member joins locked+APPROVED cluster │
│ ──▶ formerly: revert to PENDING + delete PowerObject │
│ ──▶ now: stays APPROVED + verdict_locked unchanged │
│ create cluster_member_proposals row(s) │
│ PowerObject preserved │
└─ │
│
ALL ORIGINAL FLOW PRESERVED: │

locked_preserved (PENDING locked refresh) │
orphan delete (engine_version='1' scoped) │
│
all members leave / become SOLD ──▶ cluster has 0 members ──▶ DELETEd by \_delete_orphan_clusters
(only if engine_version='1' AND NOT verdict_locked)

**Proposal lifecycle** (admin-driven, outside Engine 1):
cluster_member_proposals row created by Engine 1 (status=PENDING)
│
admin reviews via UI ──────────┤
├──▶ APPROVE: property.cluster_id = cluster.id
│ cluster.member_count++
│ proposal.status = APPROVED
│
└──▶ REJECT: ai_duplicate_feedbacks rows
inserted for each evidence pair
(cluster + property unchanged)
proposal.status = REJECTED

Engine 1 **never** writes `APPROVED` itself (status hardcoded PENDING in
`_persist_component`). The only path to APPROVED is an admin action elsewhere
in the app.

### B.11 Worked example — a typical duplicate pair

Suppose property **A** (`gl-real-estate.gr`, villa, €480k, 150 m², 12 photos)
and **B** (`halkidiki-estate.gr`, villa, €495k, 148 m², 10 photos) are the same
physical villa.

1. Both pass the **Quality Gate**.
2. The **LATERAL pre-filter** keeps the pair: cross-source ✓; price diff 3.0%
   < 30% ✓; size diff 1.3% < 15% ✓; not blacklisted; not co-clustered;
   cosine 0.972 > 0.920 ✓.
3. `_classify_pairs`:
   - **Category canonical (V1-Precision-1):** both → `RESIDENTIAL_DETACHED`.
     Pair survives.
   - Cosine 0.972 is NOT > 0.985, checks pHash. 4 photos match (after stock
     filtering) ≥ 2 → **pHash bypass** → `merge_approved`.
4. DSU unions A and B. Component `{A, B}` has `max_sim=0.972`, `max_phash=4`,
   `all_pending=False` (has approved edge).
5. `_persist_component`: SAVEPOINT begins. No existing/locked cluster → create
   `PropertyCluster(status=PENDING, engine_version='1', ai_score=0.972,
phash_matches=4)`; set `A.cluster_id = B.cluster_id = new_id`.
6. SAVEPOINT released. The cluster appears in admin queue as PENDING.

### B.12 Worked example — a gray-zone pair (V1-Recall-1)

Same pair but only **1** photo matches and cosine is 0.950:

1–2. Same survival through gate and pre-filter. 3. `_classify_pairs`: categories match (V1-Precision-1 OK). 0.950 ≤ 0.985,
and 1 < 2 → `merge_pending`. 4. **DSU change (V1-Recall-1):** both nodes are added AND unioned (previously:
added but not unioned). Component `{A, B}` forms. 5. `_persist_component`: `all_pending = True` → `gray_zone_clusters = 1`
counter. Cluster created with status=PENDING; admin reviews and decides.

If Vision were enabled: pair could be promoted to `merge_approved` (then
treated as B.11) or `reject_vision` (blacklisted in feedbacks, never re-asked).

### B.13 Worked example — new candidate for APPROVED cluster (V1-AdminAuthority)

Cluster X is APPROVED + locked: `{A, B, C}` (3 members, admin-approved
previously). New property F is scraped from a different source.

1. `_PAIR_SQL` returns pairs F-A, F-B, F-C (all > 0.92 cosine, cross-source).
2. `_classify_pairs`: all three categories match (V1-Precision-1 OK).
   F-A scores 0.96 with 3 photo matches → `merge_approved` (pHash bypass).
   F-B scores 0.94 with 1 photo match → `merge_pending`.
   F-C scores 0.93 with 0 photo matches → `merge_pending`.
3. DSU unions all 4 (V1-Recall-1: pending union enabled). Component
   `{A, B, C, F}` forms.
4. `_persist_component`: load props. Cluster X found via members A/B/C.
   `locked_cluster = X`, status=APPROVED, `new_members = [F]`.
5. **V1-AdminAuthority path triggered.** Query existing cluster members:
   `existing_member_ids = {A_id, B_id, C_id}`.
6. For new_member F:
   - Evidence pairs: F→A (0.96, 3), F→B (0.94, 1), F→C (0.93, 0).
   - max_ev_sim = 0.96, max_ev_phash = 3.
   - INSERT row into `cluster_member_proposals` (cluster_id=X, property_id=F,
     ai_score=0.96, phash_matches=3, evidence_pairs=JSON, status=PENDING).
   - `out["proposals_created"] = 1`.
7. Cluster X: refresh ai_score / phash_matches (if higher than existing);
   DO NOT touch status, verdict_locked, members, PowerObject.
8. F.cluster_id remains NULL (or whatever it was).
9. Commit. Admin sees proposal in queue, decides APPROVE (F joins X) or REJECT
   (F-A, F-B, F-C blacklisted; X unchanged; F may form its own cluster
   eventually).

---

## Part C — Critique (Sprint 8 update)

### Strengths

1. **Clean funnel with cost-aware ordering.** Cheap SQL filters run before
   expensive cosine, which runs before category canonical (V1-Precision-1
   Sprint 8 addition), which runs before even-more-expensive pHash, before
   Vision. Each level only sees what survived the previous one.
2. **Stock-photo suppression is genuinely smart.** `_STOCK_PHASH_SQL` removes
   the single biggest source of false photo matches using a data-driven
   `COUNT(DISTINCT id) > 5` rule rather than a hardcoded blacklist.
3. **Admin authority is now FULLY protected (Sprint 8 V1-AdminAuthority).**
   APPROVED clusters are sacrosanct — new evidence creates proposal records,
   never reverts. PowerObjects preserved. Admin decisions are final.
4. **Deterministic Vision budgeting** and idempotent feedback writes
   (`ON CONFLICT DO NOTHING`) show attention to reproducibility.
5. **The `engine_version='1'` orphan-delete scoping** is a correct fix to a
   real cross-engine data-loss bug. Sprint 8 also makes Engine 1 set
   `engine_version='1'` EXPLICITLY on cluster creation (V1-EngineVersionExplicit).
6. **`member_count` recomputed from authoritative `COUNT(*)`** (Bug #66) avoids
   drift.
7. **Production resilience (Sprint 8 V1-Robust-1 + V1-Defense).** Per-pair
   try/except, per-component SAVEPOINT, outer try/except in daily_sync. One
   bad row no longer aborts the run. Engine 1 failure no longer blocks
   Engine 2 / PowerObject.
8. **Recall captured (Sprint 8 V1-Recall-1).** Gray-zone components persist
   as PENDING clusters with `gray_zone_clusters` stat — no more silent loss
   of pending-only signal.
9. **Quality Gate observability (Sprint 8 V1-QualityGateMetric).** Drop count
   - per-reason breakdown surfaced in stats and logs. Silent recall loss is
     now measurable.

### Weaknesses (Sprint 8 status)

| #   | Original weakness                         | Sprint 8 status                                                                      |
| --- | ----------------------------------------- | ------------------------------------------------------------------------------------ |
| 1   | No per-pair error isolation               | **FIXED** by V1-Robust-1 (try/except in `_classify_pairs` + SAVEPOINT per component) |
| 2   | `_run_mdm_pipeline` not wrapping Engine 1 | **FIXED** by V1-Defense                                                              |
| 3   | `merge_pending` edges silently dropped    | **FIXED** by V1-Recall-1 (DSU unions pending)                                        |
| 4   | `approved_merged` dead                    | **FIXED** by V1-Cleanup (key + dead branch removed)                                  |
| 5   | Quality Gate silent recall loss           | **FIXED** by V1-QualityGateMetric                                                    |
| 6   | `COUNT(*)` per component                  | **OPEN** — V1-CountBatch deferred to Sprint 9 (P3)                                   |
| 7   | `engine_version` server-default reliance  | **FIXED** by V1-EngineVersionExplicit                                                |

**Remaining open items (Sprint 9 candidates):**

- **V1-CountBatch (P3).** Replace per-component `COUNT(*)` recompute with
  single `GROUP BY cluster_id` post-flush. ~50ms savings, not urgent.
- **V1-Recall-Investigate-216.** Quality Gate diagnostic showed 216
  properties dropped for "no bed_or_land" — verify these are legitimately
  hotels/commercial (expected drop) vs. mismapped data (real recall loss).
- **V1-EmbeddingsBackfill.** 60 properties dropped for missing
  embedding/content_hash — investigate why EmbeddingService didn't process
  them.

### Suggested improvements (Sprint 9 ranked)

| Impact  | Suggestion                                                                                                     | Anchor                          |
| ------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| **MED** | V1-AdminAuthority-1e: admin UI for proposal queue (Approve/Reject buttons + drill-down on evidence_pairs JSON) | Frontend separate from backend  |
| **MED** | V1-Recall-Investigate-216: deep-dive on 216 dropped properties                                                 | `qg_drop_no_bed_or_land` metric |
| **LOW** | V1-CountBatch: single GROUP BY for member_count                                                                | `_persist_component`            |
| **LOW** | Greximo polish: HTMLEntity-Decode, MultiCat-Clean, Breadcrumb-Fallback                                         | Scraper, not engine             |

### Inconsistencies between engines (Engine 1 perspective, Sprint 8 update)

1. **Membership model diverges by design.** Engine 1 uses `Property.cluster_id`
   FK; Engine 2 uses `cluster_v2_members` junction. **Intentional** — enables
   dual-engine parallel operation. Unchanged in Sprint 8.
2. **Error handling diverges → RESOLVED.** Both engines now isolate per-pair
   errors (V1-Robust-1 brought Engine 1 to parity with Engine 2's pattern).
3. **Confidence signals diverge.** Engine 1's `ai_score` = max raw cosine
   (or pHash count). Engine 2's `ai_score` = mean of pair confidences in `[0,1]`.
   Same column, two semantics. **Unchanged** — would require migration to fix.
4. **Vision vs LLM-text divergence.** Engine 1's expensive arbiter looks at
   photos (GPT-4o Vision); Engine 2's reads descriptions (gpt-4o-mini text).
   Could disagree on the same pair. **Intentional.**
5. **Category handling → RESOLVED.** V1-Precision-1 added Engine 1's own
   self-contained `_canonicalize_category()` (not imported from Engine 2, so
   removing Engine 2 won't break Engine 1). Both engines now reject
   cross-family pairs.
6. **Admin authority protection diverges.** Engine 1 (Sprint 8) creates
   `cluster_member_proposals` for APPROVED clusters; Engine 2 has its own
   per-engine flow (`mismerge_flags` etc.). **Intentional** — different
   data models per engine.

---

_End of Engine 1 map. Last updated: Sprint 8 V1 refactor, 2026-05-26._

🎯 Document updated reflecting all 11 Sprint 8 V1 changes. Key updates:

Funnel levels table expanded (Level -1, 2.5 added)
Call graph shows new error handling + proposals path
Step trace section includes V1-AdminAuthority new behavior in detail
B.10 state transition diagram updated to show proposal flow
B.13 worked example added for V1-AdminAuthority path
Part C weaknesses table now shows before/after status
Inconsistencies updated — Engine 1 now matches Engine 2 on error handling + category
