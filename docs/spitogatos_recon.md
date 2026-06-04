
---
## CRITICAL FINDING (continued recon session)

### Card bifurcation: VIP vs Regular
On each search page (30 cards), only the first ~4 cards are **VIP/Featured**
and have the full structured title attribute:
  `"Sale,Home,Detached house, 180m²,€700,000,Pefkochori (Pallini)"`

The remaining ~26 cards (regular listings) have only short title:
  `"Maisonette, 120m²"` (type + size only — no price/location)

**Implication for architecture**: title-attribute-only Phase 1 strategy only
captures 13% of inventory. Need either:
- Different selectors for price/location within regular cards
- Or skip search-page extraction entirely, just collect URLs, then enrich
  via detail page visits (which give MUCH richer data anyway)

### Recommended revised architecture
- **Phase 1 (Sprint 12 Day 1)**: collect URLs only from search pages, visit
  detail page for each — extract everything from there (description, full
  spec table, features, agent, hi-res images). The detail page HTML is
  clean and well-structured, no VIP/regular bifurcation.
- 3,229 listings × ~3s detail fetch = ~3 hours full run (acceptable)
- Resume-capable to allow incremental builds

### PropertyTemplate schema (verified)
Field name is `images: List[str]` (NOT `image_urls`). Other relevant:
url, site_property_id, source_domain, title, category, price, currency,
size_sqm, land_size_sqm, location_raw, latitude, longitude, description,
bedrooms, bathrooms, year_built, levels, extra_features (dict).

## Sprint 12 Day 1 Final State (2026-06-04 ~02:00)

**Properties in DB**: 120 (Batch 1: 100 new + 20 initial)
**Coords coverage**: 100% (120/120)
**Images**: ~840 total

### Architecture validated
- collect_urls + fetch_details URL flow
- Inline coord lookup via location_areas (NOT script-tag — those return shared search-center coords)
- 4-level breadcrumb extraction → calc_prefecture/municipality/area/subarea
- Enhanced variants (Paralia, ch→h, i→y, multi-word)
- Municipality centroid fallback for refdata gaps

### Refdata gaps remaining
- **Tripotamos** (Halkidiki) — village not in OSM, 2 properties fall back to Sithonia centroid

### Next session
- Batch 2 (200) — pages 7-14
- Batch 3 (200) — pages 14-21
- Target: 520 total Spitogatos properties
- Future: scale to full 3,229 (108 pages) via daily_sync
