# Shopify — GMC Feed Enrichment Pipeline

Enriches a Shopify catalog with the product attributes Google Merchant Center
requires for apparel (`gender`, `age_group`, `color`, `size`) and optimized
titles, at catalog scale (50k+ SKUs), without touching products one-by-one in
the Shopify admin.

## Pipeline Phases

| Phase | Tool | Status |
|-------|------|--------|
| 1. Export + census | `tools/clis/shopify-census.js` | ✓ Built |
| 2. Rules-layer enrichment (normalize metafields, tags, options → GMC enums) | `shopify-gmc-enrich.js` | Planned |
| 3. AI classification for rules-layer leftovers + title rewrites (Claude Batches API) | `shopify-gmc-classify.js` | Planned |
| 4. Review + bulk write-back (`metafieldsSet`) | `shopify-gmc-apply.js` | Planned |
| 5. Feed app mapping (e.g. Simprosys metafield → GMC attribute) + scheduled incremental runs | — | Planned |

## Authentication

- **Type**: Admin API access token from a custom app
- **Header**: `X-Shopify-Access-Token: {token}`
- **Setup**: Shopify admin → Settings → Apps and sales channels → Develop apps
  → Create an app → Configure Admin API scopes → Install app → Reveal token
- **Scopes**: `read_products` (Phase 1); `write_products` (Phases 2–4)

Environment variables:

```bash
export SHOPIFY_STORE_DOMAIN="your-store.myshopify.com"
export SHOPIFY_ACCESS_TOKEN="shpat_..."
export SHOPIFY_API_VERSION="2026-01"   # optional
```

## Phase 1: Export + Census

Bulk-exports the catalog (Admin GraphQL Bulk Operations API — one async job,
no pagination) and computes how much of each GMC attribute the free rules
layer can resolve, per vendor.

```bash
# Export active products + run census in one step
node tools/clis/shopify-census.js run

# Or step by step
node tools/clis/shopify-census.js export --status active --out products.jsonl
node tools/clis/shopify-census.js census --in products.jsonl --out-dir census

# Include drafts/archived
node tools/clis/shopify-census.js export --status any

# Preview the GraphQL without sending anything
node tools/clis/shopify-census.js export --dry-run
```

Outputs:

- `products.jsonl` — full catalog export (one product per line): title,
  vendor, status, tags, category, options, variant counts, and the
  `custom.brand`, `custom.gender`, `attribute.google_age_group` metafields
- `census/census.md` — human-readable report: overall + per-vendor coverage
  for gender/age/color/size, split by resolution source (metafield, variant
  option, tag, title), plus normalization inputs (distinct option names,
  unmapped metafield values)
- `census/census.json` — full stats for downstream tooling

The summary printed to stdout includes `needsAi` counts — the products per
attribute the rules layer cannot resolve, i.e. the AI layer's workload.

### How the census resolves each attribute

| Attribute | Resolution order |
|-----------|-----------------|
| Gender | `custom.gender` metafield → gender tag (Girls/Boys/Womens/...) → title keyword |
| Age group | `attribute.google_age_group` metafield → age tag (Infant/Toddler/Kids/...) → title keyword |
| Color | Color variant option → color tag |
| Size | Size variant option → size tag (One Size, 0-24M, 2T, ...) → size-in-title pattern |

Products unresolved by all sources are the AI layer's input. Metafield values
not yet covered by the normalization maps are flagged in the report so the
maps can be extended before Phase 2.

## Notes

- The census is read-only; nothing is written to the store.
- Bulk operations allow one active export per shop. If a run was interrupted,
  check progress with `status --id <gid>` (the operation ID is printed to
  stderr at start).
- GMC-valid enums targeted by the pipeline: gender `male|female|unisex`;
  age_group `newborn|infant|toddler|kids|adult`.
