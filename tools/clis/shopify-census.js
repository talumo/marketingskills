#!/usr/bin/env node

// Shopify catalog export + census for Google Merchant Center enrichment.
//
// Phase 1 of the GMC enrichment pipeline: bulk-exports the product catalog
// via the Admin GraphQL Bulk Operations API, then computes per-vendor
// coverage stats for the four GMC attributes (gender, age_group, color,
// size) so the rules layer (Phase 2) can be built from real data.
//
// Usage:
//   node shopify-census.js export [--status active|draft|archived|any] [--out products.jsonl] [--timeout-min 30] [--dry-run]
//   node shopify-census.js status --id <bulk-operation-gid>
//   node shopify-census.js census --in products.jsonl [--out-dir census]
//   node shopify-census.js run [export+census options]
//
// Env:
//   SHOPIFY_STORE_DOMAIN  e.g. my-store.myshopify.com
//   SHOPIFY_ACCESS_TOKEN  Admin API token with read_products scope
//   SHOPIFY_API_VERSION   optional, default 2026-01

const fs = require('fs')
const path = require('path')
const readline = require('readline')
const { pipeline } = require('stream/promises')

const API_VERSION = process.env.SHOPIFY_API_VERSION || '2026-01'

function parseArgs(argv) {
  const result = { _: [] }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg.startsWith('--')) {
      const key = arg.slice(2)
      const next = argv[i + 1]
      if (next && !next.startsWith('--')) {
        result[key] = next
        i++
      } else {
        result[key] = true
      }
    } else {
      result._.push(arg)
    }
  }
  return result
}

const args = parseArgs(process.argv.slice(2))
const [cmd] = args._

function requireEnv() {
  const domain = process.env.SHOPIFY_STORE_DOMAIN
  const token = process.env.SHOPIFY_ACCESS_TOKEN
  if (!domain || !token) {
    console.error(JSON.stringify({ error: 'SHOPIFY_STORE_DOMAIN and SHOPIFY_ACCESS_TOKEN environment variables required' }))
    process.exit(1)
  }
  return { domain: domain.replace(/^https?:\/\//, '').replace(/\/$/, ''), token }
}

async function graphql(query, variables) {
  if (args['dry-run']) {
    const domain = (process.env.SHOPIFY_STORE_DOMAIN || '<SHOPIFY_STORE_DOMAIN>').replace(/^https?:\/\//, '').replace(/\/$/, '')
    const url = `https://${domain}/admin/api/${API_VERSION}/graphql.json`
    return { _dry_run: true, url, headers: { 'X-Shopify-Access-Token': '***' }, body: { query, variables } }
  }
  const { domain, token } = requireEnv()
  const url = `https://${domain}/admin/api/${API_VERSION}/graphql.json`
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'X-Shopify-Access-Token': token,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query, variables }),
  })
  const json = await res.json()
  if (json.errors) {
    throw new Error(`GraphQL errors: ${JSON.stringify(json.errors)}`)
  }
  return json.data
}

// --- Export -----------------------------------------------------------

const BULK_PRODUCT_QUERY = (statusFilter) => `
{
  products${statusFilter ? `(query: "${statusFilter}")` : ''} {
    edges {
      node {
        id
        title
        vendor
        productType
        status
        tags
        hasOnlyDefaultVariant
        variantsCount { count }
        category { id fullName }
        options { name values }
        brandMeta: metafield(namespace: "custom", key: "brand") { value }
        genderMeta: metafield(namespace: "custom", key: "gender") { value }
        ageGroupMeta: metafield(namespace: "attribute", key: "google_age_group") { value }
      }
    }
  }
}`

const START_MUTATION = `
mutation StartExport($q: String!) {
  bulkOperationRunQuery(query: $q) {
    bulkOperation { id status }
    userErrors { field message }
  }
}`

const POLL_QUERY = `
query PollExport($id: ID!) {
  node(id: $id) {
    ... on BulkOperation {
      id status errorCode createdAt completedAt objectCount fileSize url partialDataUrl
    }
  }
}`

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function runExport() {
  const status = args.status || 'active'
  const statusFilter = status === 'any' ? null : `status:${status}`
  const outFile = args.out || 'products.jsonl'

  const start = await graphql(START_MUTATION, { q: BULK_PRODUCT_QUERY(statusFilter) })
  if (start._dry_run) return start

  const errors = start.bulkOperationRunQuery.userErrors
  if (errors && errors.length) {
    throw new Error(`bulkOperationRunQuery: ${JSON.stringify(errors)}`)
  }
  const opId = start.bulkOperationRunQuery.bulkOperation.id
  console.error(`Bulk operation started: ${opId}`)

  const timeoutMs = (Number(args['timeout-min']) || 30) * 60 * 1000
  const startedAt = Date.now()
  let op
  while (true) {
    await sleep(5000)
    const data = await graphql(POLL_QUERY, { id: opId })
    op = data.node
    console.error(`  ${op.status}${op.objectCount ? ` (${op.objectCount} objects)` : ''}`)
    if (op.status === 'COMPLETED') break
    if (op.status === 'FAILED' || op.status === 'CANCELED') {
      throw new Error(`Bulk operation ${op.status}: ${op.errorCode || 'unknown error'}`)
    }
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error(`Timed out after ${timeoutMs / 60000} minutes. Check later with: status --id ${opId}`)
    }
  }

  if (!op.url) {
    console.error('Bulk operation completed with no results (empty catalog for this filter).')
    fs.writeFileSync(outFile, '')
    return { operation: op, file: outFile, objects: 0 }
  }

  const res = await fetch(op.url)
  if (!res.ok) throw new Error(`Download failed: HTTP ${res.status}`)
  await pipeline(res.body, fs.createWriteStream(outFile))
  return { operation: { id: op.id, status: op.status, objectCount: op.objectCount, fileSize: op.fileSize }, file: outFile }
}

async function runStatus() {
  if (!args.id) return { error: '--id <bulk-operation-gid> required' }
  const data = await graphql(POLL_QUERY, { id: args.id })
  return data._dry_run ? data : data.node
}

// --- Census ------------------------------------------------------------

const SIZE_OPTION_NAMES = ['size', 'sizes', 'sz', 'shoe size', 'us size', 'clothing size']
const COLOR_OPTION_NAMES = ['color', 'colour', 'colors', 'colours', 'shade', 'colorway']

const COLOR_WORDS = new Set([
  'black', 'white', 'off white', 'red', 'blue', 'navy', 'navy blue', 'royal blue', 'light blue', 'sky blue',
  'green', 'lime', 'olive', 'sage', 'emerald', 'mint', 'teal', 'turquoise', 'aqua', 'yellow', 'mustard',
  'orange', 'rust', 'coral', 'peach', 'apricot', 'salmon', 'purple', 'lavender', 'lilac', 'violet', 'plum',
  'pink', 'hot pink', 'dark pink', 'light pink', 'blush', 'rose', 'fuchsia', 'magenta', 'brown', 'tan',
  'beige', 'khaki', 'camel', 'chocolate', 'espresso', 'taupe', 'sand', 'stone', 'ivory', 'cream', 'natural',
  'gray', 'grey', 'charcoal', 'heather grey', 'heather gray', 'slate', 'silver', 'gold', 'bronze', 'copper',
  'champagne', 'nude', 'burgundy', 'maroon', 'wine', 'denim', 'indigo', 'cobalt', 'periwinkle', 'mauve',
  'multi', 'multicolor', 'multi color', 'rainbow', 'clear',
])

const GENDER_TAG_MAP = {
  girls: 'female', girl: 'female', womens: 'female', women: 'female',
  boys: 'male', boy: 'male', mens: 'male', men: 'male',
  unisex: 'unisex',
}

const AGE_TAG_MAP = {
  newborn: 'newborn',
  infant: 'infant', baby: 'infant', '0-24m': 'infant',
  toddler: 'toddler',
  kids: 'kids', kid: 'kids', youth: 'kids',
  adult: 'adult', womens: 'adult', women: 'adult', mens: 'adult', men: 'adult',
}

// Known values seen in custom.gender / attribute.google_age_group metafields,
// mapped to GMC-valid enums. Unmapped values are reported in the census so
// the map can be extended before Phase 2.
const GENDER_META_MAP = {
  girls: 'female', girl: 'female', female: 'female', womens: 'female', women: 'female',
  boys: 'male', boy: 'male', male: 'male', mens: 'male', men: 'male',
  unisex: 'unisex',
}

const AGE_META_MAP = {
  newborn: 'newborn',
  infant: 'infant', baby: 'infant',
  toddler: 'toddler', 'little girls': 'kids', 'little boys': 'kids', 'little kid': 'kids',
  kids: 'kids', 'big girls': 'kids', 'big boys': 'kids', 'big kid': 'kids', youth: 'kids',
  adult: 'adult', adults: 'adult', womens: 'adult', mens: 'adult',
}

const SIZE_TAG_PATTERNS = [
  /^one size$/i,
  /^\d{1,2}(-\d{1,2})?\s?m(onths?)?$/i,        // 3M, 0-24M, 18 Months
  /^(xxs|xs|s|m|l|xl|xxl|2xl|3xl)$/i,
  /^\d{1,2}t$/i,                                // 2T-5T
  /^\d{1,2}x$/i,                                // 6X
  /^\d{1,2}(\.\d)?\s(kids|toddler|youth|infant|little kid|big kid)$/i, // 11 Kids, 5 Toddler
  /^\d{1,2}(\/\d{1,2})?$/,                      // 4, 10, 6/7
]

const TITLE_SIZE_PATTERNS = [
  /\bsizes?\s+\d/i,
  /\b(xxs|xs|s|m|l|xl|xxl)\s*(-|–|to)\s*(xs|s|m|l|xl|xxl|2xl)\b/i,
  /\b\d{1,2}\s*(-|–|to)\s*\d{1,2}\s?(m|months?|t|y|years?)\b/i,
  /\b\d{1,2}(m|t|x)\b/i,
  /\b\d{1,2}\s*-\s*\d{1,2}m\b/i,
]

const TITLE_GENDER_PATTERN = /\b(girls?|boys?|womens?|women|mens?|men|unisex)\b/i
const TITLE_AGE_PATTERN = /\b(newborn|infants?|bab(y|ies)|toddlers?|kids?|youth|girls?|boys?|womens?|women|mens?|men)\b/i

function titleAgeBucket(title) {
  const m = title.match(TITLE_AGE_PATTERN)
  if (!m) return null
  const w = m[1].toLowerCase()
  if (w.startsWith('newborn')) return 'newborn'
  if (w.startsWith('infant') || w.startsWith('bab')) return 'infant'
  if (w.startsWith('toddler')) return 'toddler'
  if (w.startsWith('women') || w.startsWith('men')) return 'adult'
  return 'kids'
}

function newAttrStats() {
  return { byMetafield: 0, byOption: 0, byTag: 0, byTitle: 0, unresolved: 0 }
}

function newVendorStats() {
  return {
    products: 0,
    byStatus: {},
    singleVariant: 0,
    apparelCategory: 0,
    noCategory: 0,
    hasBrandMeta: 0,
    brandDiffersFromVendor: 0,
    hasSizeOption: 0,
    hasColorOption: 0,
    gender: newAttrStats(),
    ageGroup: newAttrStats(),
    color: newAttrStats(),
    size: newAttrStats(),
  }
}

function tally(map, key) {
  map[key] = (map[key] || 0) + 1
}

function resolveProduct(p, stats, globals) {
  const s = stats
  s.products++
  tally(s.byStatus, p.status || 'UNKNOWN')
  if (p.hasOnlyDefaultVariant) s.singleVariant++

  const categoryName = p.category && p.category.fullName
  if (!categoryName) s.noCategory++
  else if (categoryName.startsWith('Apparel & Accessories')) s.apparelCategory++

  const brand = p.brandMeta && p.brandMeta.value
  if (brand) {
    s.hasBrandMeta++
    if (brand.trim().toLowerCase() !== (p.vendor || '').trim().toLowerCase()) s.brandDiffersFromVendor++
  }

  const options = (p.options || []).filter((o) => o && o.name)
  const optionNames = options.map((o) => o.name.toLowerCase().trim())
  for (const name of optionNames) tally(globals.optionNames, name)
  const hasSizeOption = optionNames.some((n) => SIZE_OPTION_NAMES.includes(n))
  const hasColorOption = optionNames.some((n) => COLOR_OPTION_NAMES.includes(n))
  if (hasSizeOption) s.hasSizeOption++
  if (hasColorOption) s.hasColorOption++

  const tags = (p.tags || []).map((t) => String(t).toLowerCase().trim())
  const title = p.title || ''

  // gender: metafield > tag > title
  const genderMetaRaw = p.genderMeta && p.genderMeta.value
  if (genderMetaRaw) {
    tally(globals.genderMetaValues, genderMetaRaw)
    if (!GENDER_META_MAP[genderMetaRaw.toLowerCase().trim()]) tally(globals.unmappedGenderMeta, genderMetaRaw)
  }
  const genderTags = new Set(tags.filter((t) => GENDER_TAG_MAP[t]).map((t) => GENDER_TAG_MAP[t]))
  if (genderMetaRaw && GENDER_META_MAP[genderMetaRaw.toLowerCase().trim()]) s.gender.byMetafield++
  else if (genderTags.size > 0) s.gender.byTag++
  else if (TITLE_GENDER_PATTERN.test(title)) s.gender.byTitle++
  else s.gender.unresolved++

  // age group: metafield > tag > title
  const ageMetaRaw = p.ageGroupMeta && p.ageGroupMeta.value
  if (ageMetaRaw) {
    tally(globals.ageMetaValues, ageMetaRaw)
    if (!AGE_META_MAP[ageMetaRaw.toLowerCase().trim()]) tally(globals.unmappedAgeMeta, ageMetaRaw)
  }
  const hasAgeTag = tags.some((t) => AGE_TAG_MAP[t])
  if (ageMetaRaw && AGE_META_MAP[ageMetaRaw.toLowerCase().trim()]) s.ageGroup.byMetafield++
  else if (hasAgeTag) s.ageGroup.byTag++
  else if (titleAgeBucket(title)) s.ageGroup.byTitle++
  else s.ageGroup.unresolved++

  // color: option > tag
  const hasColorTag = tags.some((t) => COLOR_WORDS.has(t))
  if (hasColorOption) s.color.byOption++
  else if (hasColorTag) s.color.byTag++
  else s.color.unresolved++

  // size: option > tag > title
  const hasSizeTag = tags.some((t) => SIZE_TAG_PATTERNS.some((re) => re.test(t)))
  const hasTitleSize = TITLE_SIZE_PATTERNS.some((re) => re.test(title))
  if (hasSizeOption) s.size.byOption++
  else if (hasSizeTag) s.size.byTag++
  else if (hasTitleSize) s.size.byTitle++
  else s.size.unresolved++
}

function pct(n, total) {
  if (!total) return '0%'
  return `${Math.round((n / total) * 100)}%`
}

function attrResolved(a, total) {
  return pct(total - a.unresolved, total)
}

async function runCensus() {
  const inFile = args.in || 'products.jsonl'
  const outDir = args['out-dir'] || 'census'
  if (!fs.existsSync(inFile)) {
    return { error: `Input file not found: ${inFile}. Run the export command first.` }
  }
  fs.mkdirSync(outDir, { recursive: true })

  const vendors = {}
  const overall = newVendorStats()
  const newGlobals = () => ({
    optionNames: {},
    genderMetaValues: {},
    ageMetaValues: {},
    unmappedGenderMeta: {},
    unmappedAgeMeta: {},
  })
  const globals = newGlobals()
  // The overall pass re-runs resolveProduct; its value tallies go to a
  // throwaway object so globals aren't double-counted.
  const discard = newGlobals()

  const rl = readline.createInterface({
    input: fs.createReadStream(inFile),
    crlfDelay: Infinity,
  })
  let lines = 0
  for await (const line of rl) {
    if (!line.trim()) continue
    const node = JSON.parse(line)
    // Bulk export emits one product per line for this query (no nested
    // connections). Skip any non-product child rows defensively.
    if (!node.id || !String(node.id).includes('/Product/')) continue
    lines++
    const vendor = node.vendor || '(no vendor)'
    if (!vendors[vendor]) vendors[vendor] = newVendorStats()
    resolveProduct(node, vendors[vendor], globals)
    resolveProduct(node, overall, discard)
  }

  const census = {
    generatedAt: new Date().toISOString(),
    input: inFile,
    totalProducts: overall.products,
    overall,
    vendors,
    distinctOptionNames: globals.optionNames,
    genderMetafieldValues: globals.genderMetaValues,
    ageGroupMetafieldValues: globals.ageMetaValues,
    unmappedGenderMetafieldValues: globals.unmappedGenderMeta,
    unmappedAgeGroupMetafieldValues: globals.unmappedAgeMeta,
  }
  fs.writeFileSync(path.join(outDir, 'census.json'), JSON.stringify(census, null, 2))
  fs.writeFileSync(path.join(outDir, 'census.md'), renderMarkdown(census))

  return {
    file: path.join(outDir, 'census.md'),
    json: path.join(outDir, 'census.json'),
    totalProducts: overall.products,
    vendors: Object.keys(vendors).length,
    resolvable: {
      gender: attrResolved(overall.gender, overall.products),
      ageGroup: attrResolved(overall.ageGroup, overall.products),
      color: attrResolved(overall.color, overall.products),
      size: attrResolved(overall.size, overall.products),
    },
    needsAi: {
      gender: overall.gender.unresolved,
      ageGroup: overall.ageGroup.unresolved,
      color: overall.color.unresolved,
      size: overall.size.unresolved,
    },
  }
}

function renderMarkdown(census) {
  const o = census.overall
  const t = o.products
  const lines = []
  lines.push('# Catalog Census — GMC Attribute Coverage')
  lines.push('')
  lines.push(`Generated: ${census.generatedAt} from \`${census.input}\``)
  lines.push('')
  lines.push('## Overall')
  lines.push('')
  lines.push(`- **Products:** ${t} across ${Object.keys(census.vendors).length} vendors`)
  lines.push(`- **Single-variant products:** ${o.singleVariant} (${pct(o.singleVariant, t)})`)
  lines.push(`- **Apparel & Accessories category:** ${o.apparelCategory} (${pct(o.apparelCategory, t)}); no category: ${o.noCategory}`)
  lines.push(`- **Brand metafield present:** ${o.hasBrandMeta} (${pct(o.hasBrandMeta, t)}); brand differs from vendor: ${o.brandDiffersFromVendor}`)
  lines.push('')
  lines.push('### Rules-layer coverage (resolvable without AI)')
  lines.push('')
  lines.push('| Attribute | Metafield | Option | Tag | Title | Unresolved (needs AI) | Resolvable |')
  lines.push('|---|---|---|---|---|---|---|')
  for (const [label, a] of [['Gender', o.gender], ['Age group', o.ageGroup], ['Color', o.color], ['Size', o.size]]) {
    lines.push(`| ${label} | ${a.byMetafield} | ${a.byOption} | ${a.byTag} | ${a.byTitle} | ${a.unresolved} | ${attrResolved(a, t)} |`)
  }
  lines.push('')
  lines.push('## Per-vendor coverage')
  lines.push('')
  lines.push('| Vendor | Products | Size opt | Color opt | Gender ok | Age ok | Color ok | Size ok |')
  lines.push('|---|---|---|---|---|---|---|---|')
  const sorted = Object.entries(census.vendors).sort((a, b) => b[1].products - a[1].products)
  for (const [vendor, s] of sorted) {
    lines.push(`| ${vendor} | ${s.products} | ${pct(s.hasSizeOption, s.products)} | ${pct(s.hasColorOption, s.products)} | ${attrResolved(s.gender, s.products)} | ${attrResolved(s.ageGroup, s.products)} | ${attrResolved(s.color, s.products)} | ${attrResolved(s.size, s.products)} |`)
  }
  lines.push('')
  lines.push('## Normalization inputs')
  lines.push('')
  lines.push('### Distinct option names')
  lines.push('')
  for (const [name, count] of Object.entries(census.distinctOptionNames).sort((a, b) => b[1] - a[1])) {
    lines.push(`- \`${name}\`: ${count}`)
  }
  lines.push('')
  lines.push('### Gender metafield values (custom.gender)')
  lines.push('')
  for (const [v, count] of Object.entries(census.genderMetafieldValues).sort((a, b) => b[1] - a[1])) {
    const unmapped = census.unmappedGenderMetafieldValues[v] ? ' **(unmapped — extend GENDER_META_MAP)**' : ''
    lines.push(`- \`${v}\`: ${count}${unmapped}`)
  }
  lines.push('')
  lines.push('### Age group metafield values (attribute.google_age_group)')
  lines.push('')
  for (const [v, count] of Object.entries(census.ageGroupMetafieldValues).sort((a, b) => b[1] - a[1])) {
    const unmapped = census.unmappedAgeGroupMetafieldValues[v] ? ' **(unmapped — extend AGE_META_MAP)**' : ''
    lines.push(`- \`${v}\`: ${count}${unmapped}`)
  }
  lines.push('')
  return lines.join('\n')
}

// --- Main --------------------------------------------------------------

const USAGE = {
  export: 'export [--status active|draft|archived|any] [--out products.jsonl] [--timeout-min 30] [--dry-run]',
  status: 'status --id <bulk-operation-gid>',
  census: 'census [--in products.jsonl] [--out-dir census]',
  run: 'run [export + census options]',
  env: 'SHOPIFY_STORE_DOMAIN, SHOPIFY_ACCESS_TOKEN (read_products), SHOPIFY_API_VERSION (optional)',
}

async function main() {
  let result
  switch (cmd) {
    case 'export':
      result = await runExport()
      break
    case 'status':
      result = await runStatus()
      break
    case 'census':
      result = await runCensus()
      break
    case 'run': {
      const exported = await runExport()
      if (exported._dry_run) { result = exported; break }
      args.in = exported.file
      result = await runCensus()
      break
    }
    default:
      result = { error: 'Unknown command', usage: USAGE }
  }
  console.log(JSON.stringify(result, null, 2))
}

main().catch((err) => {
  console.error(JSON.stringify({ error: err.message }))
  process.exit(1)
})
