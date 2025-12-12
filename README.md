# duckdb-web-archive-cdx

DuckDB extension to query web archive CDX APIs directly from SQL.

## Features

- **Two data sources**: Common Crawl (2008-present) and Wayback Machine (1996-present)
- **Smart pushdowns**: Filters, LIMIT, and SELECT are pushed to the CDX API for efficiency
- **Response fetching**: Optionally fetch archived page content
- **Collapse deduplication**: Remove duplicate entries using the `collapse` parameter

## Installation

```sql
INSTALL web_archive_cdx FROM community;
LOAD web_archive_cdx;
```

## Quick Start

### Common Crawl Index

```sql
-- Find HTML pages from a domain
SELECT url, timestamp, statuscode
FROM common_crawl_index()
WHERE url LIKE '%.example.com/%'
  AND statuscode = 200
  AND mimetype = 'text/html'
LIMIT 10;

-- Fetch page content
SELECT url, response.body
FROM common_crawl_index()
WHERE url LIKE 'https://example.com/%'
  AND statuscode = 200
LIMIT 1;
```

### Wayback Machine

```sql
-- Find archived snapshots
SELECT url, timestamp, statuscode
FROM wayback_machine()
WHERE url LIKE 'example.com/%'
  AND statuscode = 200
LIMIT 10;

-- Get latest snapshot of a page
SELECT url, timestamp, response.body
FROM wayback_machine()
WHERE url = 'example.com/about'
  AND statuscode = 200
ORDER BY timestamp DESC
LIMIT 1;
```

## Pushdown Optimizations

The extension automatically pushes operations to the CDX API for better performance:

### Filter Pushdown

WHERE clauses are converted to CDX API filter parameters:

```sql
SELECT url FROM common_crawl_index()
WHERE statuscode = 200              -- Pushed as: filter==status:200
  AND mimetype = 'text/html'        -- Pushed as: filter==mime:text/html
  AND mimetype != 'application/pdf' -- Pushed as: filter=!mime:application/pdf
LIMIT 10;
```

Supported filter columns: `statuscode`, `mimetype`

### LIMIT Pushdown

LIMIT is pushed directly to the CDX API:

```sql
-- Only fetches 5 records from the API (not 10000 then filtering)
SELECT url FROM wayback_machine()
WHERE url LIKE 'example.com/%'
LIMIT 5;
```

### SELECT Pushdown (Projection)

Only requested columns are fetched from the API:

```sql
-- Fast: only fetches url field
SELECT url FROM common_crawl_index()
WHERE url LIKE '%.example.com/%' LIMIT 10;

-- Slower: fetches filename, offset, length and downloads WARC content
SELECT url, response FROM common_crawl_index()
WHERE url LIKE '%.example.com/%' LIMIT 10;
```

## Collapse (Deduplication)

Use `collapse` to deduplicate results by URL prefix:

```sql
-- One result per unique URL (ignoring query strings)
SELECT url, timestamp
FROM wayback_machine(collapse := 'urlkey')
WHERE url LIKE 'example.com/%'
LIMIT 100;

-- One result per URL prefix (first 50 chars of urlkey)
SELECT url, timestamp
FROM wayback_machine(collapse := 'urlkey:50')
WHERE url LIKE 'example.com/%'
LIMIT 100;
```

## URL Matching Patterns

### Common Crawl

```sql
WHERE url LIKE '%.example.com/%'     -- Domain wildcard (subdomains)
WHERE url LIKE 'https://example.com/%' -- Prefix match
WHERE url SIMILAR TO '.*example\.com/$' -- Regex match for root pages only
```

### Wayback Machine

```sql
WHERE url = 'example.com'           -- Exact match
WHERE url LIKE 'example.com/%'      -- Prefix match
WHERE url LIKE '%.example.com'      -- Domain match (subdomains)
```

## Available Columns

### common_crawl_index()

| Column | Type | Description |
|--------|------|-------------|
| url | VARCHAR | Original URL |
| timestamp | TIMESTAMP | Crawl timestamp |
| statuscode | INTEGER | HTTP status code |
| mimetype | VARCHAR | Content MIME type |
| digest | VARCHAR | Content hash |
| filename | VARCHAR | WARC filename |
| offset | BIGINT | Offset in WARC |
| length | BIGINT | Content length |
| response | STRUCT | Parsed WARC response (headers + body) |

### wayback_machine()

| Column | Type | Description |
|--------|------|-------------|
| url | VARCHAR | Original URL |
| urlkey | VARCHAR | SURT-formatted URL |
| timestamp | TIMESTAMP | Archive timestamp |
| statuscode | INTEGER | HTTP status code |
| mimetype | VARCHAR | Content MIME type |
| digest | VARCHAR | Content hash |
| length | BIGINT | Content length |
| response | STRUCT | Response struct with body field |

## Parameters

### common_crawl_index()

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| max_results | BIGINT | 100 | Maximum results from CDX API |

### wayback_machine()

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| max_results | BIGINT | 100 | Maximum results from CDX API |
| collapse | VARCHAR | - | Collapse/dedupe field (e.g., 'urlkey') |

## Specifying Crawl ID (Common Crawl)

```sql
-- Use specific crawl
SELECT url FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-47'
  AND url LIKE '%.example.com/%'
LIMIT 10;

-- Use timestamp range (queries multiple crawls)
SELECT url FROM common_crawl_index()
WHERE timestamp >= '2024-01-01'
  AND timestamp < '2024-06-01'
  AND url LIKE '%.example.com/%'
LIMIT 10;

-- Default: uses latest crawl
SELECT url FROM common_crawl_index()
WHERE url LIKE '%.example.com/%'
LIMIT 10;
```

## Examples

### Find all unique domains from a TLD

```sql
SELECT DISTINCT regexp_extract(url, 'https?://([^/]+)', 1) as domain
FROM wayback_machine(collapse := 'urlkey:30')
WHERE url LIKE '%.gov/%'
  AND statuscode = 200
  AND mimetype = 'text/html'
LIMIT 1000;
```

### Get homepage snapshots over time

```sql
SELECT timestamp, statuscode, length
FROM wayback_machine()
WHERE url = 'example.com'
  AND statuscode IN (200, 301, 302)
ORDER BY timestamp;
```

### Export URLs to file

```sql
COPY (
  SELECT url, timestamp
  FROM common_crawl_index()
  WHERE url LIKE '%.example.com/%'
    AND statuscode = 200
  LIMIT 10000
) TO 'urls.csv';
```

## Data Sources

- **Common Crawl**: https://commoncrawl.org - Monthly web crawls since 2008
- **Wayback Machine**: https://web.archive.org - Continuous archiving since 1996

## CDX API Documentation

This extension uses the CDX Server API to query web archive indexes:

- **CDX Server API Reference**: https://github.com/webrecorder/pywb/wiki/CDX-Server-API
- **Common Crawl Index API**: https://index.commoncrawl.org
- **Common Crawl Index Collections**: https://index.commoncrawl.org/collinfo.json
- **Wayback Machine CDX API**: https://web.archive.org/cdx/search/cdx

### Example CDX API URLs

```
# Common Crawl - query index directly
https://index.commoncrawl.org/CC-MAIN-2025-47-index?url=*.example.com/*&output=json&limit=10

# Wayback Machine - query CDX server
https://web.archive.org/cdx/search/cdx?url=example.com&output=json&limit=10
```

## License

MIT
