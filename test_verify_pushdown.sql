SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Enable HTTP logging to verify predicate pushdown
CALL enable_logging('HTTP');

-- This should fail due to UTF-8 issue, but will log the HTTP request
SELECT url, status_code
FROM common_crawl_index('CC-MAIN-2025-43', '*.teamtailor.com/*', 5)
WHERE status_code = 200
LIMIT 1;

-- Check the HTTP logs to verify filter was pushed down
.mode line
SELECT request.url as cdx_url
FROM duckdb_logs_parsed('HTTP')
WHERE request.url LIKE 'https://index.commoncrawl.org/%'
LIMIT 1;
