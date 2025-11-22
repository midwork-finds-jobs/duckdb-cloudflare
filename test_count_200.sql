SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Test predicate pushdown with status_code = 200
SELECT url, mime_type, status_code
FROM common_crawl_index('CC-MAIN-2025-43', '*.teamtailor.com/*', 100)
WHERE status_code = 200
LIMIT 10;
