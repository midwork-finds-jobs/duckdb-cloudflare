SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Query to test timestamp conversion
SELECT url, timestamp, status_code
FROM common_crawl_index('CC-MAIN-2025-43', 5)
WHERE url LIKE '%.teamtailor.com/%'
  AND status_code = 200
LIMIT 5;
