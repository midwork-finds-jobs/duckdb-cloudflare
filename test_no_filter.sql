SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Test WITHOUT status_code filter to see all status codes
SELECT url, mime_type, status_code
FROM common_crawl_index('CC-MAIN-2025-43', '*.teamtailor.com/*', 20)
LIMIT 10;
