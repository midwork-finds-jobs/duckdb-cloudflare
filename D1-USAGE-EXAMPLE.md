# DuckDB D1 Extension - Complete Usage Guide

## Installation

```bash
# Build the extension
make release GEN=ninja

# Load in DuckDB
LOAD './build/release/extension/web_archive/web_archive.duckdb_extension';
```

## Quick Start

### 1. Create a Secret for Cloudflare Credentials

Store your Cloudflare credentials once using DuckDB's secret management:

```sql
CREATE SECRET d1 (
    TYPE d1,
    ACCOUNT_ID 'your-cloudflare-account-id',
    API_TOKEN 'your-cloudflare-api-token'
);
```

**Where to find these values:**

- **Account ID**: Cloudflare Dashboard → D1 → Your database → Copy Account ID
- **API Token**: Cloudflare Dashboard → My Profile → API Tokens → Create Token
  - Template: "Edit Cloudflare Workers"
  - Or custom token with `D1:Read` and `D1:Edit` permissions

### 2. List Available D1 Databases

```sql
SELECT * FROM d1_databases('d1');
```

Output:

```text
┌──────────────────────────────────┬──────────────┬─────────────┬────────────┬───────────┐
│               uuid               │     name     │ created_at  │ file_size  │ num_tables│
├──────────────────────────────────┼──────────────┼─────────────┼────────────┼───────────┤
│ 12345678-1234-1234-1234-12345... │ my-database  │ 2025-01-...│ 1048576    │ 5         │
└──────────────────────────────────┴──────────────┴─────────────┴────────────┴───────────┘
```

### 3. ATTACH a D1 Database

**New syntax (no SECRET needed!):**

```sql
ATTACH 'my-database' AS mydb (TYPE d1);
```

The extension automatically:

- Finds the D1 secret (looks for 'd1', 'cloudflare', or '__default_d1')
- Fetches the list of tables
- Creates views for all tables automatically

**Alternative - Explicit secret reference:**

```sql
ATTACH 'my-database' AS mydb (TYPE d1, SECRET 'custom_secret_name');
```

**Using database UUID instead of name:**

```sql
ATTACH '12345678-1234-1234-1234-123456789012' AS mydb (TYPE d1);
```

### 4. Query Tables

Once attached, all tables are available as views:

```sql
-- Simple query
SELECT * FROM mydb.users LIMIT 10;

-- With filters (pushed down to D1)
SELECT * FROM mydb.orders
WHERE status = 'completed'
  AND created_at > '2025-01-01'
LIMIT 100;

-- JOIN with local DuckDB tables
SELECT u.name, COUNT(*) as order_count
FROM mydb.users u
JOIN mydb.orders o ON u.id = o.user_id
GROUP BY u.name;
```

**Optimizations automatically applied:**

- ✅ **Projection pushdown**: Only requested columns fetched
- ✅ **Filter pushdown**: WHERE clauses sent to D1
- ✅ **LIMIT pushdown**: LIMIT sent to D1 API
- ✅ **Column filtering**: Only needed columns retrieved

### 5. List Tables in Attached Database

```sql
SELECT table_name
FROM duckdb_tables()
WHERE database_name = 'mydb';
```

Or use the D1-specific function:

```sql
SELECT * FROM d1_tables('d1', 'database-id-or-name');
```

## Advanced Features

### Transaction Batching

Multiple write operations are batched into a single HTTP request:

```sql
BEGIN TRANSACTION;
  INSERT INTO mydb.users VALUES (1, 'Alice', 'alice@example.com');
  INSERT INTO mydb.users VALUES (2, 'Bob', 'bob@example.com');
  UPDATE mydb.settings SET value = 'enabled' WHERE key = 'feature_flag';
COMMIT;
```

**What happens:**

- All 3 SQL statements buffered during transaction
- On COMMIT: Single batch HTTP request to D1 with all statements
- Significant performance improvement vs 3 separate requests

**Important notes:**

- ⚠️ **NOT true ACID transactions** - D1 auto-commits each statement
- ⚠️ If statement 2 fails, statement 1 is already committed
- ⚠️ ROLLBACK only clears buffered (not-yet-sent) statements
- ✅ Great for bulk inserts/updates where atomicity isn't critical

### Direct SQL Execution

For write operations, you can also use `d1_execute()`:

```sql
SELECT d1_execute(
    'd1',                    -- Secret name
    'my-database',           -- Database name or UUID
    'INSERT INTO users VALUES (3, ''Charlie'', ''charlie@example.com'')'
);
```

### Query Execution with Results

```sql
SELECT * FROM d1_query(
    'd1',                    -- Secret name
    'my-database',           -- Database name or UUID
    'SELECT * FROM users WHERE active = 1'
);
```

## Complete Example Workflow

```sql
-- 1. Load extension
LOAD './build/release/extension/web_archive/web_archive.duckdb_extension';

-- 2. Create secret
CREATE SECRET d1 (
    TYPE d1,
    ACCOUNT_ID 'abc123',
    API_TOKEN 'your-token-here'
);

-- 3. List databases
SELECT name, num_tables, file_size
FROM d1_databases('d1');

-- 4. Attach database
ATTACH 'my-app-db' AS app (TYPE d1);

-- 5. Explore tables
SELECT table_name FROM duckdb_tables() WHERE database_name = 'app';

-- 6. Query data
SELECT * FROM app.users WHERE created_at > '2025-01-01' LIMIT 10;

-- 7. Bulk insert with transaction batching
BEGIN TRANSACTION;
  INSERT INTO app.logs SELECT * FROM read_csv('logs.csv');
COMMIT;

-- 8. Export results
COPY (SELECT * FROM app.orders WHERE status = 'completed')
TO 'completed_orders.parquet' (FORMAT PARQUET);

-- 9. Detach when done
DETACH app;
```

## Multiple Secrets

You can create multiple secrets for different Cloudflare accounts:

```sql
-- Production account
CREATE SECRET d1_prod (
    TYPE d1,
    ACCOUNT_ID 'prod-account-id',
    API_TOKEN 'prod-token'
);

-- Staging account
CREATE SECRET d1_staging (
    TYPE d1,
    ACCOUNT_ID 'staging-account-id',
    API_TOKEN 'staging-token'
);

-- Attach databases from different accounts
ATTACH 'prod-db' AS prod (TYPE d1, SECRET 'd1_prod');
ATTACH 'staging-db' AS staging (TYPE d1, SECRET 'd1_staging');

-- Compare data across environments
SELECT
    'prod' as env, COUNT(*) as user_count
FROM prod.users
UNION ALL
SELECT
    'staging' as env, COUNT(*)
FROM staging.users;
```

## D1 Functions Reference

| Function | Purpose | Example |
|----------|---------|---------|
| `d1_databases(secret)` | List all databases | `SELECT * FROM d1_databases('d1')` |
| `d1_tables(secret, db)` | List tables in database | `SELECT * FROM d1_tables('d1', 'my-db')` |
| `d1_query(secret, db, sql)` | Execute query, return results | `SELECT * FROM d1_query('d1', 'my-db', 'SELECT * FROM users')` |
| `d1_execute(secret, db, sql)` | Execute statement, return metadata | `SELECT d1_execute('d1', 'my-db', 'INSERT INTO ...')` |
| `d1_scan(table, secret, db_id)` | Scan table with pushdowns | Used internally by views |

## Best Practices

### 1. Use ATTACH for Interactive Analysis

```sql
ATTACH 'analytics-db' AS analytics (TYPE d1);

SELECT
    DATE_TRUNC('day', timestamp) as day,
    COUNT(*) as events
FROM analytics.events
WHERE timestamp > NOW() - INTERVAL 30 days
GROUP BY day
ORDER BY day;
```

### 2. Batch Writes for Performance

```sql
-- ✅ GOOD: One batch request
BEGIN TRANSACTION;
  INSERT INTO mydb.logs SELECT * FROM read_csv('large_file.csv');
COMMIT;

-- ❌ BAD: Many individual requests
-- INSERT INTO mydb.logs VALUES (...); -- (repeated 1000s of times)
```

### 3. Use Pushdown Filters

```sql
-- ✅ GOOD: Filter pushed to D1
SELECT * FROM mydb.orders
WHERE customer_id = 123  -- Sent to D1
LIMIT 10;                -- Sent to D1

-- ❌ BAD: Fetch everything then filter locally
SELECT * FROM (SELECT * FROM mydb.orders)
WHERE customer_id = 123;
```

### 4. Export Results for Heavy Processing

```sql
-- Export D1 data to Parquet for local processing
COPY (SELECT * FROM mydb.large_table)
TO 'local_data.parquet' (FORMAT PARQUET);

-- Process locally (faster than remote queries)
SELECT * FROM 'local_data.parquet'
WHERE complex_calculation(...);
```

## Limitations

| Limitation | Impact | Workaround |
|------------|--------|------------|
| No true ACID transactions | Statements auto-commit | Use d1_execute for single statements |
| No rollback of committed statements | Can't undo after commit | Plan operations carefully |
| 30 second batch timeout | Large batches may fail | Split into smaller batches |
| Read-your-writes in transaction | Buffered writes invisible until commit | Commit before reading |
| No DDL via ATTACH | Can't CREATE TABLE | Use d1_execute() for DDL |

## Troubleshooting

### Error: "D1 attach requires a D1 secret"

**Solution:** Create a secret first:

```sql
CREATE SECRET d1 (TYPE d1, ACCOUNT_ID '...', API_TOKEN '...');
```

### Error: "HTTP request failed with status 401"

**Cause:** Invalid API token or account ID

**Solution:** Verify credentials in Cloudflare dashboard, recreate secret

### Error: "D1 database not found: my-db"

**Cause:** Database name doesn't exist or is wrong

**Solution:** Check available databases:

```sql
SELECT name FROM d1_databases('d1');
```

### Performance is slow

**Possible causes:**

- Not using filter/LIMIT pushdown
- Too many small queries (use batching)
- Network latency to Cloudflare

**Solutions:**

- Add WHERE clauses and LIMIT
- Use transactions for multiple operations
- Export data locally for heavy analysis

## Testing

Run the test suite:

```bash
# Syntax verification (no credentials needed)
./build/release/duckdb -f test-d1-syntax.sql

# Full test (requires real credentials)
# Edit test-d1-attach.sql with your credentials first
./build/release/duckdb -f test-d1-attach.sql
```

## Implementation Details

- **Architecture**: Custom StorageExtension with D1Catalog
- **Transaction Manager**: Custom D1TransactionManager with batch buffering
- **Views**: Auto-created using d1_scan table function
- **Pushdowns**: Projection, filter, LIMIT via optimizer hooks
- **Batch API**: Uses Cloudflare D1 batch endpoint for transactions

See `D1-ATTACH-SOLUTION.md` for full implementation details.
