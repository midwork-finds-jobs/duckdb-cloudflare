# Cloudflare Extension for DuckDB

Query Cloudflare D1 databases directly from DuckDB with native ATTACH DATABASE syntax, transaction batching, and automatic query optimization.

[![DuckDB Version](https://img.shields.io/badge/DuckDB-v1.4.2-blue)](https://duckdb.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Community Extension](https://img.shields.io/badge/Community-Extension-orange)](https://duckdb.org/community_extensions/)

## Installation

```sql
INSTALL cloudflare FROM community;
LOAD cloudflare;
```

## Features

- ✅ **Natural ATTACH syntax** - `ATTACH 'database' AS mydb (TYPE d1)`
- ✅ **No SECRET parameter needed** - Automatically finds D1 secret
- ✅ **Auto-create views** - All tables instantly queryable
- ✅ **Transaction batching** - Multiple operations in single HTTP request
- ✅ **Query optimization** - Automatic filter/LIMIT/projection pushdown
- ✅ **Secret management** - Store credentials once with CREATE SECRET

## Quick Start

### 1. Get Credentials

From [Cloudflare Dashboard](https://dash.cloudflare.com):

- **Account ID**: `D1 → Database → Right sidebar`
- **API Token**: `Profile → API Tokens → Create Token` (with D1 permissions)

See [detailed guide →](docs/CLOUDFLARE-CREDENTIALS.md)

### 2. Create Secret

```sql
CREATE SECRET d1 (
    TYPE d1,
    ACCOUNT_ID 'your-cloudflare-account-id',
    API_TOKEN 'your-cloudflare-api-token'
);
```

### 3. Query Databases

```sql
-- List databases
SELECT * FROM d1_databases('d1');

-- Attach database (no SECRET needed!)
ATTACH 'my-database' AS mydb (TYPE d1);

-- Query with automatic optimization
SELECT * FROM mydb.users WHERE active = true LIMIT 10;
```

### 4. Transaction Batching

Multiple operations → single HTTP request:

```sql
BEGIN TRANSACTION;
  INSERT INTO mydb.logs VALUES (1, 'Event A');
  INSERT INTO mydb.logs VALUES (2, 'Event B');
  UPDATE mydb.settings SET value = 'new' WHERE key = 'config';
COMMIT;  -- All 3 statements sent as one batch
```

## D1 Functions

| Function | Purpose | Example |
|----------|---------|---------|
| `d1_databases(secret)` | List all databases | `SELECT * FROM d1_databases('d1')` |
| `d1_tables(secret, db)` | List tables | `SELECT * FROM d1_tables('d1', 'my-db')` |
| `d1_query(secret, db, sql)` | Execute query | `SELECT * FROM d1_query('d1', 'my-db', 'SELECT * FROM users')` |
| `d1_execute(secret, db, sql)` | Execute statement | `SELECT d1_execute('d1', 'my-db', 'INSERT INTO ...')` |

## Advanced Usage

### Multiple Cloudflare Accounts

```sql
-- Production
CREATE SECRET prod (TYPE d1, ACCOUNT_ID 'prod-id', API_TOKEN 'prod-token');
ATTACH 'prod-db' AS prod (TYPE d1, SECRET 'prod');

-- Staging
CREATE SECRET staging (TYPE d1, ACCOUNT_ID 'staging-id', API_TOKEN 'staging-token');
ATTACH 'staging-db' AS staging (TYPE d1, SECRET 'staging');

-- Compare environments
SELECT 'prod' as env, COUNT(*) FROM prod.users
UNION ALL
SELECT 'staging', COUNT(*) FROM staging.users;
```

### Export to Parquet

```sql
COPY (SELECT * FROM mydb.orders WHERE status = 'completed')
TO 'orders.parquet' (FORMAT PARQUET);
```

### Join D1 with Local Data

```sql
SELECT u.name, COUNT(*) as order_count
FROM mydb.users u
JOIN mydb.orders o ON u.id = o.user_id
GROUP BY u.name;
```

## How It Works

### Architecture

```text
DuckDB → D1Catalog (custom) → D1TransactionManager (batch buffering)
            ↓
      Auto-created views
            ↓
      d1_scan (table function)
            ↓
      Cloudflare D1 API
```

**Key optimizations:**

- **Filter pushdown** - WHERE clauses sent to D1
- **LIMIT pushdown** - LIMIT sent to D1 API
- **Projection pushdown** - Only requested columns fetched
- **Batch buffering** - Multiple writes = one HTTP request

### Transaction Batching

```text
BEGIN TRANSACTION
  INSERT statement 1  →  Buffered
  INSERT statement 2  →  Buffered
  UPDATE statement    →  Buffered
COMMIT                →  Single batch HTTP request to D1
```

**Important**: D1 uses auto-commit per statement (not true ACID transactions). Best for bulk operations where per-statement atomicity is acceptable.

## Documentation

- 📚 **[Get Cloudflare Credentials](docs/CLOUDFLARE-CREDENTIALS.md)** - Account ID & API Token setup
- 📖 **[Complete Usage Guide](D1-USAGE-EXAMPLE.md)** - All features & examples
- 🏗️ **[Implementation Details](D1-ATTACH-SOLUTION.md)** - Architecture & transaction batching
- 🧪 **[Test Scripts](test-d1-syntax.sql)** - Example queries and verification

## Performance Tips

✅ **Use filter pushdown:**

```sql
-- Good - filter sent to D1
SELECT * FROM mydb.users WHERE id = 123;

-- Bad - all data fetched then filtered
SELECT * FROM (SELECT * FROM mydb.users) WHERE id = 123;
```

✅ **Batch writes:**

```sql
-- Good - one HTTP request
BEGIN TRANSACTION;
  INSERT INTO mydb.logs SELECT * FROM read_csv('data.csv');
COMMIT;
```

✅ **Export for heavy processing:**

```sql
-- Export to local Parquet, process locally
COPY (SELECT * FROM mydb.large_table) TO 'local.parquet';
SELECT * FROM 'local.parquet' WHERE complex_calculation(...);
```

## Limitations

| Limitation | Impact | Workaround |
|------------|--------|------------|
| No true ACID transactions | Statements auto-commit | Use `d1_execute()` for single statements |
| No rollback after commit | Can't undo committed data | Plan operations carefully |
| 30 second batch timeout | Large batches may fail | Split into smaller batches |
| Read-your-writes in txn | Buffered writes invisible until commit | Commit before reading |
| No DDL via ATTACH | Can't CREATE TABLE | Use `d1_execute()` for DDL |

## Building from Source

```bash
# Clone repository
git clone https://github.com/onnimonni/duckdb-cloudflare.git
cd duckdb-cloudflare

# Initialize submodules
git submodule update --init --recursive

# Build
make release GEN=ninja

# Test
./build/release/duckdb -f test-d1-syntax.sql
```

## Troubleshooting

### "D1 attach requires a D1 secret"

**Solution:** Create a secret first:

```sql
CREATE SECRET d1 (TYPE d1, ACCOUNT_ID '...', API_TOKEN '...');
```

### "HTTP request failed with status 401"

**Cause:** Invalid API token

**Solution:**

1. Verify credentials in Cloudflare dashboard
2. Create new token with D1 permissions
3. Update secret

### "D1 database not found"

**Solution:** List databases to find correct name:

```sql
SELECT name FROM d1_databases('d1');
```

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Links

- **DuckDB Community Extensions**: <https://duckdb.org/community_extensions/>
- **Cloudflare D1 Docs**: <https://developers.cloudflare.com/d1/>
- **API Token Guide**: <https://developers.cloudflare.com/fundamentals/api/get-started/create-token/>
- **GitHub Repository**: <https://github.com/onnimonni/duckdb-cloudflare>
- **Report Issues**: <https://github.com/onnimonni/duckdb-cloudflare/issues>

## License

MIT License - see [LICENSE](LICENSE) file

---

Made with ❤️ for the DuckDB community

*Query Cloudflare D1 from your local database!*
