# D1 ATTACH DATABASE Solution

## Key Discovery: D1 Batch API

D1 supports **batch operations** via the batch API endpoint, which can send multiple SQL statements in a single HTTP request.

### D1 Batch Capabilities

**What it provides:**

- Multiple SQL statements in one request
- Reduced network round trips
- Sequential execution of statements
- Individual statement results

**Critical limitations:**

- **NOT true ACID transactions** - operates in auto-commit mode
- Each statement commits sequentially
- Failure of one statement doesn't rollback others
- 30 second timeout for entire batch
- Per-statement limits still apply (100 KB max SQL length)

Source: [Cloudflare D1 Database docs](https://developers.cloudflare.com/d1/worker-api/d1-database/)

## Solution: Custom Transaction Manager with Batch Buffering

Implement ATTACH DATABASE syntax using sqlite_scanner pattern but adapt for D1's batch API.

### Architecture

```text
DuckDB Transaction Lifecycle        D1 Implementation
─────────────────────────         ─────────────────────
BEGIN TRANSACTION          →      Create D1Transaction (buffer statements)
  INSERT INTO table1       →      Buffer: INSERT INTO table1...
  UPDATE table2            →      Buffer: UPDATE table2...
  SELECT * FROM table3     →      Execute immediately (read)
COMMIT                     →      Send buffered writes as batch request
```

### Implementation Plan

#### 1. D1Transaction Class

```cpp
class D1Transaction : public Transaction {
public:
    D1Transaction(D1Catalog &catalog, TransactionManager &manager, ClientContext &context);

    void Start();
    void Commit();
    void Rollback();

    // Buffer write operations
    void BufferStatement(const string &sql);

    // Execute read operations immediately
    D1QueryResult ExecuteRead(const string &sql);

private:
    D1Catalog &d1_catalog;
    D1Config config;
    vector<string> buffered_statements;  // Batched on commit
    bool has_writes = false;
};
```

**Key methods:**

- `Start()`: Initialize empty buffer
- `BufferStatement()`: Add INSERT/UPDATE/DELETE to buffer
- `ExecuteRead()`: Execute SELECT immediately (no buffering)
- `Commit()`: Send all buffered statements as single batch request
- `Rollback()`: Clear buffer (can't undo committed statements on D1!)

#### 2. D1TransactionManager Class

```cpp
class D1TransactionManager : public TransactionManager {
public:
    explicit D1TransactionManager(AttachedDatabase &db, D1Catalog &catalog);

    Transaction &StartTransaction(ClientContext &context) override;
    ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
    void RollbackTransaction(Transaction &transaction) override;

private:
    D1Catalog &d1_catalog;
    mutex transaction_lock;
    reference_map_t<Transaction, unique_ptr<D1Transaction>> transactions;
};
```

#### 3. D1Catalog Modifications

Keep existing D1Catalog but update transaction manager creation:

```cpp
static unique_ptr<TransactionManager> D1CreateTransactionManager(
    optional_ptr<StorageExtensionInfo> storage_info,
    AttachedDatabase &db,
    Catalog &catalog) {
    auto &d1_catalog = catalog.Cast<D1Catalog>();
    return make_uniq<D1TransactionManager>(db, d1_catalog);
}
```

#### 4. Physical Operator Implementations

Update `PlanInsert`, `PlanUpdate`, `PlanDelete` to generate SQL and buffer it:

```cpp
PhysicalOperator &D1Catalog::PlanInsert(ClientContext &context,
                                         PhysicalPlanGenerator &planner,
                                         LogicalInsert &op,
                                         optional_ptr<PhysicalOperator> plan) {
    // Create D1Insert physical operator
    // Generates: INSERT INTO table (...) VALUES (?, ?, ?)
    // Execution buffers SQL in current D1Transaction
    return planner.CreatePlan<D1Insert>(op, plan);
}
```

#### 5. HTTP Batch API Support

Add batch execution to `d1_http.cpp`:

```cpp
struct D1BatchResult {
    bool success;
    vector<D1QueryResult> results;  // One per statement
    string error;
};

D1BatchResult D1ExecuteBatch(const D1Config &config, const vector<string> &statements) {
    // Build JSON: {"statements": [{"sql": "..."}, {"sql": "..."}, ...]}
    string body = "{\"statements\":[";
    for (size_t i = 0; i < statements.size(); i++) {
        if (i > 0) body += ",";
        body += "{\"sql\":\"" + EscapeJSON(statements[i]) + "\"}";
    }
    body += "]}";

    string response = HTTPPost(config.GetBatchUrl(), body, config.api_token);
    return ParseD1BatchResponse(response);
}
```

Add to D1Config:

```cpp
string GetBatchUrl() const {
    return "https://api.cloudflare.com/client/v4/accounts/" + account_id +
           "/d1/database/" + database_id + "/batch";
}
```

### Transaction Semantics

**Important limitations to document:**

1. **No true rollback**: D1 auto-commits each statement. Rollback only discards buffered (not-yet-sent) statements.

2. **Read-your-writes within transaction**: Buffered writes aren't visible to reads in same transaction until commit.

3. **Failure handling**: If batch fails partway through, some statements may have committed.

4. **Best for**: Bulk inserts/updates where atomicity across statements isn't critical.

### Usage Example

```sql
-- Create secret for D1 access
CREATE SECRET cloudflare (TYPE d1, ACCOUNT_ID '...', API_TOKEN '...');

-- Attach D1 database
ATTACH 'my-database' AS d1db (TYPE d1, SECRET 'cloudflare');

-- Read operations (immediate execution)
SELECT * FROM d1db.users LIMIT 10;

-- Write operations (batched on commit)
BEGIN TRANSACTION;
  INSERT INTO d1db.users VALUES (1, 'Alice');
  INSERT INTO d1db.users VALUES (2, 'Bob');
  UPDATE d1db.settings SET value = 'new' WHERE key = 'config';
COMMIT;  -- All 3 statements sent as one batch

-- Rollback only works for buffered statements
BEGIN TRANSACTION;
  INSERT INTO d1db.users VALUES (3, 'Charlie');
ROLLBACK;  -- Buffer discarded, INSERT never sent to D1
```

### Implementation Files

**New files:**

- `src/d1_transaction.cpp` - D1Transaction implementation
- `src/d1_transaction_manager.cpp` - D1TransactionManager implementation
- `src/include/storage/d1_transaction.hpp` - Transaction headers

**Modified files:**

- `src/d1_storage.cpp` - Update D1CreateTransactionManager
- `src/d1_http.cpp` - Add D1ExecuteBatch function
- `src/include/d1_extension.hpp` - Add batch function declaration
- `src/d1_scan.cpp` - Use transaction context for reads

**Physical operators to implement:**

- `src/storage/d1_insert.cpp` - Generate INSERT SQL, buffer in transaction
- `src/storage/d1_update.cpp` - Generate UPDATE SQL, buffer in transaction
- `src/storage/d1_delete.cpp` - Generate DELETE SQL, buffer in transaction

### Comparison: sqlite_scanner vs D1

| Aspect | sqlite_scanner | D1 (our solution) |
|--------|---------------|-------------------|
| Backend | Local SQLite file | Remote HTTP API |
| Connection | Open sqlite3* handle | HTTP requests per batch |
| Transaction | BEGIN/COMMIT in SQLite | Buffer + batch request |
| Rollback | True rollback | Discard buffer only |
| ACID | Full ACID guarantees | Auto-commit per statement |
| Reads | Direct SQLite query | HTTP request per query |
| Writes | Immediate via SQLite | Buffered, sent on commit |

### Advantages of This Approach

1. **Natural DuckDB syntax**: `ATTACH 'db' AS alias (TYPE d1)`
2. **Batch optimization**: Multiple writes = one HTTP request
3. **Consistent with other storage extensions**: Same pattern as sqlite/postgres scanners
4. **Explicit transaction boundaries**: User controls when batch is sent
5. **Read optimization**: Reads execute immediately, no buffering overhead

### Limitations to Document

1. **No true atomicity**: Statements commit individually in sequence
2. **No rollback of committed statements**: Only buffered statements can be rolled back
3. **Read-your-writes delay**: Buffered writes not visible until commit
4. **Timeout risk**: Large batches may hit 30s timeout
5. **Network dependency**: All operations require HTTP requests

### Alternative: Keep Table Function for Read-Only

Could offer both approaches:

#### Option A: ATTACH for read-write

```sql
ATTACH 'db' AS d1db (TYPE d1, SECRET 'cloudflare');
INSERT INTO d1db.table VALUES (...);  -- Requires transaction implementation
```

#### Option B: Table function for simple read-only

```sql
SELECT * FROM d1_attach('db', 'cloudflare');  -- Simpler, view-based
```

## Recommendation

**Implement custom transaction manager with batch buffering** to enable ATTACH DATABASE syntax.

**Reasoning:**

1. D1 batch API makes this feasible
2. Provides natural DuckDB syntax
3. Performance optimization via batching
4. Consistent with other storage extensions
5. Users understand transaction limitations (document clearly)

**Phase 1 (MVP):**

- Implement D1Transaction and D1TransactionManager
- Batch buffering for INSERT/UPDATE/DELETE
- Immediate execution for SELECT
- Basic error handling

**Phase 2 (enhancements):**

- Smart read buffering (cache reads during transaction)
- Automatic batch splitting for large transactions
- Better error messages for D1 limitations
- Transaction statistics (statements buffered, batch size)

## Sources

- [Cloudflare D1 Database · Workers API](https://developers.cloudflare.com/d1/worker-api/d1-database/)
- [D1 SQL Statements](https://developers.cloudflare.com/d1/sql-api/sql-statements/)
- [D1 Platform Limits](https://developers.cloudflare.com/d1/platform/limits/)
