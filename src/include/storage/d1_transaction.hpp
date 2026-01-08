#pragma once

#include "duckdb.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "d1_extension.hpp"

namespace duckdb {

// Forward declarations
class D1Catalog;
class D1TransactionManager;

// ========================================
// D1 TRANSACTION
// Buffers write operations for batch execution on commit
// ========================================

class D1Transaction : public Transaction {
public:
	D1Transaction(D1TransactionManager &manager, ClientContext &context, D1Catalog &catalog);
	~D1Transaction() override;

	void Start();
	void Commit();
	void Rollback();

	// Buffer a write statement (INSERT/UPDATE/DELETE) for batch execution
	void BufferStatement(const string &sql);

	// Execute a read statement immediately
	D1QueryResult ExecuteRead(const string &sql);

	// Get the D1 configuration
	D1Config &GetConfig() {
		return config;
	}

	// Check if transaction has buffered writes
	bool HasBufferedWrites() const {
		return !buffered_statements.empty();
	}

	// Get buffered statement count
	idx_t GetBufferedCount() const {
		return buffered_statements.size();
	}

private:
	D1TransactionManager &d1_manager;
	D1Catalog &d1_catalog;
	D1Config config;
	vector<string> buffered_statements;
	bool is_started;
};

// ========================================
// D1 TRANSACTION MANAGER
// Manages D1 transactions with batch buffering
// ========================================

class D1TransactionManager : public TransactionManager {
public:
	D1TransactionManager(AttachedDatabase &db, D1Catalog &catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	D1Catalog &GetCatalog() {
		return d1_catalog;
	}

private:
	D1Catalog &d1_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<D1Transaction>> transactions;
};

} // namespace duckdb
