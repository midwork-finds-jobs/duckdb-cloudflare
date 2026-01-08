#include "storage/d1_transaction.hpp"
#include "storage/d1_storage.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

D1Transaction::D1Transaction(D1TransactionManager &manager, ClientContext &context, D1Catalog &catalog)
    : Transaction(manager, context), d1_manager(manager), d1_catalog(catalog), is_started(false) {
	// Get D1 config from catalog
	config = catalog.GetConfig();
}

D1Transaction::~D1Transaction() {
	// Cleanup if needed
}

void D1Transaction::Start() {
	if (is_started) {
		return;
	}
	is_started = true;
	// D1 doesn't have explicit BEGIN - we just start buffering statements
}

void D1Transaction::Commit() {
	if (!is_started) {
		return;
	}

	// If we have buffered statements, send them as a batch
	if (!buffered_statements.empty()) {
		auto result = D1ExecuteBatch(config, buffered_statements);
		if (!result.success) {
			throw IOException("D1 batch commit failed: " + result.error);
		}

		// Check individual statement results
		for (size_t i = 0; i < result.results.size(); i++) {
			if (!result.results[i].success) {
				throw IOException("D1 statement " + to_string(i) + " failed: " + result.results[i].error);
			}
		}

		buffered_statements.clear();
	}

	is_started = false;
}

void D1Transaction::Rollback() {
	if (!is_started) {
		return;
	}

	// Clear buffered statements (can't rollback already-committed statements on D1)
	buffered_statements.clear();
	is_started = false;
}

void D1Transaction::BufferStatement(const string &sql) {
	if (!is_started) {
		Start();
	}
	buffered_statements.push_back(sql);
}

D1QueryResult D1Transaction::ExecuteRead(const string &sql) {
	// Reads are executed immediately, not buffered
	return D1ExecuteQuery(config, sql);
}

} // namespace duckdb
