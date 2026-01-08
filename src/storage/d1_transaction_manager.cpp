#include "storage/d1_transaction.hpp"
#include "storage/d1_storage.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

D1TransactionManager::D1TransactionManager(AttachedDatabase &db, D1Catalog &catalog)
    : TransactionManager(db), d1_catalog(catalog) {
}

Transaction &D1TransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<D1Transaction>(*this, context, d1_catalog);
	transaction->Start();
	auto &result = *transaction;

	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData D1TransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &d1_transaction = transaction.Cast<D1Transaction>();

	try {
		d1_transaction.Commit();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}

	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void D1TransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &d1_transaction = transaction.Cast<D1Transaction>();
	d1_transaction.Rollback();

	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void D1TransactionManager::Checkpoint(ClientContext &context, bool force) {
	// D1 is remote, no checkpointing needed
}

} // namespace duckdb
