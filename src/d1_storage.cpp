#include "storage/d1_storage.hpp"
#include "storage/d1_transaction.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/default/default_schemas.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

// ========================================
// D1 CATALOG IMPLEMENTATION
// ========================================

optional_ptr<CatalogEntry> D1Catalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	// For D1, we only support the default schema
	if (info.schema != DEFAULT_SCHEMA && info.schema != "main") {
		throw CatalogException("D1 catalog only supports 'main' schema");
	}
	return nullptr;
}

void D1Catalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw CatalogException("Cannot drop schema from D1 catalog");
}

PhysicalOperator &D1Catalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("CREATE TABLE AS not supported in D1 catalog");
}

PhysicalOperator &D1Catalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                        optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("INSERT not supported in D1 catalog, use d1_execute() function");
}

PhysicalOperator &D1Catalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                        PhysicalOperator &plan) {
	throw NotImplementedException("DELETE not supported in D1 catalog, use d1_execute() function");
}

PhysicalOperator &D1Catalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                        PhysicalOperator &plan) {
	throw NotImplementedException("UPDATE not supported in D1 catalog, use d1_execute() function");
}

DatabaseSize D1Catalog::GetDatabaseSize(ClientContext &context) {
	// Return empty size since D1 is remote
	DatabaseSize size;
	size.total_blocks = 0;
	size.block_size = 0;
	size.free_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	return size;
}

void D1Catalog::CreateViewsForAllTables(ClientContext &context) {
	// Get D1 config from secret
	D1Config config = GetD1ConfigFromSecret(context, secret_name);

	// Determine if database_name is UUID or name
	bool is_uuid = (database_name.size() == 36 && database_name[8] == '-' && database_name[13] == '-');

	string database_id;
	if (is_uuid) {
		database_id = database_name;
	} else {
		database_id = D1GetDatabaseIdByName(config, database_name);
	}

	config.database_id = database_id;

	// Get list of tables
	auto tables = D1GetTables(config);

	// Create views for all tables
	auto conn = Connection(db.GetDatabase());
	for (auto &table : tables) {
		conn.TableFunction("d1_scan", {Value(table.name), Value(secret_name), Value(database_id)})
		    ->CreateView(table.name, true, false);
	}
}

// ========================================
// D1 STORAGE EXTENSION
// ========================================

static unique_ptr<Catalog> D1AttachFunction(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options) {
	// info.path contains the database name (from ATTACH 'database_name')
	// Look for secret in options
	string secret_name;

	auto secret_it = info.options.find("secret");
	if (secret_it != info.options.end()) {
		secret_name = secret_it->second.ToString();
	}

	// If no secret specified, try to use the default D1 secret or find any D1 secret
	if (secret_name.empty()) {
		// Try common default names
		vector<string> default_names = {"d1", "cloudflare", "__default_d1"};

		auto &secret_manager = SecretManager::Get(context);
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

		for (const auto &default_name : default_names) {
			auto secret = secret_manager.GetSecretByName(transaction, default_name);
			if (secret) {
				secret_name = default_name;
				break;
			}
		}

		if (secret_name.empty()) {
			throw BinderException(
			    "D1 attach requires a D1 secret. Create one with: CREATE SECRET (TYPE d1, ACCOUNT_ID '...', "
			    "API_TOKEN '...')\n"
			    "Or specify an existing secret: ATTACH 'db_name' AS alias (TYPE d1, SECRET 'secret_name')");
		}
	}

	auto catalog = make_uniq<D1Catalog>(db, info.path.empty() ? name : info.path, secret_name);

	// Create views for all tables
	catalog->CreateViewsForAllTables(context);

	return std::move(catalog);
}

static unique_ptr<TransactionManager> D1CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                 AttachedDatabase &db, Catalog &catalog) {
	// Use custom D1TransactionManager for batch buffering
	auto &d1_catalog = catalog.Cast<D1Catalog>();
	return make_uniq<D1TransactionManager>(db, d1_catalog);
}

// ========================================
// REGISTER STORAGE EXTENSION
// ========================================

void RegisterD1StorageExtension(DatabaseInstance &db) {
	auto &config = DBConfig::GetConfig(db);

	auto d1_storage = make_uniq<StorageExtension>();
	d1_storage->attach = D1AttachFunction;
	d1_storage->create_transaction_manager = D1CreateTransactionManager;

	config.storage_extensions["d1"] = std::move(d1_storage);
}

} // namespace duckdb
