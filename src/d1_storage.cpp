#include "d1_extension.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
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
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

// ========================================
// D1 CATALOG
// Minimal catalog that creates views on attach
// ========================================

class D1Catalog : public Catalog {
public:
	explicit D1Catalog(AttachedDatabase &db_p, string database_name, string secret_name)
	    : Catalog(db_p), database_name(std::move(database_name)), secret_name(std::move(secret_name)) {
	}

	string GetCatalogType() override {
		return "d1";
	}

	void Initialize(bool load_builtin) override {
		// D1 catalog doesn't need initialization, views are created on attach
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override {
		// For D1, we only support the default schema
		if (info.schema != DEFAULT_SCHEMA && info.schema != "main") {
			throw CatalogException("D1 catalog only supports 'main' schema");
		}
		return nullptr;
	}

	void DropSchema(ClientContext &context, DropInfo &info) override {
		throw CatalogException("Cannot drop schema from D1 catalog");
	}

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override {
		// D1 only has the main schema
	}

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override {
		// D1 uses views in the default DuckDB catalog, not a separate schema catalog
		return nullptr;
	}

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override {
		throw NotImplementedException("CREATE TABLE AS not supported in D1 catalog");
	}

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override {
		throw NotImplementedException("INSERT not supported in D1 catalog, use d1_execute() function");
	}

	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override {
		throw NotImplementedException("DELETE not supported in D1 catalog, use d1_execute() function");
	}

	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override {
		throw NotImplementedException("UPDATE not supported in D1 catalog, use d1_execute() function");
	}

	DatabaseSize GetDatabaseSize(ClientContext &context) override {
		// Return empty size since D1 is remote
		DatabaseSize size;
		return size;
	}

	bool InMemory() override {
		return false;
	}

	string GetDBPath() override {
		return database_name;
	}

	void CreateViewsForAllTables(ClientContext &context) {
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

private:
	string database_name;
	string secret_name;
};

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

	if (secret_name.empty()) {
		throw BinderException("D1 attach requires 'secret' option: ATTACH 'db_name' (TYPE d1, SECRET 'secret_name')");
	}

	auto catalog = make_uniq<D1Catalog>(db, info.path.empty() ? name : info.path, secret_name);

	// Create views for all tables
	catalog->CreateViewsForAllTables(context);

	return std::move(catalog);
}

static unique_ptr<TransactionManager> D1CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                 AttachedDatabase &db, Catalog &catalog) {
	// D1 uses the DuckTransactionManager since views are in the default catalog
	return make_uniq<DuckTransactionManager>(db);
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
