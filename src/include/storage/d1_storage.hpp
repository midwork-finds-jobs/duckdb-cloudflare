#pragma once

#include "d1_extension.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"

namespace duckdb {

// Forward declarations
class D1TransactionManager;

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

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void DropSchema(ClientContext &context, DropInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override {
		// D1 only has the main schema
	}

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override {
		// D1 uses views in the default DuckDB catalog, not a separate schema catalog
		return nullptr;
	}

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;

	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;

	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	bool InMemory() override {
		return false;
	}

	string GetDBPath() override {
		return database_name;
	}

	void CreateViewsForAllTables(ClientContext &context);

	// Get D1 configuration
	D1Config GetConfig() const {
		// This will be initialized lazily when needed
		// For now, return a config with database_name
		D1Config config;
		config.database_name = database_name;
		return config;
	}

	const string &GetDatabaseName() const {
		return database_name;
	}

	const string &GetSecretName() const {
		return secret_name;
	}

private:
	string database_name;
	string secret_name;
};

} // namespace duckdb
