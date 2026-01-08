#define DUCKDB_EXTENSION_MAIN

#include "cloudflare_extension.hpp"
#include "d1_extension.hpp"
#include "r2_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

// D1 optimizer for LIMIT pushdown
void D1Optimizer(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeD1ScanLimitPushdown(plan);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register Cloudflare D1 functions
	RegisterD1QueryFunction(loader);
	RegisterD1DatabasesFunction(loader);
	RegisterD1TablesFunction(loader);
	RegisterD1ExecuteFunction(loader);

	// Register D1 secret type for CREATE SECRET TYPE D1
	RegisterD1SecretType(loader);

	// Register D1 storage extension for ATTACH DATABASE syntax
	RegisterD1StorageExtension(loader.GetDatabaseInstance());

	// Register d1_scan table function
	RegisterD1ScanFunction(loader);

	// Register Cloudflare R2 SQL functions
	RegisterR2SQLQueryFunction(loader);
	RegisterR2SQLDatabasesFunction(loader);
	RegisterR2SQLTablesFunction(loader);
	RegisterR2SQLDescribeFunction(loader);

	// Register R2 SQL secret type
	RegisterR2SQLSecretType(loader);

	// Register optimizer extension for LIMIT pushdown
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	OptimizerExtension optimizer;
	optimizer.optimize_function = D1Optimizer;
	config.optimizer_extensions.push_back(std::move(optimizer));
}

void CloudflareExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string CloudflareExtension::Name() {
	return "cloudflare";
}

std::string CloudflareExtension::Version() const {
#ifdef EXT_VERSION_CLOUDFLARE
	return EXT_VERSION_CLOUDFLARE;
#else
	return "0.1.0";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(cloudflare, loader) {
	duckdb::LoadInternal(loader);
}
}
