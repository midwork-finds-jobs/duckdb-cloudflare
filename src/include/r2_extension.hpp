#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Forward declarations
struct R2SQLConfig;
struct R2SQLQueryResult;

// R2 SQL Configuration
struct R2SQLConfig {
	string account_id;
	string api_token;
	string bucket_name;

	string GetQueryUrl() const {
		return StringUtil::Format("https://api.sql.cloudflarestorage.com/api/v1/accounts/%s/r2-sql/query/%s",
		                          account_id, bucket_name);
	}
};

// R2 SQL Query Result
struct R2SQLQueryResult {
	bool success;
	string error;
	string raw_response;

	R2SQLQueryResult() : success(false) {
	}
};

// R2 SQL Secret Functions
void RegisterR2SQLSecretType(ExtensionLoader &loader);
R2SQLConfig GetR2SQLConfigFromSecret(ClientContext &context, const string &secret_name);

// R2 SQL HTTP Functions
R2SQLQueryResult R2SQLQuery(const R2SQLConfig &config, const string &sql);
R2SQLQueryResult R2SQLListDatabases(const R2SQLConfig &config);
R2SQLQueryResult R2SQLListTables(const R2SQLConfig &config, const string &namespace_name);
R2SQLQueryResult R2SQLDescribeTable(const R2SQLConfig &config, const string &table_name);

// R2 SQL Table Functions
void RegisterR2SQLQueryFunction(ExtensionLoader &loader);
void RegisterR2SQLDatabasesFunction(ExtensionLoader &loader);
void RegisterR2SQLTablesFunction(ExtensionLoader &loader);
void RegisterR2SQLDescribeFunction(ExtensionLoader &loader);

} // namespace duckdb
