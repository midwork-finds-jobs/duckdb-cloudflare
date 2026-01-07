#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>
#include <vector>
#include <unordered_map>

namespace duckdb {

// ========================================
// D1 API CONFIGURATION
// ========================================

struct D1Config {
	string account_id;
	string api_token;
	string database_id;   // UUID of the database
	string database_name; // Human-readable name (optional, for lookup)

	D1Config() = default;
	D1Config(string account, string token, string db_id)
	    : account_id(std::move(account)), api_token(std::move(token)), database_id(std::move(db_id)) {
	}

	// Build the query endpoint URL
	string GetQueryUrl() const {
		return "https://api.cloudflare.com/client/v4/accounts/" + account_id + "/d1/database/" + database_id + "/query";
	}

	// Build the raw query endpoint URL (returns arrays instead of objects)
	string GetRawQueryUrl() const {
		return "https://api.cloudflare.com/client/v4/accounts/" + account_id + "/d1/database/" + database_id + "/raw";
	}

	// Build the list databases endpoint URL
	string GetListDatabasesUrl() const {
		return "https://api.cloudflare.com/client/v4/accounts/" + account_id + "/d1/database";
	}
};

// ========================================
// D1 TABLE SCHEMA
// ========================================

struct D1ColumnInfo {
	int cid;           // Column index
	string name;       // Column name
	string type;       // SQLite type (INTEGER, TEXT, REAL, BLOB, etc.)
	bool notnull;      // NOT NULL constraint
	string dflt_value; // Default value
	bool pk;           // Primary key

	D1ColumnInfo() : cid(0), notnull(false), pk(false) {
	}
};

struct D1TableInfo {
	string schema; // "main" or "temp"
	string name;   // Table name
	string type;   // "table" or "view"
	int ncol;      // Number of columns
	bool writable; // Whether table is writable
	bool strict;   // Strict mode
	vector<D1ColumnInfo> columns;

	D1TableInfo() : ncol(0), writable(false), strict(false) {
	}
};

// ========================================
// D1 QUERY RESULT
// ========================================

struct D1QueryMeta {
	bool served_by_primary;
	string served_by_region;
	double duration_ms;
	int64_t changes;
	int64_t last_row_id;
	bool changed_db;
	int64_t size_after;
	int64_t rows_read;
	int64_t rows_written;

	D1QueryMeta()
	    : served_by_primary(false), duration_ms(0), changes(0), last_row_id(0), changed_db(false), size_after(0),
	      rows_read(0), rows_written(0) {
	}
};

struct D1QueryResult {
	bool success;
	D1QueryMeta meta;
	vector<unordered_map<string, string>> results; // Each row as key-value pairs
	vector<string> column_order;                   // Preserve column order from response
	string error;

	D1QueryResult() : success(false) {
	}
};

// ========================================
// D1 DATABASE INFO
// ========================================

struct D1DatabaseInfo {
	string uuid;
	string name;
	string created_at;
	string version;
	int64_t file_size;
	int num_tables;
	string region;

	D1DatabaseInfo() : file_size(0), num_tables(0) {
	}
};

// ========================================
// HTTP CLIENT INTERFACE
// ========================================

// Execute a SQL query against D1 and return the result
D1QueryResult D1ExecuteQuery(const D1Config &config, const string &sql, const vector<string> &params = {});

// List all databases in the account
vector<D1DatabaseInfo> D1ListDatabases(const D1Config &config);

// Get database UUID by name
string D1GetDatabaseIdByName(const D1Config &config, const string &name);

// Get table list from database
vector<D1TableInfo> D1GetTables(const D1Config &config);

// Get column info for a table
vector<D1ColumnInfo> D1GetTableColumns(const D1Config &config, const string &table_name);

// ========================================
// TYPE MAPPING
// ========================================

// Convert SQLite type to DuckDB LogicalType
LogicalType SQLiteTypeToDuckDB(const string &sqlite_type);

// ========================================
// TABLE FUNCTION REGISTRATION
// ========================================

// Register the d1_query table function
void RegisterD1QueryFunction(ExtensionLoader &loader);

// Register the d1_databases table function
void RegisterD1DatabasesFunction(ExtensionLoader &loader);

// Register the d1_tables table function
void RegisterD1TablesFunction(ExtensionLoader &loader);

// Register the d1_execute scalar function
void RegisterD1ExecuteFunction(ExtensionLoader &loader);

// ========================================
// SECRET MANAGEMENT
// ========================================

// Register the D1 secret type
void RegisterD1SecretType(ExtensionLoader &loader);

// Get D1 config from a named secret
D1Config GetD1ConfigFromSecret(ClientContext &context, const string &secret_name);

// ========================================
// ATTACH SUPPORT
// ========================================

// Register D1 storage extension for ATTACH DATABASE syntax
void RegisterD1StorageExtension(DatabaseInstance &db);

// Register d1_scan table function
void RegisterD1ScanFunction(ExtensionLoader &loader);

// Optimizer for LIMIT pushdown
void OptimizeD1ScanLimitPushdown(unique_ptr<LogicalOperator> &op);

} // namespace duckdb
