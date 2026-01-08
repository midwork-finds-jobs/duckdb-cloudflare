#include "r2_extension.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

//=============================================================================
// r2_sql_query() - Execute arbitrary R2 SQL query
//=============================================================================

struct R2SQLQueryBindData : public TableFunctionData {
	R2SQLConfig config;
	string sql;
	bool finished = false;
};

static unique_ptr<FunctionData> R2SQLQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<R2SQLQueryBindData>();

	// Parameters: secret_name, bucket_name, sql
	if (input.inputs.size() != 3) {
		throw InvalidInputException("r2_sql_query requires 3 parameters: secret_name, bucket_name, sql_query");
	}

	string secret_name = input.inputs[0].ToString();
	string bucket_name = input.inputs[1].ToString();
	result->sql = input.inputs[2].ToString();

	result->config = GetR2SQLConfigFromSecret(context, secret_name);
	result->config.bucket_name = bucket_name;

	// Return raw JSON response for now
	return_types = {LogicalType::VARCHAR};
	names = {"response"};

	return std::move(result);
}

static void R2SQLQueryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<R2SQLQueryBindData>();

	if (data.finished) {
		return;
	}

	// Execute query
	auto result = R2SQLQuery(data.config, data.sql);

	if (!result.success) {
		throw IOException("R2 SQL query failed: %s", result.error);
	}

	// Return raw response
	output.SetCardinality(1);
	output.SetValue(0, 0, Value(result.raw_response));
	data.finished = true;
}

void RegisterR2SQLQueryFunction(ExtensionLoader &loader) {
	TableFunction r2_sql_query("r2_sql_query", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                           R2SQLQueryFunction, R2SQLQueryBind);
	loader.RegisterFunction(r2_sql_query);
}

//=============================================================================
// r2_sql_databases() - List databases/namespaces
//=============================================================================

struct R2SQLDatabasesBindData : public TableFunctionData {
	R2SQLConfig config;
	bool finished = false;
};

static unique_ptr<FunctionData> R2SQLDatabasesBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<R2SQLDatabasesBindData>();

	// Parameters: secret_name, bucket_name
	if (input.inputs.size() != 2) {
		throw InvalidInputException("r2_sql_databases requires 2 parameters: secret_name, bucket_name");
	}

	string secret_name = input.inputs[0].ToString();
	string bucket_name = input.inputs[1].ToString();

	result->config = GetR2SQLConfigFromSecret(context, secret_name);
	result->config.bucket_name = bucket_name;

	// Return database/namespace names
	return_types = {LogicalType::VARCHAR};
	names = {"namespace"};

	return std::move(result);
}

static void R2SQLDatabasesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<R2SQLDatabasesBindData>();

	if (data.finished) {
		return;
	}

	// Execute SHOW DATABASES query
	auto result = R2SQLListDatabases(data.config);

	if (!result.success) {
		throw IOException("R2 SQL SHOW DATABASES failed: %s", result.error);
	}

	// Return raw response for now
	output.SetCardinality(1);
	output.SetValue(0, 0, Value(result.raw_response));
	data.finished = true;
}

void RegisterR2SQLDatabasesFunction(ExtensionLoader &loader) {
	TableFunction r2_sql_databases("r2_sql_databases", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               R2SQLDatabasesFunction, R2SQLDatabasesBind);
	loader.RegisterFunction(r2_sql_databases);
}

//=============================================================================
// r2_sql_tables() - List tables in namespace
//=============================================================================

struct R2SQLTablesBindData : public TableFunctionData {
	R2SQLConfig config;
	string namespace_name;
	bool finished = false;
};

static unique_ptr<FunctionData> R2SQLTablesBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<R2SQLTablesBindData>();

	// Parameters: secret_name, bucket_name, namespace (optional)
	if (input.inputs.size() < 2 || input.inputs.size() > 3) {
		throw InvalidInputException("r2_sql_tables requires 2-3 parameters: secret_name, bucket_name, [namespace]");
	}

	string secret_name = input.inputs[0].ToString();
	string bucket_name = input.inputs[1].ToString();
	result->namespace_name = input.inputs.size() > 2 ? input.inputs[2].ToString() : "";

	result->config = GetR2SQLConfigFromSecret(context, secret_name);
	result->config.bucket_name = bucket_name;

	// Return table names
	return_types = {LogicalType::VARCHAR};
	names = {"table_name"};

	return std::move(result);
}

static void R2SQLTablesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<R2SQLTablesBindData>();

	if (data.finished) {
		return;
	}

	// Execute SHOW TABLES query
	auto result = R2SQLListTables(data.config, data.namespace_name);

	if (!result.success) {
		throw IOException("R2 SQL SHOW TABLES failed: %s", result.error);
	}

	// Return raw response for now
	output.SetCardinality(1);
	output.SetValue(0, 0, Value(result.raw_response));
	data.finished = true;
}

void RegisterR2SQLTablesFunction(ExtensionLoader &loader) {
	TableFunction r2_sql_tables("r2_sql_tables", {LogicalType::VARCHAR, LogicalType::VARCHAR}, R2SQLTablesFunction,
	                            R2SQLTablesBind);

	// Add optional namespace parameter
	r2_sql_tables.arguments.push_back(LogicalType::VARCHAR);

	loader.RegisterFunction(r2_sql_tables);
}

//=============================================================================
// r2_sql_describe() - Describe table schema
//=============================================================================

struct R2SQLDescribeBindData : public TableFunctionData {
	R2SQLConfig config;
	string table_name;
	bool finished = false;
};

static unique_ptr<FunctionData> R2SQLDescribeBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<R2SQLDescribeBindData>();

	// Parameters: secret_name, bucket_name, table_name
	if (input.inputs.size() != 3) {
		throw InvalidInputException("r2_sql_describe requires 3 parameters: secret_name, bucket_name, table_name");
	}

	string secret_name = input.inputs[0].ToString();
	string bucket_name = input.inputs[1].ToString();
	result->table_name = input.inputs[2].ToString();

	result->config = GetR2SQLConfigFromSecret(context, secret_name);
	result->config.bucket_name = bucket_name;

	// Return column info
	return_types = {LogicalType::VARCHAR};
	names = {"schema"};

	return std::move(result);
}

static void R2SQLDescribeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<R2SQLDescribeBindData>();

	if (data.finished) {
		return;
	}

	// Execute DESCRIBE query
	auto result = R2SQLDescribeTable(data.config, data.table_name);

	if (!result.success) {
		throw IOException("R2 SQL DESCRIBE failed: %s", result.error);
	}

	// Return raw response for now
	output.SetCardinality(1);
	output.SetValue(0, 0, Value(result.raw_response));
	data.finished = true;
}

void RegisterR2SQLDescribeFunction(ExtensionLoader &loader) {
	TableFunction r2_sql_describe("r2_sql_describe", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                              R2SQLDescribeFunction, R2SQLDescribeBind);
	loader.RegisterFunction(r2_sql_describe);
}

} // namespace duckdb
