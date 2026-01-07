#include "d1_extension.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

// ========================================
// D1_QUERY TABLE FUNCTION
// Executes arbitrary SQL against D1
// ========================================

struct D1QueryBindData : public TableFunctionData {
	D1Config config;
	string sql;
	D1QueryResult result;
	bool executed;

	D1QueryBindData() : executed(false) {
	}
};

struct D1QueryGlobalState : public GlobalTableFunctionState {
	idx_t current_row;

	D1QueryGlobalState() : current_row(0) {
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Helper to resolve D1 config from parameters, secret, or environment
static D1Config ResolveD1Config(ClientContext &context, TableFunctionBindInput &input) {
	D1Config config;

	// Check for secret parameter first
	auto secret_it = input.named_parameters.find("secret");
	if (secret_it != input.named_parameters.end()) {
		config = GetD1ConfigFromSecret(context, secret_it->second.GetValue<string>());
	} else {
		// Get credentials from parameters or environment
		auto account_id_it = input.named_parameters.find("account_id");
		if (account_id_it != input.named_parameters.end()) {
			config.account_id = account_id_it->second.GetValue<string>();
		} else {
			const char *env = std::getenv("CLOUDFLARE_ACCOUNT_ID");
			if (env) {
				config.account_id = env;
			}
		}

		auto api_token_it = input.named_parameters.find("api_token");
		if (api_token_it != input.named_parameters.end()) {
			config.api_token = api_token_it->second.GetValue<string>();
		} else {
			const char *env = std::getenv("CLOUDFLARE_API_TOKEN");
			if (env) {
				config.api_token = env;
			}
		}
	}

	// Validate we have credentials
	if (config.account_id.empty()) {
		throw BinderException("account_id required (via secret, parameter, or CLOUDFLARE_ACCOUNT_ID env)");
	}
	if (config.api_token.empty()) {
		throw BinderException("api_token required (via secret, parameter, or CLOUDFLARE_API_TOKEN env)");
	}

	// Get database ID
	auto database_id_it = input.named_parameters.find("database_id");
	auto database_it = input.named_parameters.find("database");

	if (database_id_it != input.named_parameters.end()) {
		config.database_id = database_id_it->second.GetValue<string>();
	} else if (database_it != input.named_parameters.end()) {
		string db_name = database_it->second.GetValue<string>();
		config.database_id = D1GetDatabaseIdByName(config, db_name);
	} else {
		const char *env = std::getenv("CLOUDFLARE_D1_DATABASE_ID");
		if (env) {
			config.database_id = env;
		}
	}

	return config;
}

static unique_ptr<FunctionData> D1QueryBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<D1QueryBindData>();

	// Get required parameters
	if (input.inputs.empty()) {
		throw BinderException("d1_query requires at least 'sql' parameter");
	}

	bind_data->sql = input.inputs[0].GetValue<string>();

	// Resolve config from secret/parameters/environment
	bind_data->config = ResolveD1Config(context, input);

	if (bind_data->config.database_id.empty()) {
		throw BinderException("database or database_id required (parameter or CLOUDFLARE_D1_DATABASE_ID env)");
	}

	// Execute the query to get schema
	bind_data->result = D1ExecuteQuery(bind_data->config, bind_data->sql);
	bind_data->executed = true;

	if (!bind_data->result.success) {
		throw IOException("D1 query failed: " + bind_data->result.error);
	}

	// Infer column types from first row or use VARCHAR for all
	if (bind_data->result.results.empty()) {
		// No results - return empty with single column
		names.push_back("result");
		return_types.push_back(LogicalType::VARCHAR);
	} else {
		// Use column order from result
		for (const auto &col : bind_data->result.column_order) {
			names.push_back(col);
			// Try to infer type from first row value
			// For now, use VARCHAR for everything (D1 returns strings anyway)
			return_types.push_back(LogicalType::VARCHAR);
		}
	}

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> D1QueryInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<D1QueryGlobalState>();
}

static void D1QueryFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<D1QueryBindData>();
	auto &state = data.global_state->Cast<D1QueryGlobalState>();

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (state.current_row < bind_data.result.results.size() && count < max_count) {
		const auto &row = bind_data.result.results[state.current_row];

		for (idx_t col_idx = 0; col_idx < bind_data.result.column_order.size(); col_idx++) {
			const string &col_name = bind_data.result.column_order[col_idx];
			auto it = row.find(col_name);
			if (it != row.end() && !it->second.empty()) {
				output.SetValue(col_idx, count, Value(it->second));
			} else {
				output.SetValue(col_idx, count, Value());
			}
		}

		state.current_row++;
		count++;
	}

	output.SetCardinality(count);
}

void RegisterD1QueryFunction(ExtensionLoader &loader) {
	TableFunction func("d1_query", {LogicalType::VARCHAR}, D1QueryFunction, D1QueryBind, D1QueryInitGlobal);

	func.named_parameters["secret"] = LogicalType::VARCHAR;
	func.named_parameters["account_id"] = LogicalType::VARCHAR;
	func.named_parameters["api_token"] = LogicalType::VARCHAR;
	func.named_parameters["database_id"] = LogicalType::VARCHAR;
	func.named_parameters["database"] = LogicalType::VARCHAR;

	loader.RegisterFunction(func);
}

// ========================================
// D1_DATABASES TABLE FUNCTION
// Lists all D1 databases in account
// ========================================

struct D1DatabasesBindData : public TableFunctionData {
	D1Config config;
	vector<D1DatabaseInfo> databases;
};

struct D1DatabasesGlobalState : public GlobalTableFunctionState {
	idx_t current_row;

	D1DatabasesGlobalState() : current_row(0) {
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> D1DatabasesBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<D1DatabasesBindData>();

	// Check for secret parameter first
	auto secret_it = input.named_parameters.find("secret");
	if (secret_it != input.named_parameters.end()) {
		bind_data->config = GetD1ConfigFromSecret(context, secret_it->second.GetValue<string>());
	} else {
		// Get credentials from parameters or environment
		auto account_id_it = input.named_parameters.find("account_id");
		if (account_id_it != input.named_parameters.end()) {
			bind_data->config.account_id = account_id_it->second.GetValue<string>();
		} else {
			const char *env = std::getenv("CLOUDFLARE_ACCOUNT_ID");
			if (env) {
				bind_data->config.account_id = env;
			}
		}

		auto api_token_it = input.named_parameters.find("api_token");
		if (api_token_it != input.named_parameters.end()) {
			bind_data->config.api_token = api_token_it->second.GetValue<string>();
		} else {
			const char *env = std::getenv("CLOUDFLARE_API_TOKEN");
			if (env) {
				bind_data->config.api_token = env;
			}
		}
	}

	if (bind_data->config.account_id.empty()) {
		throw BinderException("account_id required (via secret, parameter, or CLOUDFLARE_ACCOUNT_ID env)");
	}
	if (bind_data->config.api_token.empty()) {
		throw BinderException("api_token required (via secret, parameter, or CLOUDFLARE_API_TOKEN env)");
	}

	// Fetch databases
	bind_data->databases = D1ListDatabases(bind_data->config);

	// Define output schema
	names = {"uuid", "name", "created_at", "version", "file_size", "num_tables", "region"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::BIGINT,  LogicalType::INTEGER, LogicalType::VARCHAR};

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> D1DatabasesInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	return make_uniq<D1DatabasesGlobalState>();
}

static void D1DatabasesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<D1DatabasesBindData>();
	auto &state = data.global_state->Cast<D1DatabasesGlobalState>();

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (state.current_row < bind_data.databases.size() && count < max_count) {
		const auto &db = bind_data.databases[state.current_row];

		output.SetValue(0, count, Value(db.uuid));
		output.SetValue(1, count, Value(db.name));
		output.SetValue(2, count, Value(db.created_at));
		output.SetValue(3, count, Value(db.version));
		output.SetValue(4, count, Value::BIGINT(db.file_size));
		output.SetValue(5, count, Value::INTEGER(db.num_tables));
		output.SetValue(6, count, Value(db.region));

		state.current_row++;
		count++;
	}

	output.SetCardinality(count);
}

void RegisterD1DatabasesFunction(ExtensionLoader &loader) {
	TableFunction func("d1_databases", {}, D1DatabasesFunction, D1DatabasesBind, D1DatabasesInitGlobal);

	func.named_parameters["secret"] = LogicalType::VARCHAR;
	func.named_parameters["account_id"] = LogicalType::VARCHAR;
	func.named_parameters["api_token"] = LogicalType::VARCHAR;

	loader.RegisterFunction(func);
}

// ========================================
// D1_TABLES TABLE FUNCTION
// Lists all tables in a D1 database
// ========================================

struct D1TablesBindData : public TableFunctionData {
	D1Config config;
	vector<D1TableInfo> tables;
};

struct D1TablesGlobalState : public GlobalTableFunctionState {
	idx_t current_row;

	D1TablesGlobalState() : current_row(0) {
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> D1TablesBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<D1TablesBindData>();

	// Check for secret parameter first
	auto secret_it = input.named_parameters.find("secret");
	if (secret_it != input.named_parameters.end()) {
		bind_data->config = GetD1ConfigFromSecret(context, secret_it->second.GetValue<string>());
	} else {
		// Get credentials
		auto account_id_it = input.named_parameters.find("account_id");
		if (account_id_it != input.named_parameters.end()) {
			bind_data->config.account_id = account_id_it->second.GetValue<string>();
		} else {
			const char *env = std::getenv("CLOUDFLARE_ACCOUNT_ID");
			if (env) {
				bind_data->config.account_id = env;
			}
		}

		auto api_token_it = input.named_parameters.find("api_token");
		if (api_token_it != input.named_parameters.end()) {
			bind_data->config.api_token = api_token_it->second.GetValue<string>();
		} else {
			const char *env = std::getenv("CLOUDFLARE_API_TOKEN");
			if (env) {
				bind_data->config.api_token = env;
			}
		}
	}

	if (bind_data->config.account_id.empty()) {
		throw BinderException("account_id required");
	}
	if (bind_data->config.api_token.empty()) {
		throw BinderException("api_token required");
	}

	// Get database
	auto database_id_it = input.named_parameters.find("database_id");
	auto database_it = input.named_parameters.find("database");

	if (database_id_it != input.named_parameters.end()) {
		bind_data->config.database_id = database_id_it->second.GetValue<string>();
	} else if (database_it != input.named_parameters.end()) {
		string db_name = database_it->second.GetValue<string>();
		bind_data->config.database_id = D1GetDatabaseIdByName(bind_data->config, db_name);
	} else {
		const char *env = std::getenv("CLOUDFLARE_D1_DATABASE_ID");
		if (env) {
			bind_data->config.database_id = env;
		} else {
			throw BinderException("database or database_id required");
		}
	}

	// Fetch tables
	bind_data->tables = D1GetTables(bind_data->config);

	// Define output schema
	names = {"schema", "name", "type", "ncol", "writable", "strict"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::INTEGER, LogicalType::BOOLEAN, LogicalType::BOOLEAN};

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> D1TablesInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<D1TablesGlobalState>();
}

static void D1TablesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<D1TablesBindData>();
	auto &state = data.global_state->Cast<D1TablesGlobalState>();

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (state.current_row < bind_data.tables.size() && count < max_count) {
		const auto &table = bind_data.tables[state.current_row];

		output.SetValue(0, count, Value(table.schema));
		output.SetValue(1, count, Value(table.name));
		output.SetValue(2, count, Value(table.type));
		output.SetValue(3, count, Value::INTEGER(table.ncol));
		output.SetValue(4, count, Value::BOOLEAN(table.writable));
		output.SetValue(5, count, Value::BOOLEAN(table.strict));

		state.current_row++;
		count++;
	}

	output.SetCardinality(count);
}

void RegisterD1TablesFunction(ExtensionLoader &loader) {
	TableFunction func("d1_tables", {}, D1TablesFunction, D1TablesBind, D1TablesInitGlobal);

	func.named_parameters["secret"] = LogicalType::VARCHAR;
	func.named_parameters["account_id"] = LogicalType::VARCHAR;
	func.named_parameters["api_token"] = LogicalType::VARCHAR;
	func.named_parameters["database_id"] = LogicalType::VARCHAR;
	func.named_parameters["database"] = LogicalType::VARCHAR;

	loader.RegisterFunction(func);
}

// ========================================
// D1_EXECUTE SCALAR FUNCTION
// Executes SQL and returns affected row count
// d1_execute(sql, secret_name, database_id)
// ========================================

static void D1ExecuteScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &sql_vector = args.data[0];
	auto &secret_vector = args.data[1];
	auto &database_vector = args.data[2];

	// Execute for each row
	for (idx_t i = 0; i < args.size(); i++) {
		string sql = sql_vector.GetValue(i).ToString();
		string secret_name = secret_vector.GetValue(i).ToString();
		string database_id = database_vector.GetValue(i).ToString();

		D1Config config = GetD1ConfigFromSecret(context, secret_name);
		config.database_id = database_id;

		auto query_result = D1ExecuteQuery(config, sql);
		if (!query_result.success) {
			throw IOException("D1 execute failed: " + query_result.error);
		}
		result.SetValue(i, Value::BIGINT(query_result.meta.changes));
	}
}

void RegisterD1ExecuteFunction(ExtensionLoader &loader) {
	ScalarFunction func("d1_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                    LogicalType::BIGINT, D1ExecuteScalarFunction);
	loader.RegisterFunction(func);
}

} // namespace duckdb
