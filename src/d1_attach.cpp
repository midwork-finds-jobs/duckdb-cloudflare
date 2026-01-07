#include "d1_extension.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

#include <mutex>
#include <unordered_map>

namespace duckdb {

// ========================================
// D1 ATTACHED DATABASE REGISTRY
// ========================================

// Global registry of attached D1 databases
struct D1AttachedDatabase {
	string secret_name;
	string database_name; // Human-readable name
	string database_id;   // UUID
	D1Config config;
	vector<D1TableInfo> tables;
};

static std::mutex g_d1_registry_mutex;
static std::unordered_map<string, D1AttachedDatabase> g_d1_attached_databases;

// ========================================
// D1_ATTACH FUNCTION
// ========================================

struct D1AttachBindData : public FunctionData {
	string alias;
	string secret_name;
	string database_name;
	string database_id;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<D1AttachBindData>();
		result->alias = alias;
		result->secret_name = secret_name;
		result->database_name = database_name;
		result->database_id = database_id;
		return std::move(result);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<D1AttachBindData>();
		return alias == o.alias && secret_name == o.secret_name && database_name == o.database_name;
	}
};

static unique_ptr<FunctionData> D1AttachBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<D1AttachBindData>();

	// Parse input: d1_attach('database_name_or_id', 'secret_name', 'alias')
	if (input.inputs.size() < 2) {
		throw BinderException("d1_attach requires: database_name_or_id, secret_name [, alias]");
	}

	string db_input = input.inputs[0].GetValue<string>();
	bind_data->secret_name = input.inputs[1].GetValue<string>();

	if (input.inputs.size() >= 3) {
		bind_data->alias = input.inputs[2].GetValue<string>();
	} else {
		// Use database name as alias (extract from db_input if it looks like a UUID)
		bind_data->alias = db_input;
	}

	// Get D1 config from secret
	D1Config config = GetD1ConfigFromSecret(context, bind_data->secret_name);

	// Determine if input is UUID or name
	bool is_uuid = (db_input.size() == 36 && db_input[8] == '-' && db_input[13] == '-');

	if (is_uuid) {
		bind_data->database_id = db_input;
		// Try to get name from list
		auto databases = D1ListDatabases(config);
		for (const auto &db : databases) {
			if (db.uuid == db_input) {
				bind_data->database_name = db.name;
				break;
			}
		}
		if (bind_data->database_name.empty()) {
			bind_data->database_name = db_input;
		}
	} else {
		bind_data->database_name = db_input;
		bind_data->database_id = D1GetDatabaseIdByName(config, db_input);
	}

	// Update alias if it was defaulted to UUID
	if (bind_data->alias == bind_data->database_id && !bind_data->database_name.empty()) {
		bind_data->alias = bind_data->database_name;
	}

	// Store config for later
	config.database_id = bind_data->database_id;
	config.database_name = bind_data->database_name;

	// Fetch table list
	vector<D1TableInfo> tables = D1GetTables(config);

	// Register in global registry
	{
		std::lock_guard<std::mutex> lock(g_d1_registry_mutex);

		D1AttachedDatabase attached;
		attached.secret_name = bind_data->secret_name;
		attached.database_name = bind_data->database_name;
		attached.database_id = bind_data->database_id;
		attached.config = config;
		attached.tables = std::move(tables);

		g_d1_attached_databases[bind_data->alias] = std::move(attached);
	}

	// Return schema (success message)
	names = {"database", "alias", "tables"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER};

	return std::move(bind_data);
}

struct D1AttachGlobalState : public GlobalTableFunctionState {
	bool done = false;
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<GlobalTableFunctionState> D1AttachInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<D1AttachGlobalState>();
}

static void D1AttachFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<D1AttachGlobalState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	auto &bind_data = data.bind_data->Cast<D1AttachBindData>();

	// Get table count from registry
	int table_count = 0;
	{
		std::lock_guard<std::mutex> lock(g_d1_registry_mutex);
		auto it = g_d1_attached_databases.find(bind_data.alias);
		if (it != g_d1_attached_databases.end()) {
			table_count = static_cast<int>(it->second.tables.size());
		}
	}

	output.SetValue(0, 0, Value(bind_data.database_name));
	output.SetValue(1, 0, Value(bind_data.alias));
	output.SetValue(2, 0, Value::INTEGER(table_count));
	output.SetCardinality(1);

	state.done = true;
}

void RegisterD1AttachFunction(ExtensionLoader &loader) {
	TableFunction func("d1_attach", {LogicalType::VARCHAR, LogicalType::VARCHAR}, D1AttachFunction, D1AttachBind,
	                   D1AttachInitGlobal);
	func.varargs = LogicalType::VARCHAR; // Optional alias as 3rd arg

	loader.RegisterFunction(func);
}

// ========================================
// D1_DETACH FUNCTION
// ========================================

static void D1DetachScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &alias_vector = args.data[0];

	UnaryExecutor::Execute<string_t, bool>(alias_vector, result, args.size(), [&](string_t alias) {
		std::lock_guard<std::mutex> lock(g_d1_registry_mutex);
		auto it = g_d1_attached_databases.find(alias.GetString());
		if (it != g_d1_attached_databases.end()) {
			g_d1_attached_databases.erase(it);
			return true;
		}
		return false;
	});
}

void RegisterD1DetachFunction(ExtensionLoader &loader) {
	ScalarFunction func("d1_detach", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, D1DetachScalarFunction);
	loader.RegisterFunction(func);
}

// ========================================
// REPLACEMENT SCAN FOR D1 TABLES
// ========================================

// Check if a table reference matches an attached D1 database
static unique_ptr<TableRef> D1ReplacementScan(ClientContext &context, ReplacementScanInput &input,
                                              optional_ptr<ReplacementScanData> data) {
	// Parse table name - could be "alias.table" or just "table"
	string table_name = input.table_name;
	string db_alias;

	// Check for qualified name (alias.table)
	size_t dot_pos = table_name.find('.');
	if (dot_pos != string::npos) {
		db_alias = table_name.substr(0, dot_pos);
		table_name = table_name.substr(dot_pos + 1);
	}

	// Look up in registry
	std::lock_guard<std::mutex> lock(g_d1_registry_mutex);

	D1AttachedDatabase *attached = nullptr;

	if (!db_alias.empty()) {
		// Qualified name - look up specific alias
		auto it = g_d1_attached_databases.find(db_alias);
		if (it == g_d1_attached_databases.end()) {
			return nullptr;
		}
		attached = &it->second;
	} else {
		// Unqualified name - this could match any attached database
		// For now, we don't do unqualified replacement (user must use alias.table)
		return nullptr;
	}

	// Check if table exists in this database
	bool table_found = false;
	for (const auto &t : attached->tables) {
		if (t.name == table_name) {
			table_found = true;
			break;
		}
	}

	if (!table_found) {
		return nullptr;
	}

	// Create a table function reference to d1_scan
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;

	// d1_scan(table_name, secret, database_id)
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	children.push_back(make_uniq<ConstantExpression>(Value(attached->secret_name)));
	children.push_back(make_uniq<ConstantExpression>(Value(attached->database_id)));

	table_function->function = make_uniq<FunctionExpression>("d1_scan", std::move(children));

	return std::move(table_function);
}

// ========================================
// D1_SCAN TABLE FUNCTION
// Used by replacement scan
// ========================================

struct D1ScanBindData : public TableFunctionData {
	D1Config config;
	string table_name;
	vector<string> column_names;
	vector<LogicalType> column_types;
	D1QueryResult result;
	bool executed = false;
	string where_clause; // Pushed down WHERE clause
	idx_t limit = 0;     // Pushed down LIMIT (0 = no limit)
};

struct D1ScanGlobalState : public GlobalTableFunctionState {
	idx_t current_row = 0;
	vector<idx_t> column_ids; // Which columns were actually requested
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> D1ScanBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<D1ScanBindData>();

	if (input.inputs.size() < 3) {
		throw BinderException("d1_scan requires: table_name, secret_name, database_id");
	}

	bind_data->table_name = input.inputs[0].GetValue<string>();
	string secret_name = input.inputs[1].GetValue<string>();
	string database_id = input.inputs[2].GetValue<string>();

	// Get config from secret
	bind_data->config = GetD1ConfigFromSecret(context, secret_name);
	bind_data->config.database_id = database_id;

	// Get column info
	auto columns = D1GetTableColumns(bind_data->config, bind_data->table_name);

	for (const auto &col : columns) {
		names.push_back(col.name);
		return_types.push_back(SQLiteTypeToDuckDB(col.type));
		bind_data->column_names.push_back(col.name);
		bind_data->column_types.push_back(SQLiteTypeToDuckDB(col.type));
	}

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> D1ScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<D1ScanGlobalState>();
	// Store which columns were actually requested
	for (auto &col_id : input.column_ids) {
		state->column_ids.push_back(col_id);
	}
	return std::move(state);
}

// Helper: escape string for SQL (basic escaping)
static string EscapeSQLString(const string &str) {
	string result;
	result.reserve(str.size() + 2);
	result += "'";
	for (char c : str) {
		if (c == '\'') {
			result += "''";
		} else {
			result += c;
		}
	}
	result += "'";
	return result;
}

// Helper: convert comparison operator to SQL
static string ComparisonTypeToSQL(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		return "";
	}
}

// Helper: convert DuckDB value to SQL literal
static string ValueToSQL(const Value &value) {
	if (value.IsNull()) {
		return "NULL";
	}
	switch (value.type().id()) {
	case LogicalTypeId::VARCHAR:
		return EscapeSQLString(value.ToString());
	case LogicalTypeId::BOOLEAN:
		return value.GetValue<bool>() ? "1" : "0";
	default:
		return value.ToString();
	}
}

// Helper: convert a single comparison expression to SQL
static string ExpressionToSQL(Expression &expr, const vector<string> &column_names) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comp = expr.Cast<BoundComparisonExpression>();

		// Check for column op constant
		if (comp.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		    comp.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &col_ref = comp.left->Cast<BoundColumnRefExpression>();
			auto &constant = comp.right->Cast<BoundConstantExpression>();

			string op = ComparisonTypeToSQL(comp.type);
			if (!op.empty()) {
				return col_ref.GetName() + " " + op + " " + ValueToSQL(constant.value);
			}
		}
		// Check for constant op column (reversed)
		if (comp.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		    comp.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &col_ref = comp.right->Cast<BoundColumnRefExpression>();
			auto &constant = comp.left->Cast<BoundConstantExpression>();

			// Reverse the comparison
			ExpressionType reversed = comp.type;
			if (comp.type == ExpressionType::COMPARE_LESSTHAN) {
				reversed = ExpressionType::COMPARE_GREATERTHAN;
			} else if (comp.type == ExpressionType::COMPARE_GREATERTHAN) {
				reversed = ExpressionType::COMPARE_LESSTHAN;
			} else if (comp.type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
				reversed = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
			} else if (comp.type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				reversed = ExpressionType::COMPARE_LESSTHANOREQUALTO;
			}

			string op = ComparisonTypeToSQL(reversed);
			if (!op.empty()) {
				return col_ref.GetName() + " " + op + " " + ValueToSQL(constant.value);
			}
		}
	}
	return "";
}

// Filter pushdown for D1 - converts DuckDB filters to SQL WHERE clause
static void D1ScanPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                        vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<D1ScanBindData>();

	vector<string> sql_conditions;
	vector<idx_t> filters_to_remove;

	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];

		// Handle simple comparisons
		string sql = ExpressionToSQL(*filter, bind_data.column_names);
		if (!sql.empty()) {
			sql_conditions.push_back(sql);
			filters_to_remove.push_back(i);
			continue;
		}

		// Handle AND conjunctions
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
			auto &conjunction = filter->Cast<BoundConjunctionExpression>();
			if (conjunction.type == ExpressionType::CONJUNCTION_AND) {
				bool all_converted = true;
				vector<string> sub_conditions;
				for (auto &child : conjunction.children) {
					string child_sql = ExpressionToSQL(*child, bind_data.column_names);
					if (child_sql.empty()) {
						all_converted = false;
						break;
					}
					sub_conditions.push_back(child_sql);
				}
				if (all_converted && !sub_conditions.empty()) {
					string combined = "(";
					for (size_t j = 0; j < sub_conditions.size(); j++) {
						if (j > 0) {
							combined += " AND ";
						}
						combined += sub_conditions[j];
					}
					combined += ")";
					sql_conditions.push_back(combined);
					filters_to_remove.push_back(i);
				}
			}
		}
	}

	// Build WHERE clause
	if (!sql_conditions.empty()) {
		for (size_t i = 0; i < sql_conditions.size(); i++) {
			if (i > 0) {
				bind_data.where_clause += " AND ";
			}
			bind_data.where_clause += sql_conditions[i];
		}
	}

	// Remove filters we pushed down (in reverse order to maintain indices)
	for (auto it = filters_to_remove.rbegin(); it != filters_to_remove.rend(); ++it) {
		filters.erase(filters.begin() + *it);
	}
}

static void D1ScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<D1ScanBindData>();
	auto &state = data.global_state->Cast<D1ScanGlobalState>();

	// Execute query on first call
	if (!bind_data.executed) {
		string sql = "SELECT * FROM " + bind_data.table_name;
		if (!bind_data.where_clause.empty()) {
			sql += " WHERE " + bind_data.where_clause;
		}
		if (bind_data.limit > 0) {
			sql += " LIMIT " + std::to_string(bind_data.limit);
		}
		bind_data.result = D1ExecuteQuery(bind_data.config, sql);
		bind_data.executed = true;

		if (!bind_data.result.success) {
			throw IOException("D1 query failed: " + bind_data.result.error);
		}
	}

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (state.current_row < bind_data.result.results.size() && count < max_count) {
		const auto &row = bind_data.result.results[state.current_row];

		// Iterate over requested columns only (projection pushdown)
		for (idx_t out_idx = 0; out_idx < state.column_ids.size(); out_idx++) {
			idx_t col_idx = state.column_ids[out_idx];
			if (col_idx >= bind_data.column_names.size()) {
				// COLUMN_IDENTIFIER_ROW_ID or invalid column
				output.SetValue(out_idx, count, Value());
				continue;
			}

			const string &col_name = bind_data.column_names[col_idx];
			auto it = row.find(col_name);

			if (it != row.end() && !it->second.empty()) {
				// Convert string value to appropriate type
				auto &type = bind_data.column_types[col_idx];
				string val = it->second;

				switch (type.id()) {
				case LogicalTypeId::BIGINT:
					try {
						output.SetValue(out_idx, count, Value::BIGINT(std::stoll(val)));
					} catch (...) {
						output.SetValue(out_idx, count, Value());
					}
					break;
				case LogicalTypeId::DOUBLE:
					try {
						output.SetValue(out_idx, count, Value::DOUBLE(std::stod(val)));
					} catch (...) {
						output.SetValue(out_idx, count, Value());
					}
					break;
				case LogicalTypeId::BOOLEAN:
					output.SetValue(out_idx, count, Value::BOOLEAN(val == "1" || val == "true"));
					break;
				default:
					output.SetValue(out_idx, count, Value(val));
					break;
				}
			} else {
				output.SetValue(out_idx, count, Value());
			}
		}

		state.current_row++;
		count++;
	}

	output.SetCardinality(count);
}

void RegisterD1ScanFunction(ExtensionLoader &loader) {
	TableFunction func("d1_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, D1ScanFunction,
	                   D1ScanBind, D1ScanInitGlobal);
	func.projection_pushdown = true;
	func.pushdown_complex_filter = D1ScanPushdownComplexFilter;

	loader.RegisterFunction(func);
}

// ========================================
// REGISTER REPLACEMENT SCAN
// ========================================

void RegisterD1ReplacementScan(DatabaseInstance &db) {
	auto &config = DBConfig::GetConfig(db);
	config.replacement_scans.emplace_back(D1ReplacementScan, nullptr);
}

// ========================================
// D1_ATTACH_VIEWS FUNCTION
// Creates views for each table in attached DB
// ========================================

static void D1AttachViewsScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &alias_vector = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(alias_vector, result, args.size(), [&](string_t alias_str) {
		string alias = alias_str.GetString();

		std::lock_guard<std::mutex> lock(g_d1_registry_mutex);
		auto it = g_d1_attached_databases.find(alias);
		if (it == g_d1_attached_databases.end()) {
			throw IOException("D1 database not attached: " + alias);
		}

		auto &attached = it->second;
		string created_views;

		for (const auto &table : attached.tables) {
			// Create view: CREATE OR REPLACE VIEW alias_tablename AS SELECT * FROM d1_scan(...)
			string view_name = alias + "_" + table.name;
			string sql = "CREATE OR REPLACE VIEW " + view_name + " AS SELECT * FROM d1_scan('" + table.name + "', '" +
			             attached.secret_name + "', '" + attached.database_id + "')";

			try {
				auto conn = Connection(*context.db);
				conn.Query(sql);
				if (!created_views.empty()) {
					created_views += ", ";
				}
				created_views += view_name;
			} catch (std::exception &e) {
				// Ignore errors for now
			}
		}

		return StringVector::AddString(result, created_views);
	});
}

void RegisterD1AttachViewsFunction(ExtensionLoader &loader) {
	ScalarFunction func("d1_attach_views", {LogicalType::VARCHAR}, LogicalType::VARCHAR, D1AttachViewsScalarFunction);
	loader.RegisterFunction(func);
}

// ========================================
// LIMIT PUSHDOWN OPTIMIZER
// ========================================

void OptimizeD1ScanLimitPushdown(unique_ptr<LogicalOperator> &op) {
	// Handle TOP_N (ORDER BY + LIMIT combined)
	if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &top_n = op->Cast<LogicalTopN>();
		reference<LogicalOperator> child = *op->children[0];

		// Skip projection, filter to find GET
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION ||
		       child.get().type == LogicalOperatorType::LOGICAL_FILTER) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			OptimizeD1ScanLimitPushdown(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "d1_scan") {
			OptimizeD1ScanLimitPushdown(op->children[0]);
			return;
		}

		auto &bind_data = get.bind_data->Cast<D1ScanBindData>();
		bind_data.limit = top_n.limit;
		// Keep TOP_N in plan for ordering - D1 will just return limited rows
	}

	// Handle plain LIMIT (no ORDER BY)
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		reference<LogicalOperator> child = *op->children[0];

		// Skip projection, filter to find GET
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION ||
		       child.get().type == LogicalOperatorType::LOGICAL_FILTER) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			OptimizeD1ScanLimitPushdown(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "d1_scan") {
			OptimizeD1ScanLimitPushdown(op->children[0]);
			return;
		}

		// Only push down constant limits
		if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
			OptimizeD1ScanLimitPushdown(op->children[0]);
			return;
		}

		auto &bind_data = get.bind_data->Cast<D1ScanBindData>();
		bind_data.limit = limit.limit_val.GetConstantValue();

		// Remove the LIMIT node since we've pushed it down
		op = std::move(op->children[0]);
		return;
	}

	// Recurse into children
	for (auto &child : op->children) {
		OptimizeD1ScanLimitPushdown(child);
	}
}

} // namespace duckdb
