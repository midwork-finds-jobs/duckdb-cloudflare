#include "d1_extension.hpp"
#include <curl/curl.h>
#include <sstream>
#include <stdexcept>

namespace duckdb {

// ========================================
// CURL HELPERS
// ========================================

// Callback for libcurl to write response data
static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	string *response = static_cast<string *>(userp);
	response->append(static_cast<char *>(contents), size * nmemb);
	return size * nmemb;
}

// Simple JSON string escaping
static string EscapeJSON(const string &str) {
	string result;
	result.reserve(str.size() + 10);
	for (char c : str) {
		switch (c) {
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		default:
			result += c;
		}
	}
	return result;
}

// Simple JSON value extraction (for parsing D1 API responses)
static string ExtractJSONString(const string &json, const string &key) {
	string search = "\"" + key + "\":";
	size_t pos = json.find(search);
	if (pos == string::npos) {
		return "";
	}
	pos += search.size();

	// Skip whitespace
	while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) {
		pos++;
	}

	if (pos >= json.size()) {
		return "";
	}

	// Handle null
	if (json.substr(pos, 4) == "null") {
		return "";
	}

	// Handle string value
	if (json[pos] == '"') {
		pos++;
		size_t end = pos;
		while (end < json.size() && json[end] != '"') {
			if (json[end] == '\\' && end + 1 < json.size()) {
				end += 2;
			} else {
				end++;
			}
		}
		return json.substr(pos, end - pos);
	}

	// Handle number or boolean
	size_t end = pos;
	while (end < json.size() && json[end] != ',' && json[end] != '}' && json[end] != ']') {
		end++;
	}
	return json.substr(pos, end - pos);
}

static bool ExtractJSONBool(const string &json, const string &key) {
	string val = ExtractJSONString(json, key);
	return val == "true";
}

static int64_t ExtractJSONInt(const string &json, const string &key) {
	string val = ExtractJSONString(json, key);
	if (val.empty()) {
		return 0;
	}
	try {
		return std::stoll(val);
	} catch (...) {
		return 0;
	}
}

static double ExtractJSONDouble(const string &json, const string &key) {
	string val = ExtractJSONString(json, key);
	if (val.empty()) {
		return 0.0;
	}
	try {
		return std::stod(val);
	} catch (...) {
		return 0.0;
	}
}

// HTTP POST request helper
static string HTTPPost(const string &url, const string &body, const string &api_token) {
	CURL *curl = curl_easy_init();
	if (!curl) {
		throw IOException("Failed to initialize curl");
	}

	string response;

	// Set URL
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

	// Set POST method
	curl_easy_setopt(curl, CURLOPT_POST, 1L);

	// Set request body
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());

	// Set headers
	struct curl_slist *headers = nullptr;
	headers = curl_slist_append(headers, ("Authorization: Bearer " + api_token).c_str());
	headers = curl_slist_append(headers, "Content-Type: application/json");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	// Set write callback
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

	// SSL verification
	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

	// Timeout
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

	// Perform request
	CURLcode res = curl_easy_perform(curl);

	// Check HTTP status
	long http_code = 0;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

	// Cleanup
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	if (res != CURLE_OK) {
		throw IOException("HTTP request failed: " + string(curl_easy_strerror(res)));
	}

	if (http_code < 200 || http_code >= 300) {
		throw IOException("HTTP request failed with status " + to_string(http_code) + ": " + response);
	}

	return response;
}

// HTTP GET request helper
static string HTTPGet(const string &url, const string &api_token) {
	CURL *curl = curl_easy_init();
	if (!curl) {
		throw IOException("Failed to initialize curl");
	}

	string response;

	// Set URL
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

	// Set headers
	struct curl_slist *headers = nullptr;
	headers = curl_slist_append(headers, ("Authorization: Bearer " + api_token).c_str());
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	// Set write callback
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

	// SSL verification
	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

	// Timeout
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

	// Perform request
	CURLcode res = curl_easy_perform(curl);

	// Check HTTP status
	long http_code = 0;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

	// Cleanup
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	if (res != CURLE_OK) {
		throw IOException("HTTP request failed: " + string(curl_easy_strerror(res)));
	}

	if (http_code < 200 || http_code >= 300) {
		throw IOException("HTTP request failed with status " + to_string(http_code) + ": " + response);
	}

	return response;
}

// ========================================
// RESULT PARSING
// ========================================

// Parse a single row from the results array
static unordered_map<string, string> ParseResultRow(const string &row_json, vector<string> &column_order) {
	unordered_map<string, string> row;

	// Simple JSON object parser - find all "key": value pairs
	size_t pos = row_json.find('{');
	if (pos == string::npos) {
		return row;
	}
	pos++;

	while (pos < row_json.size()) {
		// Skip whitespace
		while (pos < row_json.size() && (row_json[pos] == ' ' || row_json[pos] == '\t' || row_json[pos] == '\n')) {
			pos++;
		}

		if (pos >= row_json.size() || row_json[pos] == '}') {
			break;
		}

		// Skip comma
		if (row_json[pos] == ',') {
			pos++;
			continue;
		}

		// Parse key
		if (row_json[pos] != '"') {
			break;
		}
		pos++;
		size_t key_end = row_json.find('"', pos);
		if (key_end == string::npos) {
			break;
		}
		string key = row_json.substr(pos, key_end - pos);
		pos = key_end + 1;

		// Skip : and whitespace
		while (pos < row_json.size() && (row_json[pos] == ':' || row_json[pos] == ' ' || row_json[pos] == '\t')) {
			pos++;
		}

		// Parse value
		string value;
		if (pos < row_json.size()) {
			if (row_json[pos] == '"') {
				// String value
				pos++;
				size_t val_end = pos;
				while (val_end < row_json.size() && row_json[val_end] != '"') {
					if (row_json[val_end] == '\\' && val_end + 1 < row_json.size()) {
						val_end += 2;
					} else {
						val_end++;
					}
				}
				value = row_json.substr(pos, val_end - pos);
				pos = val_end + 1;
			} else if (row_json.substr(pos, 4) == "null") {
				value = "";
				pos += 4;
			} else if (row_json.substr(pos, 4) == "true") {
				value = "1";
				pos += 4;
			} else if (row_json.substr(pos, 5) == "false") {
				value = "0";
				pos += 5;
			} else {
				// Number
				size_t val_end = pos;
				while (val_end < row_json.size() && row_json[val_end] != ',' && row_json[val_end] != '}') {
					val_end++;
				}
				value = row_json.substr(pos, val_end - pos);
				// Trim whitespace
				while (!value.empty() && (value.back() == ' ' || value.back() == '\t' || value.back() == '\n')) {
					value.pop_back();
				}
				pos = val_end;
			}
		}

		// Track column order (first row only)
		if (std::find(column_order.begin(), column_order.end(), key) == column_order.end()) {
			column_order.push_back(key);
		}

		row[key] = value;
	}

	return row;
}

// Parse the D1 API response
static D1QueryResult ParseD1Response(const string &response) {
	D1QueryResult result;

	// Check top-level success
	result.success = ExtractJSONBool(response, "success");

	// Check for errors
	size_t errors_pos = response.find("\"errors\":");
	if (errors_pos != string::npos) {
		size_t arr_start = response.find('[', errors_pos);
		size_t arr_end = response.find(']', arr_start);
		if (arr_start != string::npos && arr_end != string::npos) {
			string errors_arr = response.substr(arr_start, arr_end - arr_start + 1);
			if (errors_arr != "[]") {
				// Extract first error message
				size_t msg_pos = errors_arr.find("\"message\":");
				if (msg_pos != string::npos) {
					result.error = ExtractJSONString(errors_arr.substr(msg_pos), "message");
				}
			}
		}
	}

	if (!result.success && !result.error.empty()) {
		return result;
	}

	// Parse result array - find the inner results array
	size_t results_pos = response.find("\"results\":");
	if (results_pos == string::npos) {
		return result;
	}

	// Find the results array
	size_t arr_start = response.find('[', results_pos);
	if (arr_start == string::npos) {
		return result;
	}

	// Parse each row object in the results array
	size_t pos = arr_start + 1;
	int brace_depth = 0;
	size_t row_start = 0;

	while (pos < response.size()) {
		char c = response[pos];

		if (c == '{') {
			if (brace_depth == 0) {
				row_start = pos;
			}
			brace_depth++;
		} else if (c == '}') {
			brace_depth--;
			if (brace_depth == 0) {
				// Extract and parse this row
				string row_json = response.substr(row_start, pos - row_start + 1);
				auto row = ParseResultRow(row_json, result.column_order);
				if (!row.empty()) {
					result.results.push_back(std::move(row));
				}
			}
		} else if (c == ']' && brace_depth == 0) {
			break;
		}

		pos++;
	}

	// Parse meta information
	size_t meta_pos = response.find("\"meta\":");
	if (meta_pos != string::npos) {
		string meta_section = response.substr(meta_pos);
		result.meta.served_by_primary = ExtractJSONBool(meta_section, "served_by_primary");
		result.meta.served_by_region = ExtractJSONString(meta_section, "served_by_region");
		result.meta.duration_ms = ExtractJSONDouble(meta_section, "duration");
		result.meta.changes = ExtractJSONInt(meta_section, "changes");
		result.meta.last_row_id = ExtractJSONInt(meta_section, "last_row_id");
		result.meta.changed_db = ExtractJSONBool(meta_section, "changed_db");
		result.meta.size_after = ExtractJSONInt(meta_section, "size_after");
		result.meta.rows_read = ExtractJSONInt(meta_section, "rows_read");
		result.meta.rows_written = ExtractJSONInt(meta_section, "rows_written");
	}

	return result;
}

// ========================================
// D1 API FUNCTIONS
// ========================================

D1QueryResult D1ExecuteQuery(const D1Config &config, const string &sql, const vector<string> &params) {
	// Build JSON request body
	string body = "{\"sql\":\"" + EscapeJSON(sql) + "\"";

	if (!params.empty()) {
		body += ",\"params\":[";
		for (size_t i = 0; i < params.size(); i++) {
			if (i > 0) {
				body += ",";
			}
			body += "\"" + EscapeJSON(params[i]) + "\"";
		}
		body += "]";
	}

	body += "}";

	// Execute request
	string response = HTTPPost(config.GetQueryUrl(), body, config.api_token);

	// Parse response
	return ParseD1Response(response);
}

vector<D1DatabaseInfo> D1ListDatabases(const D1Config &config) {
	vector<D1DatabaseInfo> databases;

	string response = HTTPGet(config.GetListDatabasesUrl(), config.api_token);

	// Parse the response - find result array
	size_t result_pos = response.find("\"result\":");
	if (result_pos == string::npos) {
		return databases;
	}

	size_t arr_start = response.find('[', result_pos);
	if (arr_start == string::npos) {
		return databases;
	}

	// Parse each database object
	size_t pos = arr_start + 1;
	int brace_depth = 0;
	size_t obj_start = 0;

	while (pos < response.size()) {
		char c = response[pos];

		if (c == '{') {
			if (brace_depth == 0) {
				obj_start = pos;
			}
			brace_depth++;
		} else if (c == '}') {
			brace_depth--;
			if (brace_depth == 0) {
				string obj = response.substr(obj_start, pos - obj_start + 1);

				D1DatabaseInfo db;
				db.uuid = ExtractJSONString(obj, "uuid");
				db.name = ExtractJSONString(obj, "name");
				db.created_at = ExtractJSONString(obj, "created_at");
				db.version = ExtractJSONString(obj, "version");
				db.file_size = ExtractJSONInt(obj, "file_size");
				db.num_tables = static_cast<int>(ExtractJSONInt(obj, "num_tables"));
				db.region = ExtractJSONString(obj, "created_in_region");

				if (!db.uuid.empty()) {
					databases.push_back(std::move(db));
				}
			}
		} else if (c == ']' && brace_depth == 0) {
			break;
		}

		pos++;
	}

	return databases;
}

string D1GetDatabaseIdByName(const D1Config &config, const string &name) {
	auto databases = D1ListDatabases(config);
	for (const auto &db : databases) {
		if (db.name == name) {
			return db.uuid;
		}
	}
	throw IOException("D1 database not found: " + name);
}

vector<D1TableInfo> D1GetTables(const D1Config &config) {
	vector<D1TableInfo> tables;

	auto result = D1ExecuteQuery(config, "PRAGMA table_list");
	if (!result.success) {
		throw IOException("Failed to get table list: " + result.error);
	}

	for (const auto &row : result.results) {
		D1TableInfo table;

		auto it = row.find("schema");
		if (it != row.end()) {
			table.schema = it->second;
		}

		it = row.find("name");
		if (it != row.end()) {
			table.name = it->second;
		}

		it = row.find("type");
		if (it != row.end()) {
			table.type = it->second;
		}

		it = row.find("ncol");
		if (it != row.end()) {
			try {
				table.ncol = std::stoi(it->second);
			} catch (...) {
			}
		}

		it = row.find("wr");
		if (it != row.end()) {
			table.writable = (it->second == "1");
		}

		it = row.find("strict");
		if (it != row.end()) {
			table.strict = (it->second == "1");
		}

		// Filter out internal tables
		if (table.schema == "main" && !table.name.empty() && table.name[0] != '_' && table.name != "sqlite_schema") {
			tables.push_back(std::move(table));
		}
	}

	return tables;
}

vector<D1ColumnInfo> D1GetTableColumns(const D1Config &config, const string &table_name) {
	vector<D1ColumnInfo> columns;

	auto result = D1ExecuteQuery(config, "PRAGMA table_info(" + table_name + ")");
	if (!result.success) {
		throw IOException("Failed to get table columns: " + result.error);
	}

	for (const auto &row : result.results) {
		D1ColumnInfo col;

		auto it = row.find("cid");
		if (it != row.end()) {
			try {
				col.cid = std::stoi(it->second);
			} catch (...) {
			}
		}

		it = row.find("name");
		if (it != row.end()) {
			col.name = it->second;
		}

		it = row.find("type");
		if (it != row.end()) {
			col.type = it->second;
		}

		it = row.find("notnull");
		if (it != row.end()) {
			col.notnull = (it->second == "1");
		}

		it = row.find("dflt_value");
		if (it != row.end()) {
			col.dflt_value = it->second;
		}

		it = row.find("pk");
		if (it != row.end()) {
			col.pk = (it->second == "1");
		}

		columns.push_back(std::move(col));
	}

	return columns;
}

// ========================================
// TYPE MAPPING
// ========================================

LogicalType SQLiteTypeToDuckDB(const string &sqlite_type) {
	// Convert to uppercase for comparison
	string upper_type = sqlite_type;
	for (char &c : upper_type) {
		c = std::toupper(c);
	}

	// SQLite affinity rules:
	// 1. If contains "INT" -> INTEGER
	// 2. If contains "CHAR", "CLOB", or "TEXT" -> TEXT
	// 3. If contains "BLOB" or empty -> BLOB
	// 4. If contains "REAL", "FLOA", or "DOUB" -> REAL
	// 5. Otherwise -> NUMERIC

	if (upper_type.find("INT") != string::npos) {
		return LogicalType::BIGINT;
	}
	if (upper_type.find("CHAR") != string::npos || upper_type.find("CLOB") != string::npos ||
	    upper_type.find("TEXT") != string::npos) {
		return LogicalType::VARCHAR;
	}
	if (upper_type.find("BLOB") != string::npos || upper_type.empty()) {
		return LogicalType::BLOB;
	}
	if (upper_type.find("REAL") != string::npos || upper_type.find("FLOA") != string::npos ||
	    upper_type.find("DOUB") != string::npos) {
		return LogicalType::DOUBLE;
	}
	if (upper_type.find("BOOL") != string::npos) {
		return LogicalType::BOOLEAN;
	}
	if (upper_type.find("DATE") != string::npos) {
		return LogicalType::DATE;
	}
	if (upper_type.find("TIME") != string::npos) {
		return LogicalType::TIMESTAMP;
	}

	// Default to VARCHAR for flexibility
	return LogicalType::VARCHAR;
}

} // namespace duckdb
