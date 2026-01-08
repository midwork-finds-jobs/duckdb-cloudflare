#include "r2_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <curl/curl.h>
#include <sstream>

namespace duckdb {

// Helper for CURL write callback
static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	((string *)userp)->append((char *)contents, size * nmemb);
	return size * nmemb;
}

// Escape JSON string
static string EscapeJSON(const string &input) {
	string result;
	result.reserve(input.size());
	for (char c : input) {
		switch (c) {
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
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

// HTTP POST to R2 SQL API
static string HTTPPost(const string &url, const string &body, const string &api_token) {
	CURL *curl = curl_easy_init();
	if (!curl) {
		throw IOException("Failed to initialize CURL");
	}

	string response;
	struct curl_slist *headers = nullptr;
	headers = curl_slist_append(headers, "Content-Type: application/json");
	string auth_header = "Authorization: Bearer " + api_token;
	headers = curl_slist_append(headers, auth_header.c_str());

	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);

	CURLcode res = curl_easy_perform(curl);

	long http_code = 0;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	if (res != CURLE_OK) {
		throw IOException("HTTP request failed: %s", curl_easy_strerror(res));
	}

	if (http_code < 200 || http_code >= 300) {
		throw IOException("HTTP request failed with status %d: %s", http_code, response.c_str());
	}

	return response;
}

// Parse R2 SQL JSON response
R2SQLQueryResult ParseR2SQLResponse(const string &response) {
	R2SQLQueryResult result;

	// Simple JSON parser for R2 SQL response
	// Response format: {"results": [...], "metadata": {...}}

	// Find "results" array
	auto results_pos = response.find("\"results\"");
	if (results_pos == string::npos) {
		result.success = false;
		result.error = "Invalid response format: missing 'results' field";
		return result;
	}

	// Find opening bracket of results array
	auto array_start = response.find("[", results_pos);
	if (array_start == string::npos) {
		result.success = false;
		result.error = "Invalid response format: malformed results array";
		return result;
	}

	// For now, we'll store the raw JSON response
	// Full parsing can be implemented later
	result.success = true;
	result.raw_response = response;

	return result;
}

// Execute R2 SQL query
R2SQLQueryResult R2SQLQuery(const R2SQLConfig &config, const string &sql) {
	// Build JSON request body
	string body = "{\"query\":\"" + EscapeJSON(sql) + "\"}";

	try {
		string response = HTTPPost(config.GetQueryUrl(), body, config.api_token);
		return ParseR2SQLResponse(response);
	} catch (const Exception &e) {
		R2SQLQueryResult result;
		result.success = false;
		result.error = e.what();
		return result;
	}
}

// List databases/namespaces
R2SQLQueryResult R2SQLListDatabases(const R2SQLConfig &config) {
	return R2SQLQuery(config, "SHOW DATABASES");
}

// List tables in namespace
R2SQLQueryResult R2SQLListTables(const R2SQLConfig &config, const string &namespace_name) {
	string sql = "SHOW TABLES";
	if (!namespace_name.empty()) {
		sql += " IN " + namespace_name;
	}
	return R2SQLQuery(config, sql);
}

// Describe table
R2SQLQueryResult R2SQLDescribeTable(const R2SQLConfig &config, const string &table_name) {
	return R2SQLQuery(config, "DESCRIBE " + table_name);
}

} // namespace duckdb
