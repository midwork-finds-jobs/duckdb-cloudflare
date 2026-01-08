#include "duckdb.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "r2_extension.hpp"

namespace duckdb {

// R2 SQL Secret Type
static unique_ptr<BaseSecret> CreateR2SQLSecret(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope.push_back("r2-sql://");
	}

	// Create key-value secret
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Required parameters
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "account_id") {
			secret->secret_map["account_id"] = named_param.second.ToString();
		} else if (lower_name == "api_token") {
			secret->secret_map["api_token"] = named_param.second.ToString();
		} else {
			throw InvalidInputException("Unknown parameter for R2 SQL secret: %s", named_param.first);
		}
	}

	// Validate required fields
	if (secret->secret_map.find("account_id") == secret->secret_map.end()) {
		throw InvalidInputException("R2 SQL secret requires ACCOUNT_ID parameter");
	}
	if (secret->secret_map.find("api_token") == secret->secret_map.end()) {
		throw InvalidInputException("R2 SQL secret requires API_TOKEN parameter");
	}

	// Redact sensitive fields
	secret->redact_keys = {"api_token"};

	return std::move(secret);
}

void RegisterR2SQLSecretType(ExtensionLoader &loader) {
	SecretType secret_type;
	secret_type.name = "r2_sql";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	// Register the secret function
	CreateSecretFunction r2_sql_function = {secret_type.name, "config", CreateR2SQLSecret};
	r2_sql_function.named_parameters["account_id"] = LogicalType::VARCHAR;
	r2_sql_function.named_parameters["api_token"] = LogicalType::VARCHAR;
	loader.RegisterFunction(r2_sql_function);
}

// Helper to get R2 SQL config from secret
R2SQLConfig GetR2SQLConfigFromSecret(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);

	// Try to find the secret
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto secret_match = secret_manager.LookupSecret(transaction, secret_name, "r2_sql");

	if (!secret_match.HasMatch()) {
		throw InvalidInputException("R2 SQL secret '%s' not found. Create it with: CREATE SECRET %s (TYPE r2_sql, ...)",
		                            secret_name, secret_name);
	}

	auto &secret = secret_match.GetSecret();
	if (secret.GetType() != "r2_sql") {
		throw InvalidInputException("Secret '%s' is not an R2 SQL secret (type is '%s')", secret_name,
		                            secret.GetType());
	}

	// Cast to KeyValueSecret
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(secret);

	R2SQLConfig config;

	auto account_it = kv_secret.secret_map.find("account_id");
	if (account_it != kv_secret.secret_map.end()) {
		config.account_id = account_it->second.ToString();
	}

	auto token_it = kv_secret.secret_map.find("api_token");
	if (token_it != kv_secret.secret_map.end()) {
		config.api_token = token_it->second.ToString();
	}

	return config;
}

} // namespace duckdb
