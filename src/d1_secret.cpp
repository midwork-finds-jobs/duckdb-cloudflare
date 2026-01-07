#include "d1_extension.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// ========================================
// D1 SECRET TYPE
// ========================================

// Create a D1 secret from user input
static unique_ptr<BaseSecret> CreateD1SecretFunction(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;

	// Create a KeyValueSecret with type "d1", provider "config"
	auto result = make_uniq<KeyValueSecret>(scope, "d1", "config", input.name);

	// Parse options
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "account_id") {
			result->secret_map["account_id"] = named_param.second.ToString();
		} else if (lower_name == "api_token") {
			result->secret_map["api_token"] = named_param.second.ToString();
		} else {
			throw InvalidInputException("Unknown parameter for D1 secret: '%s'. Expected: account_id, api_token",
			                            lower_name);
		}
	}

	// Validate required fields
	if (result->secret_map.find("account_id") == result->secret_map.end()) {
		throw InvalidInputException("D1 secret requires 'account_id' parameter");
	}
	if (result->secret_map.find("api_token") == result->secret_map.end()) {
		throw InvalidInputException("D1 secret requires 'api_token' parameter");
	}

	// Set keys to redact in logs
	result->redact_keys = {"api_token"};

	return std::move(result);
}

// Set parameters for the create secret function
static void SetD1SecretParameters(CreateSecretFunction &function) {
	function.named_parameters["account_id"] = LogicalType::VARCHAR;
	function.named_parameters["api_token"] = LogicalType::VARCHAR;
}

// Register the D1 secret type
void RegisterD1SecretType(ExtensionLoader &loader) {
	// Define the secret type
	SecretType secret_type;
	secret_type.name = "d1";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	// Register the secret type
	loader.RegisterSecretType(secret_type);

	// Define and register the create secret function
	CreateSecretFunction d1_secret_function = {"d1", "config", CreateD1SecretFunction};
	SetD1SecretParameters(d1_secret_function);
	loader.RegisterFunction(d1_secret_function);
}

// Helper function to get D1 config from a secret
D1Config GetD1ConfigFromSecret(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);

	// Try to find the secret
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto secret_match = secret_manager.LookupSecret(transaction, secret_name, "d1");

	if (!secret_match.HasMatch()) {
		throw InvalidInputException("D1 secret '%s' not found. Create it with: CREATE SECRET %s (TYPE d1, ...)",
		                            secret_name, secret_name);
	}

	auto &secret = secret_match.GetSecret();
	if (secret.GetType() != "d1") {
		throw InvalidInputException("Secret '%s' is not a D1 secret (type is '%s')", secret_name, secret.GetType());
	}

	// Cast to KeyValueSecret
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(secret);

	D1Config config;

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
