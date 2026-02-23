#include "engines/azure_blob_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"
#include <sstream>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <iomanip>
#include <ctime>

AzureBlobEngine::AzureBlobEngine(const std::string &connectionString, const AzureBlobConfig &config)
    : connectionString_(connectionString), config_(config), curl_(nullptr) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl_ = curl_easy_init();
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "AzureBlobEngine",
                  "Failed to initialize CURL");
  }
  parseConnectionString();
}

AzureBlobEngine::~AzureBlobEngine() {
  if (curl_) {
    curl_easy_cleanup(curl_);
  }
  curl_global_cleanup();
}

size_t AzureBlobEngine::WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
  ((std::vector<uint8_t> *)userp)->insert(
      ((std::vector<uint8_t> *)userp)->end(),
      (uint8_t *)contents, (uint8_t *)contents + size * nmemb);
  return size * nmemb;
}

void AzureBlobEngine::parseConnectionString() {
  auto params = ConnectionStringParser::parse(connectionString_);
  if (params) {
    config_.account_name = params->user;
    config_.account_key = params->password;
    config_.container_name = params->db;
    if (!params->host.empty()) {
      config_.endpoint_suffix = params->host;
    }
  }
}

std::string AzureBlobEngine::buildBlobURL(const std::string &blobName) {
  std::ostringstream url;
  
  url << (config_.use_https ? "https://" : "http://");
  url << config_.account_name << ".blob." << config_.endpoint_suffix;
  url << "/" << config_.container_name;
  
  if (!blobName.empty() && blobName[0] != '/') {
    url << "/";
  }
  url << blobName;
  
  return url.str();
}

std::string AzureBlobEngine::generateAzureSignature(const std::string &method, const std::string &url,
                                                    const std::string &date, const std::string &contentLength) {
  // Simplified Azure Storage signature generation
  // For production, use proper Azure Storage REST API authentication
  // Extract path from URL
  size_t protocolPos = url.find("://");
  size_t pathStart = (protocolPos != std::string::npos)
                         ? url.find("/", protocolPos + 3)
                         : std::string::npos;
  std::string canonicalizedResource = pathStart != std::string::npos ? url.substr(pathStart) : "/";
  
  std::string stringToSign = method + "\n\n\n" + date + "\n/" + config_.account_name + canonicalizedResource;
  
  unsigned char hmac[SHA256_DIGEST_LENGTH];
  unsigned int hmacLen;
  HMAC(EVP_sha256(), config_.account_key.c_str(), config_.account_key.length(),
       (unsigned char *)stringToSign.c_str(), stringToSign.length(), hmac, &hmacLen);
  
  std::ostringstream signature;
  for (unsigned int i = 0; i < hmacLen; i++) {
    signature << std::hex << std::setw(2) << std::setfill('0') << (int)hmac[i];
  }
  
  return "SharedKey " + config_.account_name + ":" + signature.str();
}

std::vector<uint8_t> AzureBlobEngine::downloadBlob(const std::string &blobName) {
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "AzureBlobEngine",
                  "CURL not initialized");
    return {};
  }

  std::vector<uint8_t> data;
  std::string url = buildBlobURL(blobName);
  
  // Get current date in RFC 1123 format
  std::time_t now = std::time(nullptr);
  char dateStr[100];
  std::strftime(dateStr, sizeof(dateStr), "%a, %d %b %Y %H:%M:%S GMT", std::gmtime(&now));
  
  std::string signature = generateAzureSignature("GET", url, dateStr);
  
  curl_easy_reset(curl_);
  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &data);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 2L);
  
  struct curl_slist *headers = nullptr;
  std::string dateHeader = "x-ms-date: " + std::string(dateStr);
  std::string versionHeader = "x-ms-version: 2021-04-10";
  std::string authHeader = "Authorization: " + signature;
  headers = curl_slist_append(headers, dateHeader.c_str());
  headers = curl_slist_append(headers, versionHeader.c_str());
  headers = curl_slist_append(headers, authHeader.c_str());
  curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);
  
  CURLcode res = curl_easy_perform(curl_);
  curl_slist_free_all(headers);
  
  if (res != CURLE_OK) {
    Logger::error(LogCategory::DATABASE, "AzureBlobEngine",
                  "Failed to download blob: " + std::string(curl_easy_strerror(res)));
    return {};
  }
  
  long responseCode;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &responseCode);
  if (responseCode != 200) {
    Logger::error(LogCategory::DATABASE, "AzureBlobEngine",
                  "HTTP error: " + std::to_string(responseCode));
    return {};
  }
  
  return data;
}

std::vector<std::string> AzureBlobEngine::listBlobs(const std::string &prefix) {
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "AzureBlobEngine",
                  "CURL not initialized");
    return {};
  }

  std::vector<std::string> blobNames;
  std::ostringstream url;
  url << (config_.use_https ? "https://" : "http://");
  url << config_.account_name << ".blob." << config_.endpoint_suffix;
  url << "/" << config_.container_name << "?restype=container&comp=list";
  if (!prefix.empty()) {
    url << "&prefix=" << prefix;
  }
  
  std::string urlStr = url.str();
  std::time_t now = std::time(nullptr);
  char dateStr[100];
  std::strftime(dateStr, sizeof(dateStr), "%a, %d %b %Y %H:%M:%S GMT", std::gmtime(&now));
  
  std::string signature = generateAzureSignature("GET", urlStr, dateStr);
  
  std::string responseBody;
  curl_easy_reset(curl_);
  curl_easy_setopt(curl_, CURLOPT_URL, urlStr.c_str());
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, 
                   [](void *contents, size_t size, size_t nmemb, void *userp) -> size_t {
                     ((std::string *)userp)->append((char *)contents, size * nmemb);
                     return size * nmemb;
                   });
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 2L);
  
  struct curl_slist *headers = nullptr;
  std::string dateHeader2 = "x-ms-date: " + std::string(dateStr);
  std::string versionHeader2 = "x-ms-version: 2021-04-10";
  std::string authHeader2 = "Authorization: " + signature;
  headers = curl_slist_append(headers, dateHeader2.c_str());
  headers = curl_slist_append(headers, versionHeader2.c_str());
  headers = curl_slist_append(headers, authHeader2.c_str());
  curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);
  
  CURLcode res = curl_easy_perform(curl_);
  curl_slist_free_all(headers);
  
  if (res == CURLE_OK) {
    // Parse XML response to extract blob names
    // Simplified: look for <Name> tags
    size_t pos = 0;
    while ((pos = responseBody.find("<Name>", pos)) != std::string::npos) {
      size_t start = pos + 6;
      size_t end = responseBody.find("</Name>", start);
      if (end != std::string::npos) {
        blobNames.push_back(responseBody.substr(start, end - start));
        pos = end + 7;
      } else {
        break;
      }
    }
  }
  
  return blobNames;
}

std::vector<uint8_t> AzureBlobEngine::getBlob(const std::string &blobName) {
  return downloadBlob(blobName);
}

std::vector<std::string> AzureBlobEngine::listContainer(const std::string &prefix) {
  return listBlobs(prefix);
}

std::vector<json> AzureBlobEngine::parseBlobAsJSON(const std::string &blobName) {
  std::vector<uint8_t> data = getBlob(blobName);
  if (data.empty()) {
    return {};
  }
  
  try {
    std::string jsonStr(data.begin(), data.end());
    json parsed = json::parse(jsonStr);
    
    if (parsed.is_array()) {
      std::vector<json> result;
      for (const auto &item : parsed) {
        result.push_back(item);
      }
      return result;
    } else {
      return {parsed};
    }
  } catch (const json::parse_error &e) {
    Logger::error(LogCategory::DATABASE, "AzureBlobEngine",
                  "Failed to parse JSON from Azure Blob: " + std::string(e.what()));
    return {};
  }
}

void AzureBlobEngine::setConfig(const AzureBlobConfig &config) {
  config_ = config;
}
