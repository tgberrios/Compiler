#include "utils/HostnamePatternMatcher.h"
#include "utils/string_utils.h"
#include <cstring>
#include <iterator>

std::string
HostnamePatternMatcher::deriveClusterName(const std::string &hostname) {
  if (hostname.empty())
    return "";

  std::string lower = StringUtils::toLower(hostname);

  static constexpr const char *PROD_PATTERNS[] = {"prod", "production"};
  static constexpr const char *STAGING_PATTERNS[] = {"staging", "stage"};
  static constexpr const char *DEV_PATTERNS[] = {"dev", "development"};
  static constexpr const char *TEST_PATTERNS[] = {"test", "testing"};
  static constexpr const char *LOCAL_PATTERNS[] = {"local", "localhost"};

  if (matchesPattern(lower, PROD_PATTERNS, std::size(PROD_PATTERNS)))
    return "PRODUCTION";
  if (matchesPattern(lower, STAGING_PATTERNS, std::size(STAGING_PATTERNS)))
    return "STAGING";
  if (matchesPattern(lower, DEV_PATTERNS, std::size(DEV_PATTERNS)))
    return "DEVELOPMENT";
  if (matchesPattern(lower, TEST_PATTERNS, std::size(TEST_PATTERNS)))
    return "TESTING";
  if (matchesPattern(lower, LOCAL_PATTERNS, std::size(LOCAL_PATTERNS)))
    return "LOCAL";

  if (lower.find("uat") != std::string::npos)
    return "UAT";
  if (lower.find("qa") != std::string::npos)
    return "QA";

  size_t pos = lower.find("cluster");
  if (pos != std::string::npos) {
    return StringUtils::toUpper(lower.substr(pos));
  }

  pos = lower.find("db-");
  if (pos != std::string::npos) {
    return StringUtils::toUpper(lower.substr(pos));
  }

  return StringUtils::toUpper(hostname);
}

bool HostnamePatternMatcher::matchesPattern(const std::string &hostname,
                                            const char *const *patterns,
                                            size_t count) {
  for (size_t i = 0; i < count; ++i) {
    size_t pos = hostname.find(patterns[i]);
    if (pos != std::string::npos) {
      if (pos == 0 || hostname[pos - 1] == '-' || hostname[pos - 1] == '_' ||
          hostname[pos - 1] == '.') {
        size_t patternLen = strlen(patterns[i]);
        if (pos + patternLen == hostname.length() ||
            hostname[pos + patternLen] == '-' ||
            hostname[pos + patternLen] == '_' ||
            hostname[pos + patternLen] == '.') {
          return true;
        }
      }
    }
  }
  return false;
}
