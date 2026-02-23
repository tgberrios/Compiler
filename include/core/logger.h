#ifndef LOGGER_H
#define LOGGER_H

#include "core/database_log_writer.h"
#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>

enum class LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARNING = 2,
  ERROR = 3,
  CRITICAL = 4
};

enum class LogCategory {
  SYSTEM = 0,
  DATABASE = 1,
  TRANSFER = 2,
  CONFIG = 3,
  VALIDATION = 4,
  MAINTENANCE = 5,
  MONITORING = 6,
  DDL_EXPORT = 7,
  METRICS = 8,
  GOVERNANCE = 9,
  QUALITY = 10,
  TRANSFORM = 11,
  UNKNOWN = 99
};

class Logger {
private:
  static std::unique_ptr<DatabaseLogWriter> dbWriter_;
  static std::mutex logMutex;

  static LogLevel currentLogLevel;
  static bool showTimestamps;
  static bool showThreadId;
  static bool showFileLine;
  static std::mutex configMutex;

  // Deduplication: suppress identical log (level+category+function+message)
  // within this window to avoid spam.
  static std::unordered_map<std::string, std::chrono::steady_clock::time_point>
      dedupMap_;
  static constexpr std::chrono::seconds dedupWindow{60};
  static constexpr size_t dedupMaxSize{256};

  static const std::unordered_map<std::string, LogCategory> categoryMap;
  static const std::unordered_map<std::string, LogLevel> levelMap;

  static std::string formatLogMessage(const std::string &timestamp,
                                      const std::string &levelStr,
                                      const std::string &categoryStr,
                                      const std::string &function,
                                      const std::string &message) {
    std::ostringstream oss;
    oss << "[" << timestamp << "] [" << levelStr << "] [" << categoryStr << "]";
    if (!function.empty()) {
      oss << " [" << function << "]";
    }
    oss << " " << message;
    return oss.str();
  }

  static std::string getCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    struct tm tm_buf;
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) %
              1000;

    std::stringstream ss;
    localtime_r(&time_t, &tm_buf);
    ss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
    ss << "." << std::setfill('0') << std::setw(3) << ms.count();
    return ss.str();
  }

  static std::string getLevelString(LogLevel level) {
    switch (level) {
    case LogLevel::DEBUG:
      return "DEBUG";
    case LogLevel::INFO:
      return "INFO";
    case LogLevel::WARNING:
      return "WARNING";
    case LogLevel::ERROR:
      return "ERROR";
    case LogLevel::CRITICAL:
      return "CRITICAL";
    default:
      return "UNKNOWN";
    }
  }

  static std::string getCategoryString(LogCategory category) {
    switch (category) {
    case LogCategory::SYSTEM:
      return "SYSTEM";
    case LogCategory::DATABASE:
      return "DATABASE";
    case LogCategory::TRANSFER:
      return "TRANSFER";
    case LogCategory::CONFIG:
      return "CONFIG";
    case LogCategory::VALIDATION:
      return "VALIDATION";
    case LogCategory::MAINTENANCE:
      return "MAINTENANCE";
    case LogCategory::MONITORING:
      return "MONITORING";
    case LogCategory::DDL_EXPORT:
      return "DDL_EXPORT";
    case LogCategory::METRICS:
      return "METRICS";
    case LogCategory::GOVERNANCE:
      return "GOVERNANCE";
    case LogCategory::QUALITY:
      return "QUALITY";
    case LogCategory::TRANSFORM:
      return "TRANSFORM";
    default:
      return "UNKNOWN";
    }
  }

  static LogCategory stringToCategory(const std::string &categoryStr) {
    auto it = categoryMap.find(categoryStr);
    return (it != categoryMap.end()) ? it->second : LogCategory::UNKNOWN;
  }

  static void writeLog(LogLevel level, LogCategory category,
                       const std::string &function,
                       const std::string &message) {
    LogLevel minLevel;
    {
      std::lock_guard<std::mutex> configLock(configMutex);
      minLevel = currentLogLevel;
    }

    if (level < minLevel) {
      return;
    }

    std::string levelStr = getLevelString(level);
    std::string categoryStr = getCategoryString(category);
    std::string key = levelStr + "|" + categoryStr + "|" + function + "|" +
                      message;

    DatabaseLogWriter *writer = nullptr;
    {
      std::lock_guard<std::mutex> lock(logMutex);
      auto now = std::chrono::steady_clock::now();
      auto it = dedupMap_.find(key);
      if (it != dedupMap_.end() &&
          (now - it->second) < dedupWindow) {
        return;
      }
      if (dedupMap_.size() >= dedupMaxSize) {
        for (auto e = dedupMap_.begin(); e != dedupMap_.end();) {
          if (now - e->second > dedupWindow) {
            e = dedupMap_.erase(e);
          } else {
            ++e;
          }
        }
      }
      dedupMap_[key] = now;
      if (dbWriter_ && dbWriter_->isEnabled() && dbWriter_->isOpen()) {
        writer = dbWriter_.get();
      }
    }

    std::string timestamp = getCurrentTimestamp();
    if (writer) {
      writer->writeParsed(levelStr, categoryStr, function, message);
    } else {
      std::cerr << formatLogMessage(timestamp, levelStr, categoryStr,
                                    function, message)
                << std::endl;
    }
  }

  static LogLevel stringToLogLevel(const std::string &levelStr) {
    auto it = levelMap.find(levelStr);
    return (it != levelMap.end()) ? it->second : LogLevel::INFO;
  }

public:
  static void initialize();

  static void shutdown() {
    std::lock_guard<std::mutex> lock(logMutex);
    if (dbWriter_) {
      dbWriter_->close();
    }
    dbWriter_.reset();
  }

  // Convenience methods with categories
  static void debug(const std::string &message) {
    writeLog(LogLevel::DEBUG, LogCategory::SYSTEM, "", message);
  }

  static void info(const std::string &message) {
    writeLog(LogLevel::INFO, LogCategory::SYSTEM, "", message);
  }

  static void warning(const std::string &message) {
    writeLog(LogLevel::WARNING, LogCategory::SYSTEM, "", message);
  }

  static void error(const std::string &message) {
    writeLog(LogLevel::ERROR, LogCategory::SYSTEM, "", message);
  }

  static void critical(const std::string &message) {
    writeLog(LogLevel::CRITICAL, LogCategory::SYSTEM, "", message);
  }

  static void debug(const std::string &function, const std::string &message) {
    writeLog(LogLevel::DEBUG, LogCategory::SYSTEM, function, message);
  }

  static void info(const std::string &function, const std::string &message) {
    writeLog(LogLevel::INFO, LogCategory::SYSTEM, function, message);
  }

  static void warning(const std::string &function, const std::string &message) {
    writeLog(LogLevel::WARNING, LogCategory::SYSTEM, function, message);
  }

  static void error(const std::string &function, const std::string &message) {
    writeLog(LogLevel::ERROR, LogCategory::SYSTEM, function, message);
  }

  static void critical(const std::string &function,
                       const std::string &message) {
    writeLog(LogLevel::CRITICAL, LogCategory::SYSTEM, function, message);
  }

  // Categorized logging methods
  static void debug(LogCategory category, const std::string &message) {
    writeLog(LogLevel::DEBUG, category, "", message);
  }

  static void info(LogCategory category, const std::string &message) {
    writeLog(LogLevel::INFO, category, "", message);
  }

  static void warning(LogCategory category, const std::string &message) {
    writeLog(LogLevel::WARNING, category, "", message);
  }

  static void error(LogCategory category, const std::string &message) {
    writeLog(LogLevel::ERROR, category, "", message);
  }

  static void critical(LogCategory category, const std::string &message) {
    writeLog(LogLevel::CRITICAL, category, "", message);
  }

  static void debug(LogCategory category, const std::string &function,
                    const std::string &message) {
    writeLog(LogLevel::DEBUG, category, function, message);
  }

  static void info(LogCategory category, const std::string &function,
                   const std::string &message) {
    writeLog(LogLevel::INFO, category, function, message);
  }

  static void warning(LogCategory category, const std::string &function,
                      const std::string &message) {
    writeLog(LogLevel::WARNING, category, function, message);
  }

  static void error(LogCategory category, const std::string &function,
                    const std::string &message) {
    writeLog(LogLevel::ERROR, category, function, message);
  }

  static void critical(LogCategory category, const std::string &function,
                       const std::string &message) {
    writeLog(LogLevel::CRITICAL, category, function, message);
  }

  // Generic logging methods
  static void log(LogLevel level, LogCategory category,
                  const std::string &message) {
    writeLog(level, category, "", message);
  }

  static void log(LogLevel level, LogCategory category,
                  const std::string &function, const std::string &message) {
    writeLog(level, category, function, message);
  }

  // Configuration management
  static void loadDebugConfig();
  static void setDefaultConfig();
  static void setLogLevel(LogLevel level);
  static void setLogLevel(const std::string &levelStr);
  static LogLevel getCurrentLogLevel();
  static void refreshConfig();
};

// Declaración de variables estáticas (definidas en logger.cpp)

#endif
