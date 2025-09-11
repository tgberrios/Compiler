#include "logger.h"
#include "Config.h"

// Definición de variables estáticas
std::ofstream Logger::logFile;
std::mutex Logger::logMutex;
std::string Logger::logFileName = "DataSync.log";
size_t Logger::messageCount = 0;

// Debug configuration variables
LogLevel Logger::currentLogLevel = LogLevel::INFO;
bool Logger::showTimestamps = true;
bool Logger::showThreadId = false;
bool Logger::showFileLine = false;
std::mutex Logger::configMutex;

void Logger::loadDebugConfig() {
  std::lock_guard<std::mutex> lock(configMutex);

  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    // Load debug level
    auto result =
        txn.exec("SELECT value FROM metadata.config WHERE key = 'debug_level'");
    if (!result.empty()) {
      std::string levelStr = result[0][0].as<std::string>();
      currentLogLevel = stringToLogLevel(levelStr);
    }

    // Load timestamp setting
    result = txn.exec("SELECT value FROM metadata.config WHERE key = "
                      "'debug_show_timestamps'");
    if (!result.empty()) {
      showTimestamps = (result[0][0].as<std::string>() == "true");
    }

    // Load thread ID setting
    result = txn.exec(
        "SELECT value FROM metadata.config WHERE key = 'debug_show_thread_id'");
    if (!result.empty()) {
      showThreadId = (result[0][0].as<std::string>() == "true");
    }

    // Load file/line setting
    result = txn.exec(
        "SELECT value FROM metadata.config WHERE key = 'debug_show_file_line'");
    if (!result.empty()) {
      showFileLine = (result[0][0].as<std::string>() == "true");
    }

    txn.commit();
  } catch (const std::exception &e) {
    // If database is not available, use default values
    currentLogLevel = LogLevel::INFO;
    showTimestamps = true;
    showThreadId = false;
    showFileLine = false;
  }
}

void Logger::setLogLevel(LogLevel level) {
  std::lock_guard<std::mutex> lock(configMutex);
  currentLogLevel = level;
}

void Logger::setLogLevel(const std::string &levelStr) {
  setLogLevel(stringToLogLevel(levelStr));
}

LogLevel Logger::getCurrentLogLevel() {
  std::lock_guard<std::mutex> lock(configMutex);
  return currentLogLevel;
}

void Logger::refreshConfig() { loadDebugConfig(); }

// Update the initialize function to load debug config
void Logger::initialize(const std::string &fileName) {
  std::lock_guard<std::mutex> lock(logMutex);

  // Obtener la ruta del ejecutable y usar el directorio padre como raíz del
  // proyecto
  std::string executablePath = std::filesystem::current_path().string();
  logFileName = executablePath + "/" + fileName;

  logFile.open(logFileName, std::ios::app);
  messageCount = 0;

  // Load debug configuration from database
  loadDebugConfig();
}
