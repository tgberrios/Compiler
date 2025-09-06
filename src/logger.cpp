#include "logger.h"

// Definición de variables estáticas
std::ofstream Logger::logFile;
std::mutex Logger::logMutex;
std::string Logger::logFileName = "DataSync.log";
size_t Logger::messageCount = 0;
