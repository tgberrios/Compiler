#include "SyncReporter.h"
#include <chrono>
#include <fstream>
#include <sstream>
#include <sys/statvfs.h>

void SyncReporter::collectPerformanceMetrics(pqxx::connection &pgConn,
                                             SyncStats &stats) {
  try {
    pqxx::work txn(pgConn);

    // Get performance metrics from transfer_metrics table
    std::string query = "SELECT "
                        "AVG(transfer_rate_per_second) as avg_rate,"
                        "SUM(records_transferred) as total_records,"
                        "SUM(bytes_transferred) as total_bytes,"
                        "AVG(avg_latency_ms) as avg_latency,"
                        "MAX(max_latency_ms) as max_latency,"
                        "AVG(p95_latency_ms) as p95_latency "
                        "FROM metadata.transfer_metrics "
                        "WHERE created_at >= NOW() - INTERVAL '1 hour';";

    auto result = txn.exec(query);
    txn.commit();

    if (!result.empty()) {
      auto row = result[0];
      stats.avgTransferRate = row[0].is_null() ? 0.0 : row[0].as<double>();
      stats.totalRecordsTransferred =
          row[1].is_null() ? 0.0 : row[1].as<double>();
      stats.totalBytesTransferred =
          row[2].is_null() ? 0.0 : row[2].as<double>();
      stats.avgLatencyMs = row[3].is_null() ? 0.0 : row[3].as<double>();
      stats.maxLatencyMs = row[4].is_null() ? 0.0 : row[4].as<double>();
      stats.p95LatencyMs = row[5].is_null() ? 0.0 : row[5].as<double>();
    }
  } catch (const std::exception &e) {
    // Silently handle errors to avoid breaking the dashboard
  }
}

void SyncReporter::collectDatabaseHealthMetrics(pqxx::connection &pgConn,
                                                SyncStats &stats) {
  try {
    pqxx::work txn(pgConn);

    // Measure database response time
    auto start = std::chrono::high_resolution_clock::now();
    txn.exec("SELECT 1");
    auto end = std::chrono::high_resolution_clock::now();

    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats.dbResponseTime = duration.count() / 1000.0; // Convert to milliseconds

    // Get connection info
    auto connResult = txn.exec(
        "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'");
    if (!connResult.empty()) {
      stats.activeConnections = connResult[0][0].as<int>();
    }

    stats.totalConnections = 10; // Default value
    txn.commit();
  } catch (const std::exception &e) {
    stats.dbResponseTime = 999.0; // Indicate error
  }
}

void SyncReporter::collectSystemResourceMetrics(SyncStats &stats) {
  stats.cpuUsage = getCpuUsage();
  stats.memoryUsage = getMemoryUsage();
  stats.diskUsage = getDiskUsage();
}

void SyncReporter::collectRecentActivityMetrics(pqxx::connection &pgConn,
                                                SyncStats &stats) {
  try {
    pqxx::work txn(pgConn);

    // Get transfers and errors from last hour
    std::string query =
        "SELECT "
        "COUNT(*) FILTER (WHERE status = 'SUCCESS') as transfers,"
        "COUNT(*) FILTER (WHERE status = 'FAILED' AND error_message NOT LIKE '%Table marked as inactive%') as errors "
        "FROM metadata.transfer_metrics "
        "WHERE created_at >= NOW() - INTERVAL '1 hour';";

    auto result = txn.exec(query);
    if (!result.empty()) {
      auto row = result[0];
      stats.transfersLastHour = row[0].as<int>();
      stats.errorsLastHour = row[1].as<int>();
    }

    // Get last error message
    std::string errorQuery =
        "SELECT error_message FROM metadata.transfer_metrics "
        "WHERE error_message IS NOT NULL AND error_message != '' "
        "ORDER BY created_at DESC LIMIT 1;";

    auto errorResult = txn.exec(errorQuery);
    if (!errorResult.empty()) {
      stats.lastError = errorResult[0][0].as<std::string>();
    }

    stats.uptime = getUptime();
    txn.commit();
  } catch (const std::exception &e) {
    // Silently handle errors
  }
}

std::string SyncReporter::formatBytes(double bytes) {
  const char *units[] = {"B", "KB", "MB", "GB", "TB"};
  int unit = 0;

  while (bytes >= 1024 && unit < 4) {
    bytes /= 1024;
    unit++;
  }

  std::ostringstream oss;
  oss << std::fixed << std::setprecision(1) << bytes << " " << units[unit];
  return oss.str();
}

std::string SyncReporter::formatDuration(double milliseconds) {
  if (milliseconds < 1000) {
    return std::to_string(static_cast<int>(milliseconds)) + "ms";
  } else if (milliseconds < 60000) {
    return std::to_string(static_cast<int>(milliseconds / 1000)) + "s";
  } else {
    return std::to_string(static_cast<int>(milliseconds / 60000)) + "m";
  }
}

std::string SyncReporter::getUptime() {
    auto now = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - startTime);
    
    int hours = static_cast<int>(duration.count() / 3600);
    int minutes = static_cast<int>((duration.count() % 3600) / 60);
    
    return std::to_string(hours) + "h " + std::to_string(minutes) + "m";
}

double SyncReporter::getCpuUsage() {
  std::ifstream statFile("/proc/stat");
  if (!statFile.is_open())
    return 0.0;

  std::string line;
  std::getline(statFile, line);
  statFile.close();

  std::istringstream iss(line);
  std::string cpu;
  long user, nice, system, idle, iowait, irq, softirq, steal;

  iss >> cpu >> user >> nice >> system >> idle >> iowait >> irq >> softirq >>
      steal;

  long totalIdle = idle + iowait;
  long totalNonIdle = user + nice + system + irq + softirq + steal;
  long total = totalIdle + totalNonIdle;

  // Simple calculation - in a real implementation, you'd want to track previous
  // values
  return totalNonIdle * 100.0 / total;
}

double SyncReporter::getMemoryUsage() {
  std::ifstream meminfoFile("/proc/meminfo");
  if (!meminfoFile.is_open())
    return 0.0;

  long totalMem = 0, freeMem = 0, availableMem = 0;
  std::string line;

  while (std::getline(meminfoFile, line)) {
    if (line.find("MemTotal:") == 0) {
      std::istringstream iss(line);
      std::string key, value, unit;
      iss >> key >> value >> unit;
      totalMem = std::stol(value);
    } else if (line.find("MemAvailable:") == 0) {
      std::istringstream iss(line);
      std::string key, value, unit;
      iss >> key >> value >> unit;
      availableMem = std::stol(value);
    }
  }
  meminfoFile.close();

  if (totalMem > 0) {
    return ((totalMem - availableMem) * 100.0) / totalMem;
  }
  return 0.0;
}

double SyncReporter::getDiskUsage() {
  struct statvfs stat;
  if (statvfs(".", &stat) != 0)
    return 0.0;

  unsigned long total = stat.f_blocks * stat.f_frsize;
  unsigned long available = stat.f_bavail * stat.f_frsize;
  unsigned long used = total - available;

  return (used * 100.0) / total;
}

// Definición de variables estáticas
std::string SyncReporter::currentProcessingTable = "";
std::string SyncReporter::lastProcessingTable = "";
std::chrono::system_clock::time_point SyncReporter::startTime = std::chrono::system_clock::now();

void SyncReporter::refreshDebugConfig() { Logger::refreshConfig(); }
