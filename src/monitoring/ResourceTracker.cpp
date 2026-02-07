#include "monitoring/ResourceTracker.h"
#include "core/database_config.h"
#include <cstdint>
#include <fstream>
#include <pqxx/pqxx>
#include <sstream>
#include <thread>
#include <chrono>
#include <sys/sysinfo.h>

ResourceTracker::ResourceTracker(const std::string& connectionString)
    : connectionString_(connectionString) {
  // Crear tablas si no existen
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.resource_utilization ("
        "id SERIAL PRIMARY KEY,"
        "cpu_percent DECIMAL(5,2),"
        "cpu_per_core JSONB,"
        "memory_used_mb DECIMAL(10,2),"
        "memory_total_mb DECIMAL(10,2),"
        "memory_percent DECIMAL(5,2),"
        "io_read_ops BIGINT,"
        "io_write_ops BIGINT,"
        "io_read_mbps DECIMAL(10,2),"
        "io_write_mbps DECIMAL(10,2),"
        "network_bytes_in BIGINT,"
        "network_bytes_out BIGINT,"
        "network_mbps_in DECIMAL(10,2),"
        "network_mbps_out DECIMAL(10,2),"
        "db_connections INTEGER,"
        "db_locks INTEGER,"
        "db_cache_hit_ratio DECIMAL(5,2),"
        "workflow_id VARCHAR(255),"
        "job_id VARCHAR(255),"
        "timestamp TIMESTAMP DEFAULT NOW()"
        ")");

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.resource_predictions ("
        "id SERIAL PRIMARY KEY,"
        "resource_type VARCHAR(50) NOT NULL,"
        "current_usage DECIMAL(10,2),"
        "predicted_usage DECIMAL(10,2),"
        "predicted_at TIMESTAMP DEFAULT NOW(),"
        "days_until_exhaustion INTEGER,"
        "confidence DECIMAL(5,2),"
        "metadata JSONB DEFAULT '{}'::jsonb"
        ")");

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "ResourceTracker",
                  "Error creating tables: " + std::string(e.what()));
  }
}

ResourceTracker::ResourceMetrics ResourceTracker::collectCurrentMetrics() {
  ResourceMetrics metrics;
  metrics.timestamp = std::chrono::system_clock::now();

  metrics.cpuPercent = getCpuPercent();
  metrics.cpuPerCore = getCpuPerCore();
  metrics.memoryUsedMB = getMemoryUsedMB();
  metrics.memoryTotalMB = getMemoryTotalMB();
  metrics.memoryPercent = (metrics.memoryUsedMB / metrics.memoryTotalMB) * 100.0;
  metrics.ioReadOps = getIoReadOps();
  metrics.ioWriteOps = getIoWriteOps();
  metrics.ioReadMBps = getIoReadMBps();
  metrics.ioWriteMBps = getIoWriteMBps();
  metrics.networkBytesIn = getNetworkBytesIn();
  metrics.networkBytesOut = getNetworkBytesOut();
  metrics.dbConnections = getDbConnections();
  metrics.dbLocks = getDbLocks();
  metrics.dbCacheHitRatio = getDbCacheHitRatio();

  return metrics;
}

double ResourceTracker::getCpuPercent() {
  // Real CPU usage 0-100% from /proc/stat (delta of idle vs total over two samples).
  // Load average (load * 100) is not a percentage and can exceed 100; /proc/stat gives correct %.
  static bool havePrevious = false;
  static uint64_t prevTotal = 0;
  static uint64_t prevIdle = 0;

  std::ifstream stat("/proc/stat");
  if (!stat.is_open()) return 0.0;

  std::string line;
  if (!std::getline(stat, line) || line.compare(0, 4, "cpu ") != 0) return 0.0;

  uint64_t user, nice, system, idle, iowait, irq, softirq, steal;
  std::istringstream iss(line.substr(4));
  iss >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;
  uint64_t idleTotal = idle + iowait;
  uint64_t total = user + nice + system + idle + iowait + irq + softirq + steal;

  if (!havePrevious) {
    havePrevious = true;
    prevTotal = total;
    prevIdle = idleTotal;
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    return getCpuPercent(); // Second sample for first valid delta
  }

  uint64_t deltaTotal = total - prevTotal;
  uint64_t deltaIdle = idleTotal - prevIdle;
  prevTotal = total;
  prevIdle = idleTotal;

  if (deltaTotal == 0) return 0.0;
  double percent = 100.0 * (1.0 - static_cast<double>(deltaIdle) / static_cast<double>(deltaTotal));
  if (percent < 0.0) percent = 0.0;
  if (percent > 100.0) percent = 100.0;
  return percent;
}

std::vector<double> ResourceTracker::getCpuPerCore() {
  // Simplified: return single value for now
  return {getCpuPercent()};
}

double ResourceTracker::getMemoryUsedMB() {
  struct sysinfo info;
  if (sysinfo(&info) == 0) {
    unsigned long total = info.totalram * info.mem_unit;
    unsigned long free = info.freeram * info.mem_unit;
    return (total - free) / (1024.0 * 1024.0);
  }
  return 0.0;
}

double ResourceTracker::getMemoryTotalMB() {
  struct sysinfo info;
  if (sysinfo(&info) == 0) {
    return (info.totalram * info.mem_unit) / (1024.0 * 1024.0);
  }
  return 0.0;
}

int64_t ResourceTracker::getIoReadOps() { return 0; } // TODO: Implement
int64_t ResourceTracker::getIoWriteOps() { return 0; } // TODO: Implement
double ResourceTracker::getIoReadMBps() { return 0.0; } // TODO: Implement
double ResourceTracker::getIoWriteMBps() { return 0.0; } // TODO: Implement
int64_t ResourceTracker::getNetworkBytesIn() { return 0; } // TODO: Implement
int64_t ResourceTracker::getNetworkBytesOut() { return 0; } // TODO: Implement

int ResourceTracker::getDbConnections() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);
    auto result = txn.exec("SELECT count(*) FROM pg_stat_activity");
    return result[0][0].as<int>();
  } catch (...) {
    return 0;
  }
}

int ResourceTracker::getDbLocks() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);
    auto result = txn.exec("SELECT count(*) FROM pg_locks WHERE granted = false");
    return result[0][0].as<int>();
  } catch (...) {
    return 0;
  }
}

double ResourceTracker::getDbCacheHitRatio() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);
    auto result = txn.exec(
        "SELECT sum(heap_blks_hit)::float / NULLIF(sum(heap_blks_hit) + "
        "sum(heap_blks_read), 0) * 100 as ratio FROM pg_statio_user_tables");
    if (!result.empty() && !result[0][0].is_null()) {
      return result[0][0].as<double>();
    }
  } catch (...) {
  }
  return 0.0;
}

bool ResourceTracker::saveMetrics(const ResourceMetrics& metrics) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    json cpuPerCoreJson = json::array();
    for (double cpu : metrics.cpuPerCore) {
      cpuPerCoreJson.push_back(cpu);
    }

    txn.exec_params(
        "INSERT INTO metadata.resource_utilization "
        "(cpu_percent, cpu_per_core, memory_used_mb, memory_total_mb, memory_percent, "
        "io_read_ops, io_write_ops, io_read_mbps, io_write_mbps, network_bytes_in, "
        "network_bytes_out, db_connections, db_locks, db_cache_hit_ratio, workflow_id, job_id) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
        metrics.cpuPercent, cpuPerCoreJson.dump(), metrics.memoryUsedMB, metrics.memoryTotalMB,
        metrics.memoryPercent, metrics.ioReadOps, metrics.ioWriteOps, metrics.ioReadMBps,
        metrics.ioWriteMBps, metrics.networkBytesIn, metrics.networkBytesOut,
        metrics.dbConnections, metrics.dbLocks, metrics.dbCacheHitRatio,
        metrics.workflowId.empty() ? nullptr : metrics.workflowId,
        metrics.jobId.empty() ? nullptr : metrics.jobId);

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "ResourceTracker",
                  "Error saving metrics: " + std::string(e.what()));
    return false;
  }
}

std::vector<ResourceTracker::ResourceMetrics> ResourceTracker::getHistory(
    const std::string& workflowId, const std::chrono::hours& duration) {
  std::vector<ResourceMetrics> metrics;
  // TODO: Implement
  return metrics;
}

std::vector<ResourceTracker::ResourceMetrics> ResourceTracker::getMetricsByWorkflow(
    const std::string& workflowId) {
  std::vector<ResourceMetrics> metrics;
  // TODO: Implement
  return metrics;
}

std::vector<ResourceTracker::ResourcePrediction> ResourceTracker::predictCapacity(int daysAhead) {
  std::vector<ResourcePrediction> predictions;
  // TODO: Implement prediction logic
  return predictions;
}

bool ResourceTracker::savePredictionToDatabase(const ResourcePrediction& prediction) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec_params(
        "INSERT INTO metadata.resource_predictions "
        "(resource_type, current_usage, predicted_usage, days_until_exhaustion, confidence) "
        "VALUES ($1, $2, $3, $4, $5)",
        prediction.resourceType, prediction.currentUsage, prediction.predictedUsage,
        prediction.daysUntilExhaustion, prediction.confidence);

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "ResourceTracker",
                  "Error saving prediction: " + std::string(e.what()));
    return false;
  }
}
