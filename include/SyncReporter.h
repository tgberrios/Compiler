#ifndef SYNCREPORTER_H
#define SYNCREPORTER_H

#include "Config.h"
#include "ConnectionPool.h"
#include "logger.h"
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <pqxx/pqxx>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class SyncReporter {
public:
  SyncReporter() = default;
  ~SyncReporter() = default;

  static std::string currentProcessingTable;
  static std::string lastProcessingTable;

  struct TransferTypeMetrics {
    double recordsPerSecond = 0.0;
    double bytesTransferred = 0.0;
    double avgLatencyMs = 0.0;
    double maxLatencyMs = 0.0;
    size_t successCount = 0;
    size_t errorCount = 0;
    size_t activeTransfers = 0;
    size_t totalTransfers = 0;
    std::string lastError = "";
  };

  struct CurrentTransferProgress {
    std::string tableName = "";
    std::string engineType = "";
    size_t totalRows = 0;
    size_t processedRows = 0;
    size_t currentChunk = 0;
    size_t totalChunks = 0;
    double rowsPerSecond = 0.0;
    std::chrono::system_clock::time_point startTime;
    bool inProgress = false;
  };

  struct PoolMetrics {
    size_t totalPools = 0;
    size_t activeConnections = 0;
    size_t idleConnections = 0;
    size_t failedConnections = 0;
    std::string lastCleanup;
  };

  struct ResourceMetrics {
    std::vector<double> cpuPerCore;
    double totalCpuUsage = 0.0;
    double memoryRSS = 0.0;
    double memoryVirtual = 0.0;
    double totalMemory = 0.0;
    double diskReadBytesPerSec = 0.0;
    double diskWriteBytesPerSec = 0.0;
    double networkInBytesPerSec = 0.0;
    double networkOutBytesPerSec = 0.0;
  };

  struct SyncStats {
    size_t totalTables = 0;
    size_t perfectMatchCount = 0;
    size_t listeningChangesCount = 0;
    size_t fullLoadActiveCount = 0;
    size_t fullLoadInactiveCount = 0;
    size_t noDataCount = 0;
    size_t errorCount = 0;
    size_t totalSynchronized = 0;
    size_t totalErrors = 0;

    // Per-Engine Performance Metrics
    std::unordered_map<std::string, TransferTypeMetrics> engineMetrics;

    // Current Transfer Details
    CurrentTransferProgress currentTransfer;

    // Enhanced System Resources
    ResourceMetrics resources;

    // Connection Pool Metrics
    PoolMetrics poolMetrics;

    // Database Health
    int activeConnections = 0;
    int totalConnections = 0;
    double dbResponseTime = 0.0;
    double bufferHitRate = 0.0;
    double cacheHitRate = 0.0;

    // Performance Trends
    double avgTransferRateTrend = 0.0;
    double successRateTrend = 100.0;
    double latencyTrend = 0.0;
    std::string resourceStatus = "Optimal";

    // Recent Activity
    int transfersLastHour = 0;
    int errorsLastHour = 0;
    std::string lastError = "";
    std::string uptime = "";
  };

  struct TableStatus {
    std::string schema_name;
    std::string table_name;
    std::string db_engine;
    std::string status;
    std::string last_offset;
    bool active = true;
  };

  std::vector<TableStatus> getAllTableStatuses(pqxx::connection &pgConn);
  SyncStats calculateSyncStats(const std::vector<TableStatus> &tables);
  void printDashboard(const std::vector<TableStatus> &tables,
                      const SyncStats &stats);
  void generateFullReport(pqxx::connection &pgConn);
  void refreshDebugConfig();
  std::string getCurrentTimestamp();
  std::string calculateProcessingRate();
  std::string calculateLatency();

  // Metric collection functions
  void collectPerformanceMetrics(pqxx::connection &pgConn, SyncStats &stats);
  void collectDatabaseHealthMetrics(pqxx::connection &pgConn, SyncStats &stats);
  void collectSystemResourceMetrics(SyncStats &stats);
  void collectConnectionPoolMetrics(SyncStats &stats);
  void collectRecentActivityMetrics(pqxx::connection &pgConn, SyncStats &stats);
  std::string formatBytes(double bytes);
  std::string formatDuration(double milliseconds);
  std::string getUptime();
  double getCpuUsage();
  double getMemoryUsage();
  double getDiskUsage();
};

#endif // SYNCREPORTER_H