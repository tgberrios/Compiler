#include "SyncReporter.h"
#include <numeric>

std::string SyncReporter::currentProcessingTable;
std::string SyncReporter::lastProcessingTable;

std::string SyncReporter::getCurrentTimestamp() {
  static auto startTime = std::chrono::system_clock::now();
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  auto tm = *std::localtime(&time_t);

  char buffer[16];
  std::strftime(buffer, sizeof(buffer), "%H:%M:%S", &tm);
  return std::string(buffer);
}

std::string SyncReporter::getUptime() {
  static auto startTime = std::chrono::system_clock::now();
  auto now = std::chrono::system_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::seconds>(now - startTime);

  int totalSeconds = duration.count();
  int days = totalSeconds / 86400;
  int hours = (totalSeconds % 86400) / 3600;
  int minutes = (totalSeconds % 3600) / 60;
  int seconds = totalSeconds % 60;

  char buffer[32];
  if (days > 0) {
    snprintf(buffer, sizeof(buffer), "%dd %02dh %02dm %02ds", days, hours,
             minutes, seconds);
  } else if (hours > 0) {
    snprintf(buffer, sizeof(buffer), "%02dh %02dm %02ds", hours, minutes,
             seconds);
  } else {
    snprintf(buffer, sizeof(buffer), "%02dm %02ds", minutes, seconds);
  }
  return std::string(buffer);
}

std::string SyncReporter::calculateProcessingRate() {
  return std::to_string(SyncConfig::getChunkSize()) + "/chunk";
}

std::string SyncReporter::calculateLatency() { return "~1ms"; }

std::string SyncReporter::formatBytes(double bytes) {
  const char *units[] = {"B", "KB", "MB", "GB", "TB"};
  int unit = 0;
  while (bytes >= 1024 && unit < 4) {
    bytes /= 1024;
    unit++;
  }
  char buffer[32];
  snprintf(buffer, sizeof(buffer), "%.2f %s", bytes, units[unit]);
  return std::string(buffer);
}

std::string SyncReporter::formatDuration(double milliseconds) {
  if (milliseconds < 1.0) {
    return "< 1ms";
  } else if (milliseconds < 1000.0) {
    return std::to_string(static_cast<int>(milliseconds)) + "ms";
  } else {
    double seconds = milliseconds / 1000.0;
    if (seconds < 60.0) {
      return std::to_string(static_cast<int>(seconds)) + "s";
    } else {
      int minutes = static_cast<int>(seconds) / 60;
      int secs = static_cast<int>(seconds) % 60;
      char buffer[32];
      snprintf(buffer, sizeof(buffer), "%dm %02ds", minutes, secs);
      return std::string(buffer);
    }
  }
}

void SyncReporter::collectPerformanceMetrics(pqxx::connection &pgConn,
                                             SyncStats &stats) {
  try {
    pqxx::work txn(pgConn);

    // Get metrics for each engine type
    auto results = txn.exec(
        "WITH transfer_stats AS ("
        "  SELECT db_engine,"
        "         COUNT(*) as total_transfers,"
        "         COUNT(*) FILTER (WHERE status IN ('PERFECT_MATCH', "
        "'LISTENING_CHANGES')) as success_count,"
        "         COUNT(*) FILTER (WHERE status = 'ERROR') as error_count,"
        "         COUNT(*) FILTER (WHERE active = true) as active_transfers,"
        "         AVG(CASE WHEN last_sync_duration > 0 THEN last_sync_duration "
        "ELSE NULL END) as avg_duration,"
        "         MAX(CASE WHEN last_sync_duration > 0 THEN last_sync_duration "
        "ELSE NULL END) as max_duration,"
        "         SUM(last_sync_rows) as rows_transferred,"
        "         SUM(last_sync_bytes) as bytes_transferred,"
        "         MAX(last_error) as last_error"
        "  FROM metadata.catalog "
        "  GROUP BY db_engine"
        ")"
        "SELECT db_engine, total_transfers, success_count, error_count, "
        "active_transfers,"
        "       avg_duration, max_duration, rows_transferred, "
        "bytes_transferred, last_error,"
        "       CASE WHEN avg_duration > 0 THEN rows_transferred / "
        "NULLIF(avg_duration, 0) ELSE 0 END as rows_per_second "
        "FROM transfer_stats");

    for (const auto &row : results) {
      std::string engine = row[0].as<std::string>();
      TransferTypeMetrics metrics;

      metrics.totalTransfers = row[1].as<size_t>();
      metrics.successCount = row[2].as<size_t>();
      metrics.errorCount = row[3].as<size_t>();
      metrics.activeTransfers = row[4].as<size_t>();
      metrics.avgLatencyMs = row[5].is_null() ? 0.0 : row[5].as<double>();
      metrics.maxLatencyMs = row[6].is_null() ? 0.0 : row[6].as<double>();
      metrics.bytesTransferred = row[8].is_null() ? 0.0 : row[8].as<double>();
      metrics.lastError = row[9].is_null() ? "" : row[9].as<std::string>();
      metrics.recordsPerSecond = row[10].is_null() ? 0.0 : row[10].as<double>();

      stats.engineMetrics[engine] = std::move(metrics);
    }

    // Get current transfer progress
    auto currentTransfer =
        txn.exec1("SELECT schema_name, table_name, db_engine, "
                  "       total_rows, processed_rows, "
                  "       CEIL(total_rows::float / $1) as total_chunks, "
                  "       CEIL(processed_rows::float / $1) as current_chunk, "
                  "       CASE WHEN last_sync_start IS NOT NULL "
                  "            THEN EXTRACT(EPOCH FROM (NOW() - "
                  "last_sync_start)) * processed_rows / NULLIF(total_rows, 0) "
                  "            ELSE 0 END as rows_per_second "
                  "FROM metadata.catalog "
                  "WHERE status = 'PROCESSING' "
                  "LIMIT 1",
                  std::to_string(SyncConfig::getChunkSize()));

    if (!currentTransfer.empty()) {
      stats.currentTransfer.tableName = currentTransfer[0].as<std::string>() +
                                        "." +
                                        currentTransfer[1].as<std::string>();
      stats.currentTransfer.engineType = currentTransfer[2].as<std::string>();
      stats.currentTransfer.totalRows = currentTransfer[3].as<size_t>();
      stats.currentTransfer.processedRows = currentTransfer[4].as<size_t>();
      stats.currentTransfer.totalChunks = currentTransfer[5].as<size_t>();
      stats.currentTransfer.currentChunk = currentTransfer[6].as<size_t>();
      stats.currentTransfer.rowsPerSecond = currentTransfer[7].as<double>();
      stats.currentTransfer.inProgress = true;
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error("SyncReporter", "Error collecting performance metrics: " +
                                      std::string(e.what()));
  }
}

void SyncReporter::collectDatabaseHealthMetrics(pqxx::connection &pgConn,
                                                SyncStats &stats) {
  try {
    pqxx::work txn(pgConn);

    // Get connection stats
    auto connStats =
        txn.exec1("SELECT count(*) as active_connections, "
                  "       (SELECT setting::int FROM pg_settings WHERE name = "
                  "'max_connections') as max_connections, "
                  "       EXTRACT(EPOCH FROM (now() - "
                  "pg_postmaster_start_time())) as uptime_seconds");

    stats.activeConnections = connStats[0].as<int>();
    stats.totalConnections = connStats[1].as<int>();

    // Calculate uptime string
    int uptime = connStats[2].as<int>();
    int days = uptime / 86400;
    int hours = (uptime % 86400) / 3600;
    int minutes = (uptime % 3600) / 60;
    char uptimeStr[32];
    snprintf(uptimeStr, sizeof(uptimeStr), "%dd %02dh %02dm", days, hours,
             minutes);
    stats.uptime = uptimeStr;

    // Get cache hit ratios
    auto cacheStats =
        txn.exec1("SELECT sum(heap_blks_hit) / (sum(heap_blks_hit) + "
                  "sum(heap_blks_read)) * 100 as buffer_hit_ratio, "
                  "       sum(idx_blks_hit) / (sum(idx_blks_hit) + "
                  "sum(idx_blks_read)) * 100 as cache_hit_ratio "
                  "FROM pg_statio_user_tables");

    stats.bufferHitRate =
        cacheStats[0].is_null() ? 100.0 : cacheStats[0].as<double>();
    stats.cacheHitRate =
        cacheStats[1].is_null() ? 100.0 : cacheStats[1].as<double>();

    // Measure database response time
    auto start = std::chrono::high_resolution_clock::now();
    txn.exec1("SELECT 1");
    auto end = std::chrono::high_resolution_clock::now();
    stats.dbResponseTime =
        std::chrono::duration<double, std::milli>(end - start).count();

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error("SyncReporter", "Error collecting database health metrics: " +
                                      std::string(e.what()));
  }
}

void SyncReporter::collectSystemResourceMetrics(SyncStats &stats) {
  try {
    // Get CPU usage per core
    std::ifstream stat("/proc/stat");
    std::string line;
    std::vector<double> cpuUsage;

    static std::vector<std::pair<unsigned long, unsigned long>> prevCpuTimes;
    std::vector<std::pair<unsigned long, unsigned long>> currentCpuTimes;

    while (std::getline(stat, line)) {
      if (line.substr(0, 3) == "cpu") {
        std::istringstream ss(line);
        std::string cpu;
        unsigned long user, nice, system, idle, iowait, irq, softirq;
        ss >> cpu >> user >> nice >> system >> idle >> iowait >> irq >> softirq;

        unsigned long totalUser = user + nice;
        unsigned long totalSystem = system + irq + softirq;
        unsigned long total = totalUser + totalSystem + idle + iowait;
        unsigned long active = totalUser + totalSystem;

        currentCpuTimes.emplace_back(active, total);
      }
    }

    // Calculate CPU usage percentages
    if (!prevCpuTimes.empty()) {
      for (size_t i = 1; i < currentCpuTimes.size() && i <= prevCpuTimes.size();
           ++i) {
        auto &curr = currentCpuTimes[i];
        auto &prev = prevCpuTimes[i];

        if (curr.second > prev.second) { // Validate to avoid overflow
          unsigned long activeTime = curr.first - prev.first;
          unsigned long totalTime = curr.second - prev.second;

          if (totalTime > 0 && activeTime <= totalTime) { // Validate ratio
            double usage = (activeTime * 100.0) / totalTime;
            if (usage >= 0.0 && usage <= 100.0) { // Validate percentage
              cpuUsage.push_back(usage);
            }
          }
        }
      }
    }

    prevCpuTimes = std::move(currentCpuTimes);
    stats.resources.cpuPerCore = std::move(cpuUsage);
    stats.resources.totalCpuUsage =
        stats.resources.cpuPerCore.empty()
            ? 0.0
            : std::accumulate(stats.resources.cpuPerCore.begin(),
                              stats.resources.cpuPerCore.end(), 0.0) /
                  stats.resources.cpuPerCore.size();

    // Get memory usage
    std::ifstream meminfo("/proc/meminfo");
    unsigned long totalKb = 0, availableKb = 0;

    while (std::getline(meminfo, line)) {
      if (line.compare(0, 9, "MemTotal:") == 0) {
        totalKb = std::stoul(line.substr(line.find_first_of("0123456789")));
      } else if (line.compare(0, 13, "MemAvailable:") == 0) {
        availableKb = std::stoul(line.substr(line.find_first_of("0123456789")));
      }
    }

    stats.resources.totalMemory = totalKb * 1024.0;
    stats.resources.memoryRSS = (totalKb - availableKb) * 1024.0;
    stats.resources.memoryVirtual =
        stats.resources.memoryRSS * 1.5; // Aproximación

    // Get disk I/O stats
    std::ifstream diskstats("/proc/diskstats");
    unsigned long readBytes = 0, writeBytes = 0;
    static unsigned long prevReadBytes = 0, prevWriteBytes = 0;
    static auto prevTime = std::chrono::steady_clock::now();

    while (std::getline(diskstats, line)) {
      std::istringstream ss(line);
      unsigned int major, minor;
      std::string dev;
      unsigned long reads, readSectors, writes, writeSectors;

      ss >> major >> minor >> dev;
      if (dev == "sda" || dev == "nvme0n1") {
        ss >> reads >> readSectors >> writes >> writeSectors;
        readBytes += readSectors * 512;
        writeBytes += writeSectors * 512;
      }
    }

    auto now = std::chrono::steady_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(now - prevTime)
            .count();

    if (duration > 0) {
      stats.resources.diskReadBytesPerSec =
          (readBytes - prevReadBytes) / duration;
      stats.resources.diskWriteBytesPerSec =
          (writeBytes - prevWriteBytes) / duration;
    }

    prevReadBytes = readBytes;
    prevWriteBytes = writeBytes;
    prevTime = now;

    // Get network I/O stats
    std::ifstream netdev("/proc/net/dev");
    unsigned long rxBytes = 0, txBytes = 0;
    static unsigned long prevRxBytes = 0, prevTxBytes = 0;
    static auto prevNetTime = std::chrono::steady_clock::now();

    std::getline(netdev, line); // Skip header
    std::getline(netdev, line); // Skip header

    while (std::getline(netdev, line)) {
      std::istringstream ss(line);
      std::string iface;
      ss >> iface;

      if (iface != "lo:") {
        unsigned long rx, tx;
        ss >> rx;
        for (int i = 0; i < 7; ++i)
          ss >> rx; // Skip other fields
        ss >> tx;

        rxBytes += rx;
        txBytes += tx;
      }
    }

    auto netNow = std::chrono::steady_clock::now();
    auto netDuration =
        std::chrono::duration_cast<std::chrono::seconds>(netNow - prevNetTime)
            .count();

    if (netDuration > 0) {
      stats.resources.networkInBytesPerSec =
          (rxBytes - prevRxBytes) / netDuration;
      stats.resources.networkOutBytesPerSec =
          (txBytes - prevTxBytes) / netDuration;
    }

    prevRxBytes = rxBytes;
    prevTxBytes = txBytes;
    prevNetTime = netNow;

  } catch (const std::exception &e) {
    Logger::error("SyncReporter",
                  "Error collecting system metrics: " + std::string(e.what()));
  }
}

std::vector<SyncReporter::TableStatus>
SyncReporter::getAllTableStatuses(pqxx::connection &pgConn) {
  std::vector<SyncReporter::TableStatus> tables;

  try {
    pqxx::work txn(pgConn);
    auto results =
        txn.exec("SELECT schema_name, table_name, db_engine, status, "
                 "last_offset, active "
                 "FROM metadata.catalog "
                 "ORDER BY db_engine, schema_name, table_name;");
    txn.commit();

    for (const auto &row : results) {
      if (row.size() < 6)
        continue;

      TableStatus table;
      table.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
      table.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
      table.db_engine = row[2].is_null() ? "" : row[2].as<std::string>();
      table.status = row[3].is_null() ? "" : row[3].as<std::string>();
      table.last_offset = row[4].is_null() ? "0" : row[4].as<std::string>();
      table.active = row[5].is_null() ? false : row[5].as<bool>();

      tables.push_back(table);
    }
  } catch (const std::exception &e) {
    Logger::error("SyncReporter",
                  "Error getting table statuses: " + std::string(e.what()));
  }

  return tables;
}

SyncReporter::SyncStats
SyncReporter::calculateSyncStats(const std::vector<TableStatus> &tables) {
  SyncReporter::SyncStats stats;
  stats.totalTables = tables.size();

  for (const auto &table : tables) {
    if (table.status == "PERFECT_MATCH") {
      stats.perfectMatchCount++;
    } else if (table.status == "LISTENING_CHANGES") {
      stats.listeningChangesCount++;
    } else if (table.status == "NO_DATA") {
      stats.noDataCount++;
    } else if (table.status == "FULL_LOAD") {
      if (table.active) {
        stats.fullLoadActiveCount++;
      } else {
        stats.fullLoadInactiveCount++;
      }
    } else if (table.status == "RESET") {
      if (table.active) {
        stats.fullLoadActiveCount++;
      } else {
        stats.fullLoadInactiveCount++;
      }
    } else if (table.status == "ERROR") {
      stats.errorCount++;
    }
  }

  stats.totalSynchronized =
      stats.perfectMatchCount + stats.listeningChangesCount;
  stats.totalErrors = stats.errorCount;

  return stats;
}

void SyncReporter::printDashboard(const std::vector<TableStatus> &tables,
                                  const SyncReporter::SyncStats &stats) {
#ifdef _WIN32
  system("cls");
#else
  system("clear");
#endif
  std::cout << std::flush;
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  double progress = (stats.totalTables > 0)
                        ? static_cast<double>(stats.totalSynchronized) /
                              static_cast<double>(stats.totalTables)
                        : 0.0;
  int progressPercent = static_cast<int>(progress * 100.0);
  int progressBars = static_cast<int>(progress * 30.0);

  std::cout << "╔════════════════════════════════════════════════════════════"
               "══════════════════╗\n";
  std::cout << "║                           DataSync Real-Time Dashboard     "
               "                 ║\n";
  std::cout << "╚════════════════════════════════════════════════════════════"
               "══════════════════╝\n\n";

  // Main Status Section
  std::cout << "■ SYNCHRONIZATION STATUS\n";
  std::cout << "├─ Progress: ";
  for (int i = 0; i < 30; ++i) {
    if (i < progressBars) {
      std::cout << "█";
    } else {
      std::cout << "░";
    }
  }
  std::cout << " " << progressPercent << "%\n";
  std::cout << "├─ Perfect Match: " << stats.perfectMatchCount << "\n";
  std::cout << "├─ Listening Changes: " << stats.listeningChangesCount << "\n";
  std::cout << "├─ Full Load (Active): " << stats.fullLoadActiveCount << "\n";
  std::cout << "├─ Full Load (Inactive): " << stats.fullLoadInactiveCount
            << "\n";
  std::cout << "├─ No Data: " << stats.noDataCount << "\n";
  std::cout << "├─ Errors: " << stats.errorCount << "\n";

  // Current Processing
  if (!currentProcessingTable.empty()) {
    std::cout << "├─ ► Currently Processing: " << currentProcessingTable
              << "\n";
  } else if (!lastProcessingTable.empty()) {
    std::cout << "├─ • Last Processed: " << lastProcessingTable << "\n";
  }

  // Transfer Performance by Engine
  std::cout << "\n▲ TRANSFER PERFORMANCE BY ENGINE\n";
  const std::vector<std::string> engines = {"MariaDB", "MSSQL", "MongoDB",
                                            "PostgreSQL"};
  bool hasActiveEngines = false;

  for (const auto &engine : engines) {
    auto it = stats.engineMetrics.find(engine);
    if (it != stats.engineMetrics.end() &&
        (it->second.activeTransfers > 0 || it->second.totalTransfers > 0)) {
      hasActiveEngines = true;
      const auto &metrics = it->second;
      std::cout << "├─ " << engine << " → PostgreSQL\n";
      std::cout << "│  ├─ Transfer Rate: " << std::fixed << std::setprecision(2)
                << metrics.recordsPerSecond << " records/sec\n";
      std::cout << "│  ├─ Success Rate: "
                << (metrics.successCount + metrics.errorCount > 0
                        ? (metrics.successCount * 100.0 /
                           (metrics.successCount + metrics.errorCount))
                        : 100.0)
                << "%\n";
      std::cout << "│  ├─ Avg Latency: " << formatDuration(metrics.avgLatencyMs)
                << "\n";
      std::cout << "│  └─ Active Transfers: " << metrics.activeTransfers << "/"
                << metrics.totalTransfers << "\n";
      std::cout << "│\n";
    }
  }

  if (!hasActiveEngines) {
    std::cout << "├─ No active transfers\n";
    std::cout << "└─ System ready for synchronization\n";
  }

  // Current Transfer Details
  if (stats.currentTransfer.inProgress) {
    std::cout << "\n■ CURRENT TRANSFER DETAILS\n";
    std::cout << "├─ Table: " << stats.currentTransfer.tableName << "\n";
    std::cout << "├─ Engine: " << stats.currentTransfer.engineType << "\n";

    double progress =
        stats.currentTransfer.totalRows > 0
            ? (static_cast<double>(stats.currentTransfer.processedRows) /
               stats.currentTransfer.totalRows * 100.0)
            : 0.0;

    std::cout << "├─ Progress: " << std::fixed << std::setprecision(1)
              << progress << "% (" << stats.currentTransfer.processedRows << "/"
              << stats.currentTransfer.totalRows << " rows)\n";
    std::cout << "├─ Speed: " << stats.currentTransfer.rowsPerSecond
              << " rows/sec\n";

    if (stats.currentTransfer.rowsPerSecond > 0) {
      size_t remainingRows =
          stats.currentTransfer.totalRows - stats.currentTransfer.processedRows;
      double remainingSeconds =
          remainingRows / stats.currentTransfer.rowsPerSecond;
      int hours = remainingSeconds / 3600;
      int minutes = (static_cast<int>(remainingSeconds) % 3600) / 60;
      int seconds = static_cast<int>(remainingSeconds) % 60;

      char timeStr[20];
      snprintf(timeStr, sizeof(timeStr), "%02d:%02d:%02d", hours, minutes,
               seconds);
      std::cout << "├─ Est. Time Remaining: " << timeStr << "\n";
    }

    std::cout << "└─ Current Chunk: " << stats.currentTransfer.currentChunk
              << "/" << stats.currentTransfer.totalChunks << "\n";
  }

  // System Resources Section
  std::cout << "\n● SYSTEM RESOURCES\n";
  std::cout << "├─ CPU Usage: " << std::fixed << std::setprecision(1)
            << stats.resources.totalCpuUsage << "% ("
            << stats.resources.cpuPerCore.size() << " cores)\n";

  // Display per-core CPU usage in pairs
  for (size_t i = 0; i < stats.resources.cpuPerCore.size(); i += 2) {
    std::cout << "│  ├─ Core " << (i + 1) << ": " << std::setw(3)
              << stats.resources.cpuPerCore[i] << "%  ";
    if (i + 1 < stats.resources.cpuPerCore.size()) {
      std::cout << "Core " << (i + 2) << ": " << std::setw(3)
                << stats.resources.cpuPerCore[i + 1] << "%";
    }
    std::cout << "\n";
  }

  std::cout << "│\n├─ Memory: " << formatBytes(stats.resources.memoryRSS) << "/"
            << formatBytes(stats.resources.totalMemory) << " (" << std::fixed
            << std::setprecision(1)
            << (stats.resources.memoryRSS * 100.0 / stats.resources.totalMemory)
            << "%)\n";
  std::cout << "│  ├─ RSS: " << formatBytes(stats.resources.memoryRSS) << "\n";
  std::cout << "│  └─ Virtual: " << formatBytes(stats.resources.memoryVirtual)
            << "\n";
  std::cout << "│\n";

  std::cout << "├─ Disk I/O\n";
  std::cout << "│  ├─ Read: "
            << formatBytes(stats.resources.diskReadBytesPerSec) << "/s\n";
  std::cout << "│  └─ Write: "
            << formatBytes(stats.resources.diskWriteBytesPerSec) << "/s\n";
  std::cout << "│\n";

  std::cout << "└─ Network I/O\n";
  std::cout << "   ├─ Incoming: "
            << formatBytes(stats.resources.networkInBytesPerSec) << "/s\n";
  std::cout << "   └─ Outgoing: "
            << formatBytes(stats.resources.networkOutBytesPerSec) << "/s\n";

  // Database Health Section
  std::cout << "\n■ DATABASE HEALTH\n";
  std::cout << "├─ Active Connections: " << stats.activeConnections << "/"
            << stats.totalConnections << "\n";
  std::cout << "├─ Response Time: " << formatDuration(stats.dbResponseTime)
            << "\n";
  std::cout << "├─ Buffer Hit Rate: " << std::fixed << std::setprecision(1)
            << stats.bufferHitRate << "%\n";
  std::cout << "├─ Cache Hit Rate: " << stats.cacheHitRate << "%\n";
  std::cout << "└─ Status: "
            << (stats.dbResponseTime < 100 ? "✓ Healthy" : "⚠ Slow") << "\n";

  // Connection Pool Section
  std::cout << "\n■ CONNECTION POOLING\n";
  std::cout << "├─ Total Pools: " << stats.poolMetrics.totalPools
            << " (PG, MSSQL, MariaDB, MongoDB)\n";
  std::cout << "├─ Active Connections: " << stats.poolMetrics.activeConnections
            << "\n";
  std::cout << "├─ Idle Connections: " << stats.poolMetrics.idleConnections
            << "\n";
  std::cout << "├─ Failed Connections: " << stats.poolMetrics.failedConnections
            << "\n";
  std::cout << "└─ Last Cleanup: " << stats.poolMetrics.lastCleanup << "\n";

  // Recent Activity Section
  std::cout << "\n▲ RECENT ACTIVITY (Last Hour)\n";
  std::cout << "├─ Transfers: " << stats.transfersLastHour << "\n";
  std::cout << "├─ Errors: " << stats.errorsLastHour << "\n";
  if (!stats.lastError.empty()) {
    std::cout << "├─ Last Error: " << stats.lastError.substr(0, 50) << "...\n";
  }
  std::cout << "└─ Uptime: " << stats.uptime << "\n";

  // Footer
  std::cout << "\n◄ " << getCurrentTimestamp() << " | Press Ctrl+C to exit\n";
}

void SyncReporter::collectConnectionPoolMetrics(SyncStats &stats) {
  if (g_connectionPool) {
    auto poolStats = g_connectionPool->getStats();
    stats.poolMetrics.totalPools = 4; // PG, MSSQL, MariaDB, MongoDB
    stats.poolMetrics.activeConnections = poolStats.activeConnections;
    stats.poolMetrics.idleConnections = poolStats.idleConnections;
    stats.poolMetrics.failedConnections = poolStats.failedConnections;

    // Format last cleanup time
    auto now = std::chrono::steady_clock::now();
    auto cleanupDiff = std::chrono::duration_cast<std::chrono::minutes>(
                           now - poolStats.lastCleanup)
                           .count();
    stats.poolMetrics.lastCleanup = std::to_string(cleanupDiff) + "m ago";
  }
}

void SyncReporter::generateFullReport(pqxx::connection &pgConn) {
  auto tables = getAllTableStatuses(pgConn);
  auto stats = calculateSyncStats(tables);

  // Set uptime at the start of report generation
  stats.uptime = getUptime();

  // Collect additional metrics
  collectPerformanceMetrics(pgConn, stats);
  collectDatabaseHealthMetrics(pgConn, stats);
  collectSystemResourceMetrics(stats);
  collectConnectionPoolMetrics(stats);
  collectRecentActivityMetrics(pgConn, stats);

  printDashboard(tables, stats);
}

void SyncReporter::collectRecentActivityMetrics(pqxx::connection &pgConn,
                                                SyncStats &stats) {
  try {
    pqxx::work txn(pgConn);

    auto results = txn.exec1(
        "SELECT COUNT(*) FILTER (WHERE last_sync_start > NOW() - INTERVAL '1 "
        "hour') as transfers_last_hour, "
        "       COUNT(*) FILTER (WHERE status = 'ERROR' AND last_error_time > "
        "NOW() - INTERVAL '1 hour') as errors_last_hour, "
        "       MAX(CASE WHEN status = 'ERROR' THEN last_error ELSE NULL END) "
        "as last_error "
        "FROM metadata.catalog");

    stats.transfersLastHour = results[0].as<int>();
    stats.errorsLastHour = results[1].as<int>();
    stats.lastError = results[2].is_null() ? "" : results[2].as<std::string>();

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error("SyncReporter", "Error collecting recent activity metrics: " +
                                      std::string(e.what()));
  }
}