#ifndef SYNCREPORTER_H
#define SYNCREPORTER_H

#include "Config.h"
#include <chrono>
#include <cstdlib>
#include <fstream>
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
  };

  struct TableStatus {
    std::string schema_name;
    std::string table_name;
    std::string db_engine;
    std::string status;
    std::string last_offset;
    bool active = true;
  };

  std::vector<TableStatus> getAllTableStatuses(pqxx::connection &pgConn) {
    std::vector<TableStatus> tables;

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
      std::cerr << "Error getting table statuses: " << e.what() << std::endl;
    }

    return tables;
  }

  SyncStats calculateSyncStats(const std::vector<TableStatus> &tables) {
    SyncStats stats;
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

  void printDashboard(const std::vector<TableStatus> &tables,
                      const SyncStats &stats) {
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

    std::cout << "DataSync Status:\n";
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
    std::cout << "├─ Listening Changes: " << stats.listeningChangesCount
              << "\n";
    std::cout << "├─ Full Load (Active): " << stats.fullLoadActiveCount << "\n";
    std::cout << "├─ Full Load (Inactive): " << stats.fullLoadInactiveCount
              << "\n";
    std::cout << "├─ No Data: " << stats.noDataCount << "\n";
    std::cout << "├─ Errors: " << stats.errorCount << "\n";

    // Mostrar tabla actualmente procesando
    if (!currentProcessingTable.empty()) {
      std::cout << "├─ ▶ Currently Processing: " << currentProcessingTable
                << "\n";
    } else if (!lastProcessingTable.empty()) {
      std::cout << "├─ • Last Processed: " << lastProcessingTable << "\n";
    }

    std::cout << "├─ Processing Rate: " << calculateProcessingRate() << "\n";
    std::cout << "├─ Latency: " << calculateLatency() << "\n";
    std::cout << "├─ Chunk Size: " << SyncConfig::getChunkSize() << "\n";
    std::cout << "├─ Interval: " << SyncConfig::getSyncInterval() << "s\n";
    std::cout << "└─ Time: " << getCurrentTimestamp() << std::endl;
  }

  void generateFullReport(pqxx::connection &pgConn) {
    auto tables = getAllTableStatuses(pgConn);
    auto stats = calculateSyncStats(tables);
    printDashboard(tables, stats);
  }

  std::string getCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto tm = *std::localtime(&time_t);

    char buffer[16];
    std::strftime(buffer, sizeof(buffer), "%H:%M:%S", &tm);
    return std::string(buffer);
  }

  std::string calculateProcessingRate() {
    return std::to_string(SyncConfig::getChunkSize()) + "/chunk";
  }

  std::string calculateLatency() { return "~1ms"; }
};

// Definición de variables estáticas
std::string SyncReporter::currentProcessingTable = "";
std::string SyncReporter::lastProcessingTable = "";

#endif // SYNCREPORTER_H
