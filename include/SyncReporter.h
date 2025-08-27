#ifndef SYNCREPORTER_H
#define SYNCREPORTER_H

#include "ConnectionManager.h"
#include <chrono>
#include <fstream>
#include <iostream>
#include <pqxx/pqxx>
#include <string>
#include <unordered_map>
#include <vector>

class SyncReporter {
public:
  SyncReporter() = default;
  ~SyncReporter() = default;

  struct SyncStats {
    size_t totalTables = 0;
    size_t skippedTables = 0;
    size_t perfectMatchCount = 0;
    size_t listeningChangesCount = 0;
    size_t errorCount = 0;
    size_t inProgressCount = 0;
    size_t totalSynchronized = 0;
    size_t totalErrors = 0;
  };

  struct TableStatus {
    std::string schema_name;
    std::string table_name;
    std::string db_engine;
    std::string status;
    std::string last_sync_time;
    std::string last_sync_column;
    std::string last_offset;
    std::string connection_string;
  };

  std::vector<TableStatus> getAllTableStatuses(pqxx::connection &pgConn) {
    ConnectionManager cm;
    std::vector<TableStatus> tables;

    auto results = cm.executeQueryPostgres(
        pgConn,
        "SELECT schema_name, table_name, db_engine, status, "
        "last_sync_time, last_sync_column, last_offset, connection_string "
        "FROM metadata.catalog "
        "WHERE active='YES' "
        "ORDER BY db_engine, schema_name, table_name;");

    for (const auto &row : results) {
      if (row.size() < 8)
        continue;

      TableStatus table;
      table.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
      table.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
      table.db_engine = row[2].is_null() ? "" : row[2].as<std::string>();
      table.status = row[3].is_null() ? "" : row[3].as<std::string>();
      table.last_sync_time = row[4].is_null() ? "" : row[4].as<std::string>();
      table.last_sync_column = row[5].is_null() ? "" : row[5].as<std::string>();
      table.last_offset = row[6].is_null() ? "0" : row[6].as<std::string>();
      table.connection_string =
          row[7].is_null() ? "" : row[7].as<std::string>();

      tables.push_back(table);
    }

    return tables;
  }

  SyncStats calculateSyncStats(const std::vector<TableStatus> &tables) {
    SyncStats stats;
    stats.totalTables = tables.size();

    for (const auto &table : tables) {
      if (table.status == "PERFECT MATCH") {
        stats.perfectMatchCount++;
      } else if (table.status == "LISTENING_CHANGES") {
        stats.listeningChangesCount++;
      } else if (table.status == "error") {
        stats.errorCount++;
      } else if (table.status == "in_progress") {
        stats.inProgressCount++;
      }
    }

    stats.totalSynchronized =
        stats.perfectMatchCount + stats.listeningChangesCount;
    stats.totalErrors = stats.errorCount + stats.inProgressCount;

    return stats;
  }

  void printDetailedSyncReport(const std::vector<TableStatus> &tables,
                               const SyncStats &stats) {
    std::cout << "\n\u25A0 Synchronization Summary Report" << std::endl;
    std::cout << "=====================================" << std::endl;
    std::cout << "   Total tables: " << stats.totalTables << std::endl;
    std::cout << "   Skipped (already synced): " << stats.skippedTables
              << std::endl;
    std::cout << "   PERFECT_MATCH: " << stats.perfectMatchCount << std::endl;
    std::cout << "   LISTENING_CHANGES: " << stats.listeningChangesCount
              << std::endl;
    std::cout << "   Errors: " << stats.errorCount << std::endl;
    std::cout << "   In Progress: " << stats.inProgressCount << std::endl;
    std::cout << "   Total synchronized: " << stats.totalSynchronized << "/"
              << stats.totalTables << " completed" << std::endl;
    std::cout << "   Success Rate: "
              << (stats.totalTables > 0
                      ? (stats.totalSynchronized * 100 / stats.totalTables)
                      : 0)
              << "%" << std::endl;

    if (stats.totalErrors > 0) {
      std::cout << "\n\u26A0 Tables with Issues:" << std::endl;
      for (const auto &table : tables) {
        if (table.status == "error" || table.status == "in_progress") {
          std::cout << "   - " << table.schema_name << "." << table.table_name
                    << " (" << table.db_engine << ") - " << table.status
                    << std::endl;
        }
      }
    }

    std::cout << "\n\u2714 Tables Successfully Synced:" << std::endl;
    for (const auto &table : tables) {
      if (table.status == "PERFECT MATCH" ||
          table.status == "LISTENING_CHANGES") {
        std::cout << "   ✓ " << table.schema_name << "." << table.table_name
                  << " (" << table.db_engine << ") - " << table.status
                  << " [Offset: " << table.last_offset << "]" << std::endl;
      }
    }
  }

  void printEngineSpecificReport(const std::vector<TableStatus> &tables,
                                 const std::string &engine) {
    std::cout << "\n\u25A0 " << engine << " Synchronization Report"
              << std::endl;
    std::cout << "=====================================" << std::endl;

    size_t engineTables = 0;
    size_t enginePerfectMatch = 0;
    size_t engineListeningChanges = 0;
    size_t engineErrors = 0;

    for (const auto &table : tables) {
      if (table.db_engine == engine) {
        engineTables++;
        if (table.status == "PERFECT MATCH") {
          enginePerfectMatch++;
        } else if (table.status == "LISTENING_CHANGES") {
          engineListeningChanges++;
        } else if (table.status == "error") {
          engineErrors++;
        }
      }
    }

    std::cout << "   Total " << engine << " tables: " << engineTables
              << std::endl;
    std::cout << "   PERFECT_MATCH: " << enginePerfectMatch << std::endl;
    std::cout << "   LISTENING_CHANGES: " << engineListeningChanges
              << std::endl;
    std::cout << "   Errors: " << engineErrors << std::endl;
    std::cout << "   Success Rate: "
              << (engineTables > 0
                      ? ((enginePerfectMatch + engineListeningChanges) * 100 /
                         engineTables)
                      : 0)
              << "%" << std::endl;

    std::cout << "\n\u2714 " << engine << " Tables Details:" << std::endl;
    for (const auto &table : tables) {
      if (table.db_engine == engine) {
        std::string statusIcon = (table.status == "PERFECT MATCH" ||
                                  table.status == "LISTENING_CHANGES")
                                     ? "✓"
                                     : "✗";
        std::cout << "   " << statusIcon << " " << table.schema_name << "."
                  << table.table_name << " - " << table.status
                  << " [Offset: " << table.last_offset << "]" << std::endl;
      }
    }
  }

  void printBidirectionalSyncReport(const std::vector<TableStatus> &tables) {
    std::cout << "\n\u25A0 Bidirectional Synchronization Status" << std::endl;
    std::cout << "===========================================" << std::endl;

    std::unordered_map<std::string, std::vector<std::string>> schemaTables;

    for (const auto &table : tables) {
      std::string key = table.schema_name + "." + table.table_name;
      schemaTables[key].push_back(table.db_engine);
    }

    size_t bidirectionalTables = 0;
    size_t unidirectionalTables = 0;

    for (const auto &[tableKey, engines] : schemaTables) {
      if (engines.size() > 1) {
        bidirectionalTables++;
        std::cout << "   🔄 " << tableKey << " - Bidirectional ("
                  << engines.size() << " engines)" << std::endl;
      } else {
        unidirectionalTables++;
      }
    }

    std::cout << "\n   Bidirectional tables: " << bidirectionalTables
              << std::endl;
    std::cout << "   Unidirectional tables: " << unidirectionalTables
              << std::endl;
    std::cout << "   Total unique tables: " << schemaTables.size() << std::endl;
  }

  void printPerformanceMetrics(const std::vector<TableStatus> &tables) {
    std::cout << "\n\u25A0 Performance Metrics" << std::endl;
    std::cout << "=========================" << std::endl;

    size_t totalRecords = 0;
    size_t activeSyncTables = 0;

    for (const auto &table : tables) {
      if (table.status == "PERFECT MATCH" ||
          table.status == "LISTENING_CHANGES") {
        activeSyncTables++;
        try {
          totalRecords += std::stoul(table.last_offset);
        } catch (...) {
          totalRecords += 0;
        }
      }
    }

    std::cout << "   Active sync tables: " << activeSyncTables << std::endl;
    std::cout << "   Total records synced: " << totalRecords << std::endl;
    std::cout << "   Average records per table: "
              << (activeSyncTables > 0 ? totalRecords / activeSyncTables : 0)
              << std::endl;
  }

  void generateFullReport(pqxx::connection &pgConn) {
    auto tables = getAllTableStatuses(pgConn);
    auto stats = calculateSyncStats(tables);

    printDetailedSyncReport(tables, stats);
    printEngineSpecificReport(tables, "MariaDB");
    printEngineSpecificReport(tables, "Postgres");
    printBidirectionalSyncReport(tables);
    printPerformanceMetrics(tables);

    std::cout << "\n\u25A0 Report Generated at: " << getCurrentTimestamp()
              << std::endl;
    std::cout << "=====================================" << std::endl;
  }

  std::string getCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto tm = *std::localtime(&time_t);

    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm);
    return std::string(buffer);
  }

  void exportReportToFile(const std::vector<TableStatus> &tables,
                          const SyncStats &stats, const std::string &filename) {
    std::ofstream file(filename);
    if (!file.is_open()) {
      std::cerr << "Error: Could not open file " << filename << " for writing"
                << std::endl;
      return;
    }

    file << "DataSync Synchronization Report" << std::endl;
    file << "Generated: " << getCurrentTimestamp() << std::endl;
    file << "=====================================" << std::endl;
    file << "Total tables: " << stats.totalTables << std::endl;
    file << "PERFECT_MATCH: " << stats.perfectMatchCount << std::endl;
    file << "LISTENING_CHANGES: " << stats.listeningChangesCount << std::endl;
    file << "Errors: " << stats.errorCount << std::endl;
    file << "Success Rate: "
         << (stats.totalTables > 0
                 ? (stats.totalSynchronized * 100 / stats.totalTables)
                 : 0)
         << "%" << std::endl;
    file << std::endl;

    file << "Table Details:" << std::endl;
    file << "=============" << std::endl;
    for (const auto &table : tables) {
      file << table.schema_name << "." << table.table_name << " | "
           << table.db_engine << " | " << table.status << " | "
           << table.last_offset << std::endl;
    }

    file.close();
    std::cout << "Report exported to " << filename << std::endl;
  }
};

#endif // SYNCREPORTER_H
