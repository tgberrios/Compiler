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
    size_t perfectMatchCount = 0;
    size_t listeningChangesCount = 0;
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
  };

  std::vector<TableStatus> getAllTableStatuses(pqxx::connection &pgConn) {
    ConnectionManager cm;
    std::vector<TableStatus> tables;

    auto results = cm.executeQueryPostgres(
        pgConn,
        "SELECT schema_name, table_name, db_engine, status, last_offset "
        "FROM metadata.catalog "
        "WHERE active='YES' "
        "ORDER BY db_engine, schema_name, table_name;");

    for (const auto &row : results) {
      if (row.size() < 5)
        continue;

      TableStatus table;
      table.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
      table.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
      table.db_engine = row[2].is_null() ? "" : row[2].as<std::string>();
      table.status = row[3].is_null() ? "" : row[3].as<std::string>();
      table.last_offset = row[4].is_null() ? "0" : row[4].as<std::string>();

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
      }
    }

    stats.totalSynchronized =
        stats.perfectMatchCount + stats.listeningChangesCount;
    stats.totalErrors = stats.errorCount;

    return stats;
  }

  void printCompactReport(const std::vector<TableStatus> &tables,
                          const SyncStats &stats) {
    std::cout << "\r\u25A0 Sync: " << stats.totalSynchronized << "/"
              << stats.totalTables << " tables | ✓ " << stats.perfectMatchCount
              << " | 🔄 " << stats.listeningChangesCount << " | ✗ "
              << stats.errorCount << " | " << getCurrentTimestamp()
              << std::flush;
  }

  void printFinalReport(const std::vector<TableStatus> &tables,
                        const SyncStats &stats) {
    std::cout << "\n\n\u25A0 FINAL SYNC REPORT " << getCurrentTimestamp()
              << std::endl;
    std::cout << "=====================================" << std::endl;
    std::cout << "Total: " << stats.totalTables
              << " | Synced: " << stats.totalSynchronized << " | Success: "
              << (stats.totalTables > 0
                      ? (stats.totalSynchronized * 100 / stats.totalTables)
                      : 0)
              << "%" << std::endl;

    if (stats.errorCount > 0) {
      std::cout << "\n\u26A0 Issues: ";
      for (const auto &table : tables) {
        if (table.status == "error") {
          std::cout << table.schema_name << "." << table.table_name << " ";
        }
      }
      std::cout << std::endl;
    }

    std::cout << "\n\u2714 Active: ";
    for (const auto &table : tables) {
      if (table.status == "PERFECT MATCH" ||
          table.status == "LISTENING_CHANGES") {
        std::cout << table.schema_name << "." << table.table_name << "("
                  << table.last_offset << ") ";
      }
    }
    std::cout << std::endl;
  }

  void generateFullReport(pqxx::connection &pgConn) {
    auto tables = getAllTableStatuses(pgConn);
    auto stats = calculateSyncStats(tables);
    printCompactReport(tables, stats);
  }

  void generateFinalReport(pqxx::connection &pgConn) {
    auto tables = getAllTableStatuses(pgConn);
    auto stats = calculateSyncStats(tables);
    printFinalReport(tables, stats);
  }

  std::string getCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto tm = *std::localtime(&time_t);

    char buffer[16];
    std::strftime(buffer, sizeof(buffer), "%H:%M:%S", &tm);
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

    file << "DataSync Report - " << getCurrentTimestamp() << std::endl;
    file << "Total: " << stats.totalTables
         << " | Synced: " << stats.totalSynchronized << " | Success: "
         << (stats.totalTables > 0
                 ? (stats.totalSynchronized * 100 / stats.totalTables)
                 : 0)
         << "%" << std::endl;

    for (const auto &table : tables) {
      file << table.schema_name << "." << table.table_name << " | "
           << table.db_engine << " | " << table.status << " | "
           << table.last_offset << std::endl;
    }

    file.close();
  }
};

#endif // SYNCREPORTER_H
