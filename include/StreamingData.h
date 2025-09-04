#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "Config.h"
#include "MSSQLToPostgres.h"
#include "MariaDBToPostgres.h"
#include "PostgresToMariaDB.h"
#include "SyncReporter.h"
#include "ConnectionManager.h"
#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <thread>
#include <vector>
#include <pqxx/pqxx>

class StreamingData {
public:
  StreamingData() = default;
  ~StreamingData() = default;

  void run() {
    MariaDBToPostgres mariaToPg;
    PostgresToMariaDB pgToMaria;
    MSSQLToPostgres mssqlToPg;
    SyncReporter reporter;

    int minutes_counter = 0;
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

    std::cout << "Starting streaming data... :)" << std::endl;

    mariaToPg.syncCatalogMariaDBToPostgres();
    mariaToPg.setupTableTargetMariaDBToPostgres();

    pgToMaria.syncCatalogPostgresToMariaDB();
    pgToMaria.setupTableTargetPostgresToMariaDB();

    mssqlToPg.syncCatalogMSSQLToPostgres();
    mssqlToPg.setupTableTargetMSSQLToPostgres();

    std::atomic<bool> stop_threads{false};

    // Ejecutar transferencias continuas sin interrupciones
    std::vector<std::future<void>> maria_futures;
    std::vector<std::future<void>> pg_futures;
    std::vector<std::future<void>> mssql_futures;

    // Lanzar UN SOLO thread por tipo de BD (sin paralelización múltiple)
    maria_futures.emplace_back(std::async(std::launch::async, [&mariaToPg]() {
      while (true) {
        mariaToPg.transferDataMariaDBToPostgres();
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval()));
      }
    }));

    pg_futures.emplace_back(std::async(std::launch::async, [&pgToMaria]() {
      while (true) {
        pgToMaria.transferDataPostgresToMariaDB();
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval()));
      }
    }));

    mssql_futures.emplace_back(std::async(std::launch::async, [&mssqlToPg]() {
      while (true) {
        mssqlToPg.transferDataMSSQLToPostgres();
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval()));
      }
    }));

    // Bucle principal solo para reporting y setup periódico
    while (true) {
      loadChunkSizeFromDatabase(pgConn);
      loadSyncIntervalFromDatabase(pgConn);
      reporter.generateFullReport(pgConn);

      minutes_counter += 1;
      if (minutes_counter >= 2) {
        mariaToPg.setupTableTargetMariaDBToPostgres();
        mssqlToPg.setupTableTargetMSSQLToPostgres();
        minutes_counter = 0;
      }

      std::this_thread::sleep_for(
          std::chrono::seconds(SyncConfig::getSyncInterval()));
    }
  }

private:
  void loadChunkSizeFromDatabase(pqxx::connection& pgConn) {
    try {
      ConnectionManager cm;
      auto results = cm.executeQueryPostgres(
          pgConn, "SELECT value FROM metadata.config WHERE key='chunk_size';");
      
      if (!results.empty() && !results[0][0].is_null()) {
        size_t newChunkSize = std::stoul(results[0][0].as<std::string>());
        if (newChunkSize > 0 && newChunkSize != SyncConfig::getChunkSize()) {
          SyncConfig::setChunkSize(newChunkSize);
          std::cout << "Chunk size updated to: " << newChunkSize << std::endl;
        }
      }
    } catch (const std::exception& e) {
      // Silently continue with current chunk size if config table doesn't exist
    }
  }

  void loadSyncIntervalFromDatabase(pqxx::connection& pgConn) {
    try {
      ConnectionManager cm;
      auto results = cm.executeQueryPostgres(
          pgConn, "SELECT value FROM metadata.config WHERE key='sync_interval';");
      
      if (!results.empty() && !results[0][0].is_null()) {
        size_t newSyncInterval = std::stoul(results[0][0].as<std::string>());
        if (newSyncInterval > 0 && newSyncInterval != SyncConfig::getSyncInterval()) {
          SyncConfig::setSyncInterval(newSyncInterval);
          std::cout << "Sync interval updated to: " << newSyncInterval << " seconds" << std::endl;
        }
      }
    } catch (const std::exception& e) {
      // Silently continue with current sync interval if config table doesn't exist
    }
  }
};

#endif // STREAMINGDATA_H