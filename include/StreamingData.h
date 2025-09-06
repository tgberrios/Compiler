#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "Config.h"
#include "MSSQLToPostgres.h"
#include "MariaDBToPostgres.h"
#include "PostgresToMariaDB.h"
#include "SyncReporter.h"
#include "catalog_manager.h"
#include "logger.h"
#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <pqxx/pqxx>
#include <thread>
#include <vector>

class StreamingData {
public:
  StreamingData() = default;
  ~StreamingData() = default;

  void run() {
    Logger::initialize();
    Logger::info("StreamingData", "DataSync system started");

    MariaDBToPostgres mariaToPg;
    PostgresToMariaDB pgToMaria;
    MSSQLToPostgres mssqlToPg;
    SyncReporter reporter;
    CatalogManager catalogManager;

    int minutes_counter = 0;
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
    Logger::info("StreamingData", "PostgreSQL connection established");

    Logger::info("StreamingData",
                 "Starting MariaDB -> PostgreSQL catalog synchronization");
    catalogManager.syncCatalogMariaDBToPostgres();
    Logger::info("StreamingData",
                 "Setting up target tables MariaDB -> PostgreSQL");
    mariaToPg.setupTableTargetMariaDBToPostgres();

    Logger::info("StreamingData",
                 "Starting PostgreSQL -> MariaDB catalog synchronization");
    catalogManager.syncCatalogPostgresToMariaDB();
    Logger::info("StreamingData",
                 "Setting up target tables PostgreSQL -> MariaDB");
    pgToMaria.setupTableTargetPostgresToMariaDB();

    Logger::info("StreamingData",
                 "Starting MSSQL -> PostgreSQL catalog synchronization");
    catalogManager.syncCatalogMSSQLToPostgres();
    Logger::info("StreamingData",
                 "Setting up target tables MSSQL -> PostgreSQL");
    mssqlToPg.setupTableTargetMSSQLToPostgres();

    std::atomic<bool> stop_threads{false};

    // Ejecutar transferencias continuas sin interrupciones
    std::vector<std::future<void>> maria_futures;
    std::vector<std::future<void>> pg_futures;
    std::vector<std::future<void>> mssql_futures;

    // Lanzar UN SOLO thread por tipo de BD (sin paralelización múltiple)
    Logger::info("StreamingData",
                 "Starting MariaDB -> PostgreSQL transfer thread");
    maria_futures.emplace_back(std::async(std::launch::async, [&mariaToPg]() {
      Logger::info("MariaDBToPostgres", "Transfer thread started");
      while (true) {
        try {
          mariaToPg.transferDataMariaDBToPostgres();
        } catch (const std::exception &e) {
          Logger::error("MariaDBToPostgres",
                        "Transfer error: " + std::string(e.what()));
        }
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval()));
      }
    }));

    Logger::info("StreamingData",
                 "Starting PostgreSQL -> MariaDB transfer thread");
    pg_futures.emplace_back(std::async(std::launch::async, [&pgToMaria]() {
      Logger::info("PostgresToMariaDB", "Transfer thread started");
      while (true) {
        try {
          pgToMaria.transferDataPostgresToMariaDB();
        } catch (const std::exception &e) {
          Logger::error("PostgresToMariaDB",
                        "Transfer error: " + std::string(e.what()));
        }
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval()));
      }
    }));

    Logger::info("StreamingData",
                 "Starting MSSQL -> PostgreSQL transfer thread");
    mssql_futures.emplace_back(std::async(std::launch::async, [&mssqlToPg]() {
      Logger::info("MSSQLToPostgres", "Transfer thread started");
      while (true) {
        try {
          mssqlToPg.transferDataMSSQLToPostgres();
        } catch (const std::exception &e) {
          Logger::error("MSSQLToPostgres",
                        "Transfer error: " + std::string(e.what()));
        }
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval()));
      }
    }));

    // Bucle principal solo para reporting y setup periódico
    Logger::info("StreamingData", "Starting main monitoring loop");
    while (true) {
      try {
        Logger::debug("StreamingData", "Loading configuration from database");
        loadConfigFromDatabase(pgConn);

        Logger::debug("StreamingData", "Generating full report");
        reporter.generateFullReport(pgConn);

        minutes_counter += 1;
        if (minutes_counter >= 2) {
          Logger::info("StreamingData",
                       "Running periodic maintenance (every 2 minutes)");
          Logger::debug("StreamingData",
                        "Setting up target tables MariaDB -> PostgreSQL");
          mariaToPg.setupTableTargetMariaDBToPostgres();

          Logger::debug("StreamingData",
                        "Starting MSSQL -> PostgreSQL catalog synchronization");
          catalogManager.syncCatalogMSSQLToPostgres();

          Logger::debug(
              "StreamingData",
              "Starting PostgreSQL -> MariaDB catalog synchronization");
          catalogManager.syncCatalogPostgresToMariaDB();

          Logger::debug("StreamingData", "Cleaning catalog");
          catalogManager.cleanCatalog();

          minutes_counter = 0;
          Logger::info("StreamingData", "Periodic maintenance completed");
        }

        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval()));
      } catch (const std::exception &e) {
        Logger::error("StreamingData",
                      "Main loop error: " + std::string(e.what()));
        std::this_thread::sleep_for(std::chrono::seconds(5));
      }
    }
  }

private:
  void loadConfigFromDatabase(pqxx::connection &pgConn) {
    try {
      pqxx::work txn(pgConn);
      auto results =
          txn.exec("SELECT key, value FROM metadata.config WHERE key IN "
                   "('chunk_size', 'sync_interval');");
      txn.commit();

      for (const auto &row : results) {
        if (row.size() < 2)
          continue;
        std::string key = row[0].as<std::string>();
        std::string value = row[1].as<std::string>();

        if (key == "chunk_size") {
          size_t newSize = std::stoul(value);
          if (newSize > 0 && newSize != SyncConfig::getChunkSize()) {
            Logger::info("loadConfigFromDatabase",
                         "Updating chunk_size from " +
                             std::to_string(SyncConfig::getChunkSize()) +
                             " to " + std::to_string(newSize));
            SyncConfig::setChunkSize(newSize);
          }
        } else if (key == "sync_interval") {
          size_t newInterval = std::stoul(value);
          if (newInterval > 0 && newInterval != SyncConfig::getSyncInterval()) {
            Logger::info("loadConfigFromDatabase",
                         "Updating sync_interval from " +
                             std::to_string(SyncConfig::getSyncInterval()) +
                             " to " + std::to_string(newInterval));
            SyncConfig::setSyncInterval(newInterval);
          }
        }
      }
    } catch (const std::exception &e) {
      Logger::warning("loadConfigFromDatabase",
                      "Could not load configuration: " + std::string(e.what()));
    }
  }
};

#endif // STREAMINGDATA_H