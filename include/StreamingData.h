#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "Config.h"
#include "MSSQLToPostgres.h"
#include "MariaDBToPostgres.h"
#include "PostgresToMariaDB.h"
#include "SyncReporter.h"
#include "catalog_clean.h"
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
    MariaDBToPostgres mariaToPg;
    PostgresToMariaDB pgToMaria;
    // MSSQLToPostgres mssqlToPg;
    SyncReporter reporter;

    int minutes_counter = 0;
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

    mariaToPg.syncCatalogMariaDBToPostgres();
    mariaToPg.setupTableTargetMariaDBToPostgres();

    pgToMaria.syncCatalogPostgresToMariaDB();
    pgToMaria.setupTableTargetPostgresToMariaDB();

    // mssqlToPg.syncCatalogMSSQLToPostgres();
    // mssqlToPg.setupTableTargetMSSQLToPostgres();

    std::atomic<bool> stop_threads{false};

    // Ejecutar transferencias continuas sin interrupciones
    std::vector<std::future<void>> maria_futures;
    std::vector<std::future<void>> pg_futures;
    // std::vector<std::future<void>> mssql_futures;

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

    // mssql_futures.emplace_back(std::async(std::launch::async, [&mssqlToPg]()
    // {
    //   while (true) {
    //     mssqlToPg.transferDataMSSQLToPostgres();
    //     std::this_thread::sleep_for(
    //         std::chrono::seconds(SyncConfig::getSyncInterval()));
    //   }
    // }));

    // Bucle principal solo para reporting y setup periódico
    while (true) {
      loadConfigFromDatabase(pgConn);
      reporter.generateFullReport(pgConn);

      minutes_counter += 1;
      if (minutes_counter >= 2) {
        mariaToPg.setupTableTargetMariaDBToPostgres();
        // mssqlToPg.setupTableTargetMSSQLToPostgres();

        // Limpiar catálogo cada 2 minutos
        CatalogClean cleaner;
        cleaner.cleanCatalog();

        minutes_counter = 0;
      }

      std::this_thread::sleep_for(
          std::chrono::seconds(SyncConfig::getSyncInterval()));
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
            SyncConfig::setChunkSize(newSize);
          }
        } else if (key == "sync_interval") {
          size_t newInterval = std::stoul(value);
          if (newInterval > 0 && newInterval != SyncConfig::getSyncInterval()) {
            SyncConfig::setSyncInterval(newInterval);
          }
        }
      }
    } catch (const std::exception &e) {
      // Continue with current config if table doesn't exist
    }
  }
};

#endif // STREAMINGDATA_H