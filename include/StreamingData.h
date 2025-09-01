#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "Config.h"
#include "MSSQLToPostgres.h"
#include "MariaDBToPostgres.h"
#include "PostgresToMariaDB.h"
#include "SyncReporter.h"
#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <thread>
#include <vector>

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
            std::chrono::seconds(SyncConfig::SYNC_INTERVAL_SECONDS));
      }
    }));

    pg_futures.emplace_back(std::async(std::launch::async, [&pgToMaria]() {
      while (true) {
        pgToMaria.transferDataPostgresToMariaDB();
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::SYNC_INTERVAL_SECONDS));
      }
    }));

    mssql_futures.emplace_back(std::async(std::launch::async, [&mssqlToPg]() {
      while (true) {
        mssqlToPg.transferDataMSSQLToPostgres();
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::SYNC_INTERVAL_SECONDS));
      }
    }));

    // Bucle principal solo para reporting y setup periódico
    while (true) {
      reporter.generateFullReport(pgConn);

      minutes_counter += 1;
      if (minutes_counter >= 2) {
        mariaToPg.setupTableTargetMariaDBToPostgres();
        mssqlToPg.setupTableTargetMSSQLToPostgres();
        minutes_counter = 0;
      }

      std::this_thread::sleep_for(
          std::chrono::seconds(SyncConfig::SYNC_INTERVAL_SECONDS));
    }
  }
};

#endif // STREAMINGDATA_H