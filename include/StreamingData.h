#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "MSSQLToPostgres.h"
#include "MariaDBToPostgres.h"
#include "PostgresToMariaDB.h"
#include "SyncReporter.h"
#include <chrono>
#include <iostream>
#include <thread>

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
    pqxx::connection pgConn("host=localhost user=tomy.berrios "
                            "password=Yucaquemada1 dbname=DataLake");

    std::cout << "🚀 Starting DataSync System..." << std::endl;
    std::cout << "Initializing database connections..." << std::endl;

    mariaToPg.syncCatalogMariaDBToPostgres();
    mariaToPg.setupTableTargetMariaDBToPostgres();
    mariaToPg.transferDataMariaDBToPostgres();

    pgToMaria.syncCatalogPostgresToMariaDB();
    pgToMaria.setupTableTargetPostgresToMariaDB();
    pgToMaria.transferDataPostgresToMariaDB();

    mssqlToPg.syncCatalogMSSQLToPostgres();
    mssqlToPg.setupTableTargetMSSQLToPostgres();
    mssqlToPg.transferDataMSSQLToPostgres();

    std::cout << "✅ Initial sync completed. Starting continuous monitoring..."
              << std::endl;

    while (true) {
      mariaToPg.transferDataMariaDBToPostgres();
      pgToMaria.transferDataPostgresToMariaDB();
      mssqlToPg.transferDataMSSQLToPostgres();

      reporter.generateFullReport(pgConn);

      minutes_counter += 1;
      if (minutes_counter >= 2) {
        mariaToPg.setupTableTargetMariaDBToPostgres();
        mssqlToPg.setupTableTargetMSSQLToPostgres();
        minutes_counter = 0;
      }

      std::cout << " | Pausing 30s... ";
      const int total = 30;
      for (int i = 0; i <= total; ++i) {
        int progress = (i * 20) / total;
        std::cout << "\r[";
        for (int j = 0; j < 20; ++j) {
          if (j < progress)
            std::cout << "█";
          else
            std::cout << " ";
        }
        std::cout << "] " << (i * 100 / total) << "%";
        std::cout.flush();
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      std::cout << std::endl;
    }
  }
};

#endif // STREAMINGDATA_H