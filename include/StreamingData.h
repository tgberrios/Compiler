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

    std::cout << "Starting streaming data... :)" << std::endl;

    mariaToPg.syncCatalogMariaDBToPostgres();
    mariaToPg.setupTableTargetMariaDBToPostgres();
    mariaToPg.transferDataMariaDBToPostgres();

    pgToMaria.syncCatalogPostgresToMariaDB();
    pgToMaria.setupTableTargetPostgresToMariaDB();
    pgToMaria.transferDataPostgresToMariaDB();

    mssqlToPg.syncCatalogMSSQLToPostgres();
    mssqlToPg.setupTableTargetMSSQLToPostgres();
    mssqlToPg.transferDataMSSQLToPostgres();

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

      std::this_thread::sleep_for(std::chrono::seconds(30));
    }
  }
};

#endif // STREAMINGDATA_H