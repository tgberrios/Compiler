#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "MariaDBToPostgres.h"
#include "PostgresToMariaDB.h"
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

    int minutes_counter = 0;

    mariaToPg.syncCatalogMariaDBToPostgres();
    mariaToPg.setupTableTargetMariaDBToPostgres();
    mariaToPg.transferDataMariaDBToPostgres();

    pgToMaria.syncCatalogPostgresToMariaDB();
    pgToMaria.setupTableTargetPostgresToMariaDB();
    pgToMaria.transferDataPostgresToMariaDB();

    while (true) {
      mariaToPg.transferDataMariaDBToPostgres();

      pgToMaria.transferDataPostgresToMariaDB();

      minutes_counter += 1;
      if (minutes_counter >= 2) {
        // mariaToPg.syncCatalogMariaDBToPostgres();
        mariaToPg.setupTableTargetMariaDBToPostgres();

        // pgToMaria.syncCatalogPostgresToMariaDB();

        minutes_counter = 0;
      }

      std::cout << "Pausando 30 segundos antes del siguiente delta load... ";
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