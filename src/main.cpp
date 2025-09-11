#include "StreamingData.h"
#include <iostream>
#include <mongoc/mongoc.h>

int main() {
  Logger::initialize();
  Logger::info("MAIN", "Starting DataLake Synchronizer System :) ");

  mongoc_init();

  StreamingData sd;
  sd.initialize();
  sd.run();

  mongoc_cleanup();
  return 0;
}
