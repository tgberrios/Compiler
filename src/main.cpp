#include "DataGovernance.h"
#include "StreamingData.h"
#include <chrono>
#include <iostream>
#include <thread>

int main() {
  Logger::initialize();
  Logger::info("main", "Starting DataSync system with Data Governance");

  DataGovernance dg;
  dg.initialize();
  dg.runDiscovery();
  dg.generateReport();

  StreamingData sd;
  sd.run();
  return 0;
}
