#include "DDLExporter.h"
#include "DataGovernance.h"
#include "StreamingData.h"
#include <chrono>
#include <iostream>
#include <thread>

int main() {
  Logger::initialize();
  Logger::info("main",
               "Starting DataSync system with Data Governance and DDL Export");

  DataGovernance dg;
  dg.initialize();
  dg.runDiscovery();
  dg.generateReport();

  DDLExporter ddlExporter;
  ddlExporter.exportAllDDL();

  StreamingData sd;
  sd.run();
  return 0;
}
