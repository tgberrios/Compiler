#include "DDLExporter.h"
#include "DataGovernance.h"
#include "MetricsCollector.h"
#include "StreamingData.h"
#include <chrono>
#include <iostream>
#include <thread>

int main() {
  Logger::initialize();
  Logger::info("main", "Starting DataSync system with Data Governance, DDL "
                       "Export and Metrics Collection");

  DataGovernance dg;
  dg.initialize();
  dg.runDiscovery();
  dg.generateReport();

  DDLExporter ddlExporter;
  ddlExporter.exportAllDDL();

  MetricsCollector metricsCollector;
  metricsCollector.collectAllMetrics();

  StreamingData sd;
  sd.run();
  return 0;
}
