#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "Config.h"
#include "DataGovernance.h"
#include "DDLExporter.h"
#include "MetricsCollector.h"
#include "MSSQLToPostgres.h"
#include "MariaDBToPostgres.h"
#include "MongoToPostgres.h"
#include "PostgresToPostgres.h"
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

  void initialize() {
    //Logger::info("StreamingData", "Initializing DataSync system components");
    
    DataGovernance dg;
    dg.initialize();
    dg.runDiscovery();
    dg.generateReport();
    //Logger::info("StreamingData", "Data Governance initialization completed");

    DDLExporter ddlExporter;
    ddlExporter.exportAllDDL();
    //Logger::info("StreamingData", "DDL Export completed");

    MetricsCollector metricsCollector;
    metricsCollector.collectAllMetrics();
    Logger::info("StreamingData", "Metrics Collection completed");
    
    Logger::info("StreamingData", "System initialization completed");
  }

  void run() {
    Logger::info("StreamingData", "Starting run() method");
    Logger::info("StreamingData", "DataSync system started");

    Logger::info("StreamingData", "Creating transfer objects");
    MariaDBToPostgres mariaToPg;
    MSSQLToPostgres mssqlToPg;
    PostgresToPostgres pgToPg;
    MongoToPostgres mongoToPg;
    SyncReporter reporter;
    CatalogManager catalogManager;

    int minutes_counter = 0;
    Logger::info("StreamingData", "Establishing PostgreSQL connection");
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
    Logger::info("StreamingData", "PostgreSQL connection established");

    Logger::info("StreamingData",
                 "Starting MariaDB -> PostgreSQL catalog synchronization");
    try {
      catalogManager.syncCatalogMariaDBToPostgres();
      Logger::info("StreamingData", "MariaDB catalog sync completed");
    } catch (const std::exception &e) {
      Logger::error("StreamingData", "MariaDB catalog sync failed: " + std::string(e.what()));
    }
    
    Logger::info("StreamingData",
                 "Setting up target tables MariaDB -> PostgreSQL");
    try {
      mariaToPg.setupTableTargetMariaDBToPostgres();
      Logger::info("StreamingData", "MariaDB target tables setup completed");
    } catch (const std::exception &e) {
      Logger::error("StreamingData", "MariaDB target tables setup failed: " + std::string(e.what()));
    }

    Logger::info("StreamingData",
                 "Starting MSSQL -> PostgreSQL catalog synchronization");
    catalogManager.syncCatalogMSSQLToPostgres();
    Logger::info("StreamingData",
                 "Setting up target tables MSSQL -> PostgreSQL");
    mssqlToPg.setupTableTargetMSSQLToPostgres();

    Logger::info("StreamingData",
                 "Starting PostgreSQL -> PostgreSQL catalog synchronization");
    catalogManager.syncCatalogPostgresToPostgres();
    Logger::info("StreamingData",
                 "Setting up target tables PostgreSQL -> PostgreSQL");
    pgToPg.setupTableTargetPostgresToPostgres();

    Logger::info("StreamingData",
                 "Starting MongoDB -> PostgreSQL catalog synchronization");
    catalogManager.syncCatalogMongoToPostgres();
    Logger::info("StreamingData",
                 "Setting up target tables MongoDB -> PostgreSQL");
    mongoToPg.setupTableTargetMongoToPostgres();

    // Ejecutar transferencias de forma secuencial para evitar conflictos de
    // concurrencia
    Logger::info("StreamingData", "Starting sequential data transfer process");

    // Bucle principal con transferencias secuenciales
    Logger::info("StreamingData", "Starting main monitoring loop");
    Logger::info("StreamingData", "Entering infinite loop for data transfers");
    while (true) {
      try {
        //Logger::debug("StreamingData", "Loading configuration from database");
        loadConfigFromDatabase(pgConn);

        // Ejecutar transferencias de forma secuencial para evitar conflictos
        Logger::info("StreamingData",
                      "Executing MariaDB -> PostgreSQL transfer");
        try {
          mariaToPg.transferDataMariaDBToPostgres();
          Logger::info("MariaDBToPostgres", "Transfer completed successfully");
        } catch (const std::exception &e) {
          Logger::error("MariaDBToPostgres",
                        "Transfer error: " + std::string(e.what()));
        } catch (...) {
          Logger::error("MariaDBToPostgres", "Unknown error during transfer");
        }

        //Logger::debug("StreamingData",
        //              "Executing MSSQL -> PostgreSQL transfer");
        try {
          mssqlToPg.transferDataMSSQLToPostgres();
        } catch (const std::exception &e) {
          Logger::error("MSSQLToPostgres",
                        "Transfer error: " + std::string(e.what()));
        }

        //Logger::debug("StreamingData",
        //              "Executing PostgreSQL -> PostgreSQL transfer");
        try {
          pgToPg.transferDataPostgresToPostgres();
        } catch (const std::exception &e) {
          Logger::error("PostgresToPostgres",
                        "Transfer error: " + std::string(e.what()));
        }

        //Logger::debug("StreamingData",
        //              "Executing MongoDB -> PostgreSQL transfer");
        try {
          mongoToPg.transferDataMongoToPostgres();
        } catch (const std::exception &e) {
          Logger::error("MongoToPostgres",
                        "Transfer error: " + std::string(e.what()));
        }

        //Logger::debug("StreamingData", "Generating full report");
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
              "Starting PostgreSQL -> PostgreSQL catalog synchronization");
          catalogManager.syncCatalogPostgresToPostgres();

          Logger::debug(
              "StreamingData",
              "Starting MongoDB -> PostgreSQL catalog synchronization");
          catalogManager.syncCatalogMongoToPostgres();

          //Logger::debug("StreamingData", "Cleaning catalog");
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