#include "catalog/catalog_cleaner.h"
#include "core/Config.h"
#include "core/logger.h"
#include "engines/mongodb_engine.h"
#include "engines/oracle_engine.h"
#include "engines/postgres_engine.h"
#include "utils/connection_utils.h"
#include "utils/string_utils.h"
#include "utils/table_utils.h"
#include <algorithm>
#include <set>

CatalogCleaner::CatalogCleaner(std::string metadataConnStr)
    : metadataConnStr_(std::move(metadataConnStr)),
      repo_(std::make_unique<MetadataRepository>(metadataConnStr_)) {}

template <typename EngineType>
size_t
CatalogCleaner::cleanNonExistentTablesForEngine(const std::string &dbEngine) {
  size_t totalDeleted = 0;
  auto connStrings = repo_->getConnectionStrings(dbEngine);

  for (const auto &connStr : connStrings) {
    try {
      EngineType engine(connStr);
      auto existingTables = engine.discoverTables();

      std::set<std::pair<std::string, std::string>> existingSet;
      for (const auto &table : existingTables) {
        existingSet.insert({table.schema, table.table});
      }

      auto catalogEntries = repo_->getCatalogEntries(dbEngine, connStr);
      for (const auto &entry : catalogEntries) {
        if (existingSet.find({entry.schema, entry.table}) ==
            existingSet.end()) {
          repo_->deleteTable(entry.schema, entry.table, dbEngine, connStr,
                             true);
          totalDeleted++;
        }
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                    "Error checking " + dbEngine +
                        " connection: " + std::string(e.what()));
    }
  }

  return totalDeleted;
}

void CatalogCleaner::cleanNonExistentPostgresTables() {
  try {
    size_t totalDeleted =
        cleanNonExistentTablesForEngine<PostgreSQLEngine>("PostgreSQL");
    if (totalDeleted > 0) {
      Logger::info(
          LogCategory::DATABASE, "CatalogCleaner",
          "PostgreSQL cleanup completed: " + std::to_string(totalDeleted) +
              " non-existent tables removed");
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning PostgreSQL tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanNonExistentMariaDBTables() {
  try {
    size_t totalDeleted =
        cleanNonExistentTablesForEngine<MariaDBEngine>("MariaDB");
    if (totalDeleted > 0) {
      Logger::info(
          LogCategory::DATABASE, "CatalogCleaner",
          "MariaDB cleanup completed: " + std::to_string(totalDeleted) +
              " non-existent tables removed");
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning MariaDB tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanNonExistentMSSQLTables() {
  try {
    size_t totalDeleted = cleanNonExistentTablesForEngine<MSSQLEngine>("MSSQL");
    if (totalDeleted > 0) {
      Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                   "MSSQL cleanup completed: " + std::to_string(totalDeleted) +
                       " non-existent tables removed");
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning MSSQL tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanNonExistentOracleTables() {
  try {
    size_t totalDeleted =
        cleanNonExistentTablesForEngine<OracleEngine>("Oracle");
    if (totalDeleted > 0) {
      Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                   "Oracle cleanup completed: " + std::to_string(totalDeleted) +
                       " non-existent tables removed");
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning Oracle tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanNonExistentMongoDBTables() {
  try {
    size_t totalDeleted = 0;
    auto connStrings = repo_->getConnectionStrings("MongoDB");

    for (const auto &connStr : connStrings) {
      if (!isMongoConnectionStringValid(connStr)) {
        Logger::warning(LogCategory::DATABASE, "CatalogCleaner",
                       "Invalid MongoDB connection string: " +
                       StringUtils::sanitizeConnectionStringForLog(connStr, 50) +
                       ". Skipping cleanup.");
        continue;
      }

      try {
        MongoDBEngine engine(connStr);
        
        std::vector<CatalogTableInfo> existingTables;
        if (engine.isValid()) {
          try {
            existingTables = engine.discoverAllDatabasesAndCollections();
          } catch (const std::exception &e) {
            Logger::warning(LogCategory::DATABASE, "CatalogCleaner",
                           "Failed to discover all MongoDB databases for connection: " +
                           StringUtils::sanitizeConnectionStringForLog(connStr, 50) + ". Error: " + std::string(e.what()) +
                           ". Skipping cleanup for this connection to prevent data loss.");
            continue;
          }
        } else {
          Logger::warning(LogCategory::DATABASE, "CatalogCleaner",
                         "MongoDB engine invalid for connection: " +
                         StringUtils::sanitizeConnectionStringForLog(connStr, 50) +
                         ". Skipping cleanup to prevent data loss.");
          continue;
        }

        std::set<std::pair<std::string, std::string>> existingSet;
        for (const auto &table : existingTables) {
          existingSet.insert({table.schema, table.table});
        }

        auto catalogEntries = repo_->getCatalogEntries("MongoDB", connStr);
        for (const auto &entry : catalogEntries) {
          if (existingSet.find({entry.schema, entry.table}) ==
              existingSet.end()) {
            repo_->deleteTable(entry.schema, entry.table, "MongoDB", connStr,
                             true);
            totalDeleted++;
          }
        }
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                     "Error checking MongoDB connection: " + std::string(e.what()) +
                     ". Skipping cleanup for this connection to prevent data loss.");
      }
    }

    if (totalDeleted > 0) {
      Logger::info(
          LogCategory::DATABASE, "CatalogCleaner",
          "MongoDB cleanup completed: " + std::to_string(totalDeleted) +
              " non-existent tables removed");
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                 "Error cleaning MongoDB tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanOrphanedTables() {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    auto result1 =
        txn.exec("DELETE FROM metadata.catalog "
                 "WHERE connection_string IS NULL OR connection_string = ''");
    size_t deleted1 = result1.affected_rows();

    auto result2 = txn.exec("DELETE FROM metadata.catalog "
                            "WHERE db_engine NOT IN ('PostgreSQL', 'MariaDB', "
                            "'MSSQL', 'MongoDB', 'Oracle')");
    size_t deleted2 = result2.affected_rows();

    auto result3 = txn.exec("DELETE FROM metadata.catalog "
                            "WHERE schema_name IS NULL OR schema_name = '' "
                            "OR table_name IS NULL OR table_name = ''");
    size_t deleted3 = result3.affected_rows();

    txn.commit();

    size_t totalDeleted = deleted1 + deleted2 + deleted3;
    if (totalDeleted > 0) {
      Logger::info(
          LogCategory::DATABASE, "CatalogCleaner",
          "Orphaned tables cleanup completed: " + std::to_string(totalDeleted) +
              " entries removed (empty_conn=" + std::to_string(deleted1) +
              ", invalid_engine=" + std::to_string(deleted2) +
              ", invalid_names=" + std::to_string(deleted3) + ")");
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning orphaned tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanOldLogs(int retentionHours) {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    auto countBefore = txn.exec("SELECT COUNT(*) FROM metadata.logs");
    int logsBefore = countBefore[0][0].as<int>();

    if (logsBefore == 0) {
      txn.commit();
      return;
    }

    auto result = txn.exec_params("DELETE FROM metadata.logs WHERE ts < NOW() "
                                  "- make_interval(hours => $1)",
                                  retentionHours);
    int deletedLogs = result.affected_rows();

    auto countAfter = txn.exec("SELECT COUNT(*) FROM metadata.logs");
    int logsAfter = countAfter[0][0].as<int>();

    txn.commit();

    Logger::info(LogCategory::MAINTENANCE, "cleanOldLogs",
                 "Deleted " + std::to_string(deletedLogs) + " old logs (" +
                     std::to_string(retentionHours) +
                     "h retention). Remaining: " + std::to_string(logsAfter));
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning old logs: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanOrphanedGovernanceData() {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    auto countBefore1 = txn.exec("SELECT COUNT(*) FROM metadata.data_governance_catalog");
    int recordsBefore1 = countBefore1[0][0].as<int>();

    if (recordsBefore1 == 0) {
      // Skip if no data
    } else {
      auto result1 = txn.exec("DELETE FROM metadata.data_governance_catalog "
                              "WHERE snapshot_date < NOW() - INTERVAL '8 months'");
      size_t deleted1 = result1.affected_rows();
      auto countAfter1 = txn.exec("SELECT COUNT(*) FROM metadata.data_governance_catalog");
      int recordsAfter1 = countAfter1[0][0].as<int>();

      if (deleted1 > 0) {
        Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                     "Deleted " + std::to_string(deleted1) +
                         " old governance data records (8 months retention). "
                         "Remaining: " + std::to_string(recordsAfter1));
      }
    }

    auto countBefore2 = txn.exec("SELECT COUNT(*) FROM metadata.data_governance_catalog_mariadb");
    int recordsBefore2 = countBefore2[0][0].as<int>();

    if (recordsBefore2 > 0) {
      auto result2 = txn.exec("DELETE FROM metadata.data_governance_catalog_mariadb "
                              "WHERE snapshot_date < NOW() - INTERVAL '8 months'");
      size_t deleted2 = result2.affected_rows();
      auto countAfter2 = txn.exec("SELECT COUNT(*) FROM metadata.data_governance_catalog_mariadb");
      int recordsAfter2 = countAfter2[0][0].as<int>();

      if (deleted2 > 0) {
        Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                     "Deleted " + std::to_string(deleted2) +
                         " old MariaDB governance data records (8 months retention). "
                         "Remaining: " + std::to_string(recordsAfter2));
      }
    }

    auto countBefore3 = txn.exec("SELECT COUNT(*) FROM metadata.data_governance_catalog_mssql");
    int recordsBefore3 = countBefore3[0][0].as<int>();

    if (recordsBefore3 > 0) {
      auto result3 = txn.exec("DELETE FROM metadata.data_governance_catalog_mssql "
                              "WHERE snapshot_date < NOW() - INTERVAL '8 months'");
      size_t deleted3 = result3.affected_rows();
      auto countAfter3 = txn.exec("SELECT COUNT(*) FROM metadata.data_governance_catalog_mssql");
      int recordsAfter3 = countAfter3[0][0].as<int>();

      if (deleted3 > 0) {
        Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                     "Deleted " + std::to_string(deleted3) +
                         " old MSSQL governance data records (8 months retention). "
                         "Remaining: " + std::to_string(recordsAfter3));
      }
    }

    auto countBefore4 = txn.exec("SELECT COUNT(*) FROM metadata.data_governance_catalog_mongodb");
    int recordsBefore4 = countBefore4[0][0].as<int>();

    if (recordsBefore4 > 0) {
      auto result4 = txn.exec("DELETE FROM metadata.data_governance_catalog_mongodb "
                              "WHERE snapshot_date < NOW() - INTERVAL '8 months'");
      size_t deleted4 = result4.affected_rows();
      auto countAfter4 = txn.exec("SELECT COUNT(*) FROM metadata.data_governance_catalog_mongodb");
      int recordsAfter4 = countAfter4[0][0].as<int>();

      if (deleted4 > 0) {
        Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                     "Deleted " + std::to_string(deleted4) +
                         " old MongoDB governance data records (8 months retention). "
                         "Remaining: " + std::to_string(recordsAfter4));
      }
    }

    auto countBefore5 = txn.exec("SELECT COUNT(*) FROM metadata.data_governance_catalog_oracle");
    int recordsBefore5 = countBefore5[0][0].as<int>();

    if (recordsBefore5 > 0) {
      auto result5 = txn.exec("DELETE FROM metadata.data_governance_catalog_oracle "
                              "WHERE snapshot_date < NOW() - INTERVAL '8 months'");
      size_t deleted5 = result5.affected_rows();
      auto countAfter5 = txn.exec("SELECT COUNT(*) FROM metadata.data_governance_catalog_oracle");
      int recordsAfter5 = countAfter5[0][0].as<int>();

      if (deleted5 > 0) {
        Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                     "Deleted " + std::to_string(deleted5) +
                         " old Oracle governance data records (8 months retention). "
                         "Remaining: " + std::to_string(recordsAfter5));
      }
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning old governance data: " +
                      std::string(e.what()));
  }
}

void CatalogCleaner::cleanOrphanedQualityData() {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    auto countBefore = txn.exec("SELECT COUNT(*) FROM metadata.data_quality");
    int recordsBefore = countBefore[0][0].as<int>();

    if (recordsBefore == 0) {
      txn.commit();
      return;
    }

    auto result = txn.exec("DELETE FROM metadata.data_quality "
                           "WHERE check_timestamp < NOW() - INTERVAL '8 months'");
    int deletedRecords = result.affected_rows();

    auto countAfter = txn.exec("SELECT COUNT(*) FROM metadata.data_quality");
    int recordsAfter = countAfter[0][0].as<int>();

    txn.commit();

    if (deletedRecords > 0) {
      Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                   "Deleted " + std::to_string(deletedRecords) +
                       " old quality data records (8 months retention). "
                       "Remaining: " + std::to_string(recordsAfter));
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning old quality data: " +
                      std::string(e.what()));
  }
}

void CatalogCleaner::cleanOrphanedMaintenanceData() {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    std::string deleteMaintenance = R"(
      DELETE FROM metadata.maintenance_control
      WHERE (schema_name, object_name) NOT IN (
        SELECT schema_name, table_name FROM metadata.catalog
      )
    )";
    auto result = txn.exec(deleteMaintenance);
    size_t deleted = result.affected_rows();

    txn.commit();

    if (deleted > 0) {
      Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                   "Orphaned maintenance data cleanup completed: " +
                       std::to_string(deleted) + " entries removed");
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning orphaned maintenance data: " +
                      std::string(e.what()));
  }
}

void CatalogCleaner::cleanOrphanedLineageData() {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    auto countBefore1 = txn.exec("SELECT COUNT(*) FROM metadata.mdb_lineage");
    int recordsBefore1 = countBefore1[0][0].as<int>();

    if (recordsBefore1 > 0) {
      auto result1 = txn.exec("DELETE FROM metadata.mdb_lineage "
                              "WHERE last_seen_at < NOW() - INTERVAL '8 months'");
      size_t deleted1 = result1.affected_rows();
      auto countAfter1 = txn.exec("SELECT COUNT(*) FROM metadata.mdb_lineage");
      int recordsAfter1 = countAfter1[0][0].as<int>();

      if (deleted1 > 0) {
        Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                     "Deleted " + std::to_string(deleted1) +
                         " old MariaDB lineage data records (8 months retention). "
                         "Remaining: " + std::to_string(recordsAfter1));
      }
    }

    auto countBefore2 = txn.exec("SELECT COUNT(*) FROM metadata.mssql_lineage");
    int recordsBefore2 = countBefore2[0][0].as<int>();

    if (recordsBefore2 > 0) {
      auto result2 = txn.exec("DELETE FROM metadata.mssql_lineage "
                              "WHERE last_seen_at < NOW() - INTERVAL '8 months'");
      size_t deleted2 = result2.affected_rows();
      auto countAfter2 = txn.exec("SELECT COUNT(*) FROM metadata.mssql_lineage");
      int recordsAfter2 = countAfter2[0][0].as<int>();

      if (deleted2 > 0) {
        Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                     "Deleted " + std::to_string(deleted2) +
                         " old MSSQL lineage data records (8 months retention). "
                         "Remaining: " + std::to_string(recordsAfter2));
      }
    }

    auto countBefore3 = txn.exec("SELECT COUNT(*) FROM metadata.mongo_lineage");
    int recordsBefore3 = countBefore3[0][0].as<int>();

    if (recordsBefore3 > 0) {
      // mongo_lineage has snapshot_date
      auto result3 = txn.exec("DELETE FROM metadata.mongo_lineage "
                              "WHERE COALESCE(snapshot_date, last_seen_at) < NOW() - INTERVAL '8 months'");
      size_t deleted3 = result3.affected_rows();
      auto countAfter3 = txn.exec("SELECT COUNT(*) FROM metadata.mongo_lineage");
      int recordsAfter3 = countAfter3[0][0].as<int>();

      if (deleted3 > 0) {
        Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                     "Deleted " + std::to_string(deleted3) +
                         " old MongoDB lineage data records (8 months retention). "
                         "Remaining: " + std::to_string(recordsAfter3));
      }
    }

    auto countBefore4 = txn.exec("SELECT COUNT(*) FROM metadata.oracle_lineage");
    int recordsBefore4 = countBefore4[0][0].as<int>();

    if (recordsBefore4 > 0) {
      auto result4 = txn.exec("DELETE FROM metadata.oracle_lineage "
                              "WHERE last_seen_at < NOW() - INTERVAL '8 months'");
      size_t deleted4 = result4.affected_rows();
      auto countAfter4 = txn.exec("SELECT COUNT(*) FROM metadata.oracle_lineage");
      int recordsAfter4 = countAfter4[0][0].as<int>();

      if (deleted4 > 0) {
        Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                     "Deleted " + std::to_string(deleted4) +
                         " old Oracle lineage data records (8 months retention). "
                         "Remaining: " + std::to_string(recordsAfter4));
      }
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning old lineage data: " +
                      std::string(e.what()));
  }
}

template size_t
CatalogCleaner::cleanNonExistentTablesForEngine<PostgreSQLEngine>(
    const std::string &);
template size_t CatalogCleaner::cleanNonExistentTablesForEngine<MariaDBEngine>(
    const std::string &);
template size_t CatalogCleaner::cleanNonExistentTablesForEngine<MSSQLEngine>(
    const std::string &);
template size_t CatalogCleaner::cleanNonExistentTablesForEngine<OracleEngine>(
    const std::string &);
template size_t CatalogCleaner::cleanNonExistentTablesForEngine<MongoDBEngine>(
    const std::string &);
