#include "metrics/MetricsCollector.h"
#include "engines/database_engine.h"
#include "utils/string_utils.h"
#include "utils/time_utils.h"
#include <chrono>
#include <ctime>
#include <iomanip>
#include <pqxx/pqxx>
#include <sstream>
#include <unordered_map>
#ifdef _WIN32
#include <errno.h>
#endif

// Main entry point for collecting all transfer metrics. Orchestrates the
// complete metrics collection process by calling all collection methods in
// sequence: creates the metrics table, collects transfer metrics, performance
// metrics, metadata metrics, timestamp metrics, saves everything to the
// database, and generates a report. If any step fails, logs an error but
// continues with remaining steps. This function should be called periodically
// to maintain up-to-date metrics.
void MetricsCollector::collectAllMetrics() {

  try {
    createMetricsTable();
    collectTransferMetrics();
    collectPerformanceMetrics();
    collectMetadataMetrics();
    collectTimestampMetrics();
    // Ensure every metric has started_at and completed_at so the dashboard can compute throughput.
    // collectTimestampMetrics() only fills them for MongoDB (mongo_last_sync_time); for MSSQL/MariaDB
    // we use current time as completed_at and estimate started_at as 1 hour earlier.
    for (auto &metric : metrics) {
      if (metric.completed_at.empty()) {
        metric.completed_at = TimeUtils::getCurrentTimestamp();
        metric.started_at = getEstimatedStartTime(metric.completed_at);
      } else if (metric.started_at.empty()) {
        metric.started_at = getEstimatedStartTime(metric.completed_at);
      }
    }
    saveMetricsToDatabase();
    generateMetricsReport();

  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "collectAllMetrics",
                  "Error in metrics collection: " + std::string(e.what()));
  }
}

// Creates the metadata.transfer_metrics table and required indexes if they do
// not already exist. The table stores comprehensive transfer metrics including
// records transferred, bytes transferred, memory usage, I/O operations,
// transfer type, status, error messages, and timestamps. Sets connection
// timeouts (30s statement, 10s lock) to prevent long-running operations from
// hanging. Creates indexes on schema/table, db_engine, and status for efficient
// querying. The table has a unique constraint on (schema_name, table_name,
// db_engine, created_date) to prevent duplicate entries per day.
void MetricsCollector::createMetricsTable() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());

    conn.set_session_var("statement_timeout", "30000");
    conn.set_session_var("lock_timeout", "10000");

    if (!conn.is_open()) {
      Logger::error(LogCategory::METRICS, "createMetricsTable",
                    "Failed to connect to database");
      return;
    }

    pqxx::work txn(conn);

    // Create table with proper formatting and validation
    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS metadata.transfer_metrics ("
        "id SERIAL PRIMARY KEY,"
        "schema_name VARCHAR(100) NOT NULL,"
        "table_name VARCHAR(100) NOT NULL,"
        "db_engine VARCHAR(50) NOT NULL,"
        "records_transferred BIGINT DEFAULT 0,"
        "bytes_transferred BIGINT DEFAULT 0,"
        "memory_used_mb DECIMAL(15,2) DEFAULT 0.0,"
        "io_operations_per_second INTEGER DEFAULT 0,"
        "transfer_type VARCHAR(20) DEFAULT 'UNKNOWN',"
        "status VARCHAR(20) DEFAULT 'PENDING',"
        "error_message TEXT,"
        "started_at TIMESTAMP,"
        "completed_at TIMESTAMP,"
        "created_at TIMESTAMP DEFAULT NOW(),"
        "created_date DATE GENERATED ALWAYS AS (created_at::DATE) STORED,"
        "CONSTRAINT unique_table_metrics UNIQUE (schema_name, table_name, "
        "db_engine, created_date)"
        ");";

    txn.exec(createTableSQL);

    std::string createIndexesSQL =
        "CREATE INDEX IF NOT EXISTS idx_transfer_metrics_schema_table "
        "ON metadata.transfer_metrics (schema_name, table_name);"
        "CREATE INDEX IF NOT EXISTS idx_transfer_metrics_db_engine "
        "ON metadata.transfer_metrics (db_engine);"
        "CREATE INDEX IF NOT EXISTS idx_transfer_metrics_status "
        "ON metadata.transfer_metrics (status);";

    txn.exec(createIndexesSQL);
    txn.commit();

  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "createMetricsTable",
                  "Error creating metrics table: " + std::string(e.what()));
  }
}

// Collects transfer metrics by querying metadata.catalog and
// pg_stat_user_tables to gather information about table sizes, record counts,
// and sync status. Joins catalog data with PostgreSQL statistics to get current
// record counts (n_live_tup) and table sizes (pg_total_relation_size).
// Validates all data before processing, skipping rows with null or invalid
// values. Maps catalog status to transfer_type (FULL_LOAD, INCREMENTAL, SYNC,
// UNKNOWN) and status (SUCCESS, FAILED, PENDING, UNKNOWN). Stores all
// metrics in the internal metrics vector for later processing and saving.
void MetricsCollector::collectTransferMetrics() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());

    conn.set_session_var("statement_timeout", "30000");
    conn.set_session_var("lock_timeout", "10000");

    if (!conn.is_open()) {
      Logger::error(LogCategory::METRICS, "collectTransferMetrics",
                    "Failed to connect to database");
      return;
    }

    pqxx::work txn(conn);

    std::string transferQuery =
        "SELECT "
        "lower(c.schema_name) as schema_name,"
        "lower(c.table_name) as table_name,"
        "c.db_engine,"
        "c.status,"
        "COALESCE(pg.n_live_tup, 0) as current_records,"
        "COALESCE(pg_total_relation_size(pc.oid), 0) as table_size_bytes "
        "FROM metadata.catalog c "
        "LEFT JOIN pg_stat_user_tables pg ON lower(c.schema_name) = "
        "pg.schemaname AND "
        "lower(c.table_name) = pg.relname "
        "LEFT JOIN pg_class pc ON pg.relname = pc.relname AND pg.schemaname "
        "= pc.relnamespace::regnamespace::text "
        "WHERE c.db_engine IS NOT NULL AND c.active = true;";

    auto result = txn.exec(transferQuery);
    txn.commit();

    // Reserve memory to avoid reallocations
    metrics.clear();
    metrics.reserve(result.size());

    for (const auto &row : result) {
      if (row.size() < 6) {
        continue;
      }

      TransferMetrics metric;

      if (row[0].is_null() || row[1].is_null() || row[2].is_null()) {
        continue;
      }

      metric.schema_name = row[0].as<std::string>();
      metric.table_name = row[1].as<std::string>();
      metric.db_engine = row[2].as<std::string>();

      if (metric.schema_name.empty() || metric.table_name.empty() ||
          metric.db_engine.empty() || metric.schema_name.length() > 100 ||
          metric.table_name.length() > 100 || metric.db_engine.length() > 50) {
        continue;
      }

      std::string status = row[3].is_null() ? "" : row[3].as<std::string>();
      long long currentRecords = row[4].is_null() ? 0 : row[4].as<long long>();
      long long tableSizeBytes = row[5].is_null() ? 0 : row[5].as<long long>();

      // Validate numeric values
      if (currentRecords < 0) {
        currentRecords = 0;
      }

      if (tableSizeBytes < 0) {
        tableSizeBytes = 0;
      }

      metric.records_transferred = currentRecords;
      metric.bytes_transferred = tableSizeBytes;

      if (tableSizeBytes > 0) {
        metric.memory_used_mb = tableSizeBytes / (1024.0 * 1024.0);
      } else {
        metric.memory_used_mb = 0.0;
      }

      metric.io_operations_per_second = 0;

      // Map transfer type based on status
      if (status == "full_load" || status == "FULL_LOAD") {
        metric.transfer_type = "FULL_LOAD";
      } else if (status == "incremental" || status == "INCREMENTAL") {
        metric.transfer_type = "INCREMENTAL";
      } else if (status == "sync" || status == "SYNC") {
        metric.transfer_type = "SYNC";
      } else {
        metric.transfer_type = "UNKNOWN";
      }

      // Map status based on catalog status
      if (status == "ERROR" || status == "FAILED") {
        metric.status = "FAILED";
        metric.error_message = "Transfer failed";
      } else if (status == "NO_DATA" || status == "EMPTY") {
        metric.status = "SUCCESS";
        metric.error_message = "No data to transfer";
      } else if (status == "LISTENING_CHANGES" || status == "ACTIVE") {
        metric.status = "SUCCESS";
        metric.error_message = "";
      } else if (status == "PENDING" || status == "WAITING") {
        metric.status = "PENDING";
        metric.error_message = "Waiting for sync";
      } else {
        metric.status = "UNKNOWN";
        metric.error_message = "Unknown status: " + status;
      }

      metric.started_at = TimeUtils::getCurrentTimestamp();
      metric.completed_at = "";

      metrics.push_back(metric);
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "collectTransferMetrics",
                  "Error collecting transfer metrics: " +
                      std::string(e.what()));
  }
}

// Collects performance metrics by querying pg_stat_user_tables to get I/O
// operation counts (inserts, updates, deletes) and table sizes. Uses a hash
// map for O(1) lookup performance instead of O(n²) nested loops. Updates
// existing metrics in the metrics vector by matching schema_name and
// table_name. Calculates io_operations_per_second as the sum of all I/O
// operations and updates memory_used_mb with the current table size. Only
// processes tables that exist in both the metrics vector and
// pg_stat_user_tables.
void MetricsCollector::collectPerformanceMetrics() {
  try {
    if (metrics.empty()) {
      return;
    }

    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());

    conn.set_session_var("statement_timeout", "30000");
    conn.set_session_var("lock_timeout", "10000");

    if (!conn.is_open()) {
      Logger::error(LogCategory::METRICS, "collectPerformanceMetrics",
                    "Failed to connect to database");
      return;
    }

    pqxx::work txn(conn);

    std::string performanceQuery =
        "SELECT "
        "pst.schemaname,"
        "pst.relname,"
        "pst.n_tup_ins as inserts,"
        "pst.n_tup_upd as updates,"
        "pst.n_tup_del as deletes,"
        "pst.n_live_tup as live_tuples,"
        "pst.n_dead_tup as dead_tuples,"
        "pst.last_autoanalyze,"
        "pst.last_autovacuum,"
        "COALESCE(pg_total_relation_size(pc.oid), 0) as table_size_bytes "
        "FROM pg_stat_user_tables pst "
        "LEFT JOIN pg_class pc ON pst.relname = pc.relname "
        "AND pst.schemaname = "
        "pc.relnamespace::regnamespace::text "
        "WHERE pst.schemaname IN (SELECT DISTINCT lower(schema_name) FROM "
        "metadata.catalog);";

    auto result = txn.exec(performanceQuery);
    txn.commit();

    // Build hash map for O(1) lookup instead of O(n²)
    std::unordered_map<std::string, pqxx::row> perfMap;
    for (const auto &row : result) {
      if (row.size() < 10) {
        continue;
      }
      if (row[0].is_null() || row[1].is_null()) {
        continue;
      }
      std::string key =
          row[0].as<std::string>() + "\x1E" + row[1].as<std::string>();
      perfMap[key] = row;
    }

    for (auto &metric : metrics) {
      std::string key = metric.schema_name + "\x1E" + metric.table_name;
      auto it = perfMap.find(key);
      if (it != perfMap.end()) {
        const auto &row = it->second;
        if (row.size() >= 10) {
          long long inserts = row[2].is_null() ? 0 : row[2].as<long long>();
          long long updates = row[3].is_null() ? 0 : row[3].as<long long>();
          long long deletes = row[4].is_null() ? 0 : row[4].as<long long>();
          long long total_operations = inserts + updates + deletes;

          if (total_operations > 0 && total_operations <= 2147483647LL) {
            metric.io_operations_per_second =
                static_cast<int>(total_operations);
          }

          if (!row[9].is_null()) {
            long long tableSizeBytes = row[9].as<long long>();
            if (tableSizeBytes > 0) {
              metric.memory_used_mb = tableSizeBytes / (1024.0 * 1024.0);
            }
          }
        }
      }
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "collectPerformanceMetrics",
                  "Error collecting performance metrics: " +
                      std::string(e.what()));
  }
}

// Collects metadata metrics by querying metadata.catalog to get sync status and
// active flags. Uses a hash map for O(1) lookup performance. Updates existing
// metrics in the metrics vector by matching schema_name, table_name, and
// db_engine. Determines transfer_type based on catalog status (full_load,
// incremental, or sync). Determines status based on active flag and presence of
// active status (SUCCESS if active, FAILED if inactive). This function refines
// the metrics collected in collectTransferMetrics with more accurate metadata.
void MetricsCollector::collectMetadataMetrics() {
  try {
    if (metrics.empty()) {
      return;
    }

    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());

    conn.set_session_var("statement_timeout", "30000");
    conn.set_session_var("lock_timeout", "10000");

    if (!conn.is_open()) {
      Logger::error(LogCategory::METRICS, "collectMetadataMetrics",
                    "Failed to connect to database");
      return;
    }

    pqxx::work txn(conn);

    std::string metadataQuery = "SELECT "
                                "lower(schema_name) as schema_name,"
                                "lower(table_name) as table_name,"
                                "db_engine,"
                                "status,"
                                "active "
                                "FROM metadata.catalog "
                                "WHERE db_engine IS NOT NULL;";

    auto result = txn.exec(metadataQuery);
    txn.commit();

    // Build hash map for O(1) lookup instead of O(n²)
    std::unordered_map<std::string, pqxx::row> metaMap;
    for (const auto &row : result) {
      if (row.size() < 5) {
        continue;
      }
      if (row[0].is_null() || row[1].is_null() || row[2].is_null()) {
        continue;
      }
      std::string key = row[0].as<std::string>() + "\x1E" +
                        row[1].as<std::string>() + "\x1E" +
                        row[2].as<std::string>();
      metaMap[key] = row;
    }

    for (auto &metric : metrics) {
      std::string key = metric.schema_name + "\x1E" + metric.table_name +
                        "\x1E" + metric.db_engine;
      auto it = metaMap.find(key);
      if (it != metaMap.end()) {
        const auto &row = it->second;
        if (row.size() >= 5) {
          std::string status = row[3].is_null() ? "" : row[3].as<std::string>();
          if (status == "full_load") {
            metric.transfer_type = "FULL_LOAD";
          } else if (status == "incremental") {
            metric.transfer_type = "INCREMENTAL";
          } else if (metric.transfer_type.empty() ||
                     metric.transfer_type == "UNKNOWN") {
            metric.transfer_type = "SYNC";
          }

          bool active = row[4].is_null() ? false : row[4].as<bool>();
          if (!active) {
            metric.status = "FAILED";
            metric.error_message = "Table marked as inactive";
          } else {
            if (metric.status.empty() || metric.status == "UNKNOWN") {
              metric.status = "SUCCESS";
            }
          }
        }
      }
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "collectMetadataMetrics",
                  "Error collecting metadata metrics: " +
                      std::string(e.what()));
  }
}

// Collects timestamp metrics by querying metadata.catalog for
// mongo_last_sync_time values (MongoDB only). Uses a hash map for O(1) lookup
// performance. Updates existing metrics in the metrics vector by matching
// schema_name, table_name, and db_engine. Sets both completed_at and started_at
// to the mongo_last_sync_time value if available. This function ensures
// timestamp accuracy by using actual sync times from the catalog rather than
// estimates.
void MetricsCollector::collectTimestampMetrics() {
  try {
    if (metrics.empty()) {
      return;
    }

    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());

    conn.set_session_var("statement_timeout", "30000");
    conn.set_session_var("lock_timeout", "10000");

    if (!conn.is_open()) {
      Logger::error(LogCategory::METRICS, "collectTimestampMetrics",
                    "Failed to connect to database");
      return;
    }

    pqxx::work txn(conn);

    std::string timestampQuery =
        "SELECT "
        "lower(schema_name) as schema_name,"
        "lower(table_name) as table_name,"
        "db_engine,"
        "mongo_last_sync_time "
        "FROM metadata.catalog "
        "WHERE db_engine = 'MongoDB' AND mongo_last_sync_time IS NOT NULL;";

    auto result = txn.exec(timestampQuery);
    txn.commit();

    // Build hash map for O(1) lookup instead of O(n²)
    std::unordered_map<std::string, pqxx::row> timeMap;
    for (const auto &row : result) {
      if (row.size() < 4) {
        continue;
      }
      if (row[0].is_null() || row[1].is_null() || row[2].is_null()) {
        continue;
      }
      std::string key = row[0].as<std::string>() + "\x1E" +
                        row[1].as<std::string>() + "\x1E" +
                        row[2].as<std::string>();
      timeMap[key] = row;
    }

    for (auto &metric : metrics) {
      std::string key = metric.schema_name + "\x1E" + metric.table_name +
                        "\x1E" + metric.db_engine;
      auto it = timeMap.find(key);
      if (it != timeMap.end()) {
        const auto &row = it->second;
        if (row.size() >= 4 && !row[3].is_null()) {
          std::string completedTime = row[3].as<std::string>();
          if (!completedTime.empty()) {
            if (metric.completed_at.empty()) {
              metric.completed_at = completedTime;
              metric.started_at = getEstimatedStartTime(metric.completed_at);
            } else if (metric.started_at.empty()) {
              metric.started_at = getEstimatedStartTime(metric.completed_at);
            }
          }
        }
      }
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "collectTimestampMetrics",
                  "Error collecting timestamp metrics: " +
                      std::string(e.what()));
  }
}

// Saves all collected metrics to the metadata.transfer_metrics table using
// parameterized queries to prevent SQL injection. Uses INSERT ... ON CONFLICT
// DO UPDATE to handle duplicate entries (same schema/table/engine/date). If a
// record already exists for the same day, updates it with new values. Handles
// NULL values properly by passing nullptr for empty strings. Commits all
// inserts/updates in a single transaction for atomicity. If saving fails, logs
// an error but does not throw an exception.
void MetricsCollector::saveMetricsToDatabase() {
  try {
    if (metrics.empty()) {
      return;
    }

    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());

    conn.set_session_var("statement_timeout", "30000");
    conn.set_session_var("lock_timeout", "10000");

    if (!conn.is_open()) {
      Logger::error(LogCategory::METRICS, "saveMetricsToDatabase",
                    "Failed to connect to database");
      return;
    }

    pqxx::work txn(conn);

    std::string insertQuery =
        "INSERT INTO metadata.transfer_metrics ("
        "schema_name, table_name, db_engine,"
        "records_transferred, bytes_transferred, memory_used_mb, "
        "io_operations_per_second,"
        "transfer_type, status, error_message,"
        "started_at, completed_at"
        ") VALUES ("
        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12"
        ") ON CONFLICT (schema_name, table_name, db_engine, "
        "created_date) DO UPDATE SET "
        "records_transferred = EXCLUDED.records_transferred,"
        "bytes_transferred = EXCLUDED.bytes_transferred,"
        "memory_used_mb = EXCLUDED.memory_used_mb,"
        "io_operations_per_second = EXCLUDED.io_operations_per_second,"
        "transfer_type = EXCLUDED.transfer_type,"
        "status = EXCLUDED.status,"
        "error_message = EXCLUDED.error_message,"
        "started_at = EXCLUDED.started_at,"
        "completed_at = EXCLUDED.completed_at;";

    for (const auto &metric : metrics) {
      if (metric.schema_name.length() > 100 ||
          metric.table_name.length() > 100 || metric.db_engine.length() > 50 ||
          metric.transfer_type.length() > 20 || metric.status.length() > 20) {
        continue;
      }

      pqxx::params params;
      params.append(metric.schema_name);
      params.append(metric.table_name);
      params.append(metric.db_engine);
      params.append(metric.records_transferred);
      params.append(metric.bytes_transferred);
      params.append(metric.memory_used_mb);
      params.append(metric.io_operations_per_second);
      params.append(metric.transfer_type);
      params.append(metric.status);
      params.append(metric.error_message.empty() ? nullptr : metric.error_message.c_str());
      params.append(metric.started_at.empty() ? nullptr : metric.started_at.c_str());
      params.append(metric.completed_at.empty() ? nullptr : metric.completed_at.c_str());
      txn.exec(pqxx::zview(insertQuery), params);
    }

    txn.commit();

  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "saveMetricsToDatabase",
                  "Error saving metrics to database: " + std::string(e.what()));
  }
}

// Generates an aggregated metrics report for the current day by querying
// metadata.transfer_metrics. Calculates total tables, successful/failed/pending
// transfer counts, total records and bytes transferred, average memory usage,
// total I/O operations, and success rate percentage. Converts bytes to MB for
// readability. Currently calculates all values but does not output them
// (similar to DataGovernance::generateReport). The calculated values should be
// logged or returned for display.
void MetricsCollector::generateMetricsReport() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());

    conn.set_session_var("statement_timeout", "30000");
    conn.set_session_var("lock_timeout", "10000");

    if (!conn.is_open()) {
      Logger::error(LogCategory::METRICS, "generateMetricsReport",
                    "Failed to connect to database");
      return;
    }

    pqxx::work txn(conn);

    std::string reportQuery =
        "SELECT "
        "COUNT(*) as total_tables,"
        "COUNT(*) FILTER (WHERE status = 'SUCCESS') as successful_transfers,"
        "COUNT(*) FILTER (WHERE status = 'FAILED') as failed_transfers,"
        "COUNT(*) FILTER (WHERE status = 'PENDING') as pending_transfers,"
        "SUM(records_transferred) as total_records_transferred,"
        "SUM(bytes_transferred) as total_bytes_transferred,"
        "AVG(memory_used_mb) as avg_memory_used_mb,"
        "SUM(io_operations_per_second) as total_io_operations "
        "FROM metadata.transfer_metrics "
        "WHERE created_at >= CURRENT_DATE;";

    auto result = txn.exec(reportQuery);
    txn.commit();

    if (!result.empty() && result[0].size() >= 8) {
      auto row = result[0];
      int totalTables = row[0].as<int>();
      int successfulTransfers = row[1].as<int>();
      int failedTransfers = row[2].as<int>();
      int pendingTransfers = row[3].as<int>();
      long long totalRecords = row[4].is_null() ? 0 : row[4].as<long long>();
      long long totalBytes = row[5].is_null() ? 0 : row[5].as<long long>();
      double avgMemoryUsed = row[6].is_null() ? 0.0 : row[6].as<double>();
      long long totalIOOperations =
          row[7].is_null() ? 0 : row[7].as<long long>();

      // Calculate success rate
      double successRate =
          totalTables > 0 ? (double)successfulTransfers / totalTables * 100.0
                          : 0.0;

      // Convert bytes to MB for readability
      double totalMB = totalBytes / (1024.0 * 1024.0);

      Logger::info(
          LogCategory::METRICS, "generateMetricsReport",
          "Metrics Report: Total tables=" + std::to_string(totalTables) +
              ", Successful=" + std::to_string(successfulTransfers) +
              ", Failed=" + std::to_string(failedTransfers) +
              ", Pending=" + std::to_string(pendingTransfers) +
              ", Total records=" + std::to_string(totalRecords) +
              ", Total MB=" + std::to_string(totalMB) +
              ", Avg memory MB=" + std::to_string(avgMemoryUsed) +
              ", Total I/O ops=" + std::to_string(totalIOOperations) +
              ", Success rate=" + std::to_string(successRate) + "%");
    } else {
      Logger::info(LogCategory::METRICS, "generateMetricsReport",
                   "No metrics found for current date");
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "generateMetricsReport",
                  "Error generating metrics report: " + std::string(e.what()));
  }
}

std::string
MetricsCollector::getEstimatedStartTime(const std::string &completedAt) {
  try {
    if (completedAt.empty()) {
      return TimeUtils::getCurrentTimestamp();
    }

    std::tm tm = {};
    std::istringstream ss(completedAt);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

    // Do not require ss.eof(): completedAt may have fractional seconds (e.g. .560000)
    // that get_time does not consume; we only need the date/time part for the 1h offset.
    if (ss.fail()) {
      return TimeUtils::getCurrentTimestamp();
    }

    std::time_t time = std::mktime(&tm);
    if (time == -1) {
      return TimeUtils::getCurrentTimestamp();
    }

    auto time_point = std::chrono::system_clock::from_time_t(time);
    auto now = std::chrono::system_clock::now();
    if (time_point > now) {
      return TimeUtils::getCurrentTimestamp();
    }

    auto estimated_start = time_point - std::chrono::hours(1);
    auto time_t_start = std::chrono::system_clock::to_time_t(estimated_start);

    std::tm tm_result = {};
#ifdef _WIN32
    errno_t err = localtime_s(&tm_result, &time_t_start);
    if (err != 0) {
      return TimeUtils::getCurrentTimestamp();
    }
#else
    std::tm *tm_ptr = localtime_r(&time_t_start, &tm_result);
    if (!tm_ptr) {
      return TimeUtils::getCurrentTimestamp();
    }
#endif

    std::stringstream result;
    result << std::put_time(&tm_result, "%Y-%m-%d %H:%M:%S");
    result << ".000";

    return result.str();
  } catch (const std::exception &e) {
    return TimeUtils::getCurrentTimestamp();
  }
}
