#include "DataQuality.h"

bool DataQuality::validateTable(pqxx::connection &conn,
                                const std::string &schema,
                                const std::string &table,
                                const std::string &engine) {
  auto start = std::chrono::high_resolution_clock::now();

  try {
    QualityMetrics metrics;
    metrics.schema_name = schema;
    metrics.table_name = table;
    metrics.source_db_engine = engine;

    // Collect all metrics
    metrics = collectMetrics(conn, schema, table);

    // Calculate duration
    auto end = std::chrono::high_resolution_clock::now();
    metrics.check_duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();

    // Determine final status and save
    metrics.validation_status = determineValidationStatus(metrics);
    return saveMetrics(conn, metrics);
  } catch (const std::exception &e) {
    Logger::error("DataQuality", "Error validating table " + schema + "." +
                                     table + ": " + e.what());
    return false;
  }
}

DataQuality::QualityMetrics
DataQuality::collectMetrics(pqxx::connection &conn, const std::string &schema,
                            const std::string &table) {
  QualityMetrics metrics;
  metrics.schema_name = schema;
  metrics.table_name = table;

  try {
    // Check data types and collect type-related metrics
    checkDataTypes(conn, metrics);

    // Check for nulls
    checkNullCounts(conn, metrics);

    // Check for duplicates
    checkDuplicates(conn, metrics);

    // Check constraints
    checkConstraints(conn, metrics);

    // Calculate checksum
    metrics.data_checksum = calculateChecksum(conn, schema, table);

    // Calculate final quality score
    calculateQualityScore(metrics);

  } catch (const std::exception &e) {
    Logger::error("DataQuality",
                  "Error collecting metrics: " + std::string(e.what()));
    metrics.error_details = e.what();
    metrics.validation_status = "FAILED";
  }

  return metrics;
}

bool DataQuality::checkDataTypes(pqxx::connection &conn,
                                 QualityMetrics &metrics) {
  try {
    pqxx::work txn(conn);

    // Get column information
    auto result =
        txn.exec("SELECT column_name, data_type, is_nullable "
                 "FROM information_schema.columns "
                 "WHERE table_schema = " +
                 txn.quote(metrics.schema_name) +
                 " AND table_name = " + txn.quote(metrics.table_name));

    json type_mismatches;

    for (const auto &row : result) {
      std::string column = row[0].as<std::string>();
      std::string type = row[1].as<std::string>();

      // Check for type-specific issues
      auto type_check = txn.exec("SELECT COUNT(*) FROM " + metrics.schema_name +
                                 "." + metrics.table_name + " WHERE " + column +
                                 " IS NOT NULL AND " + "NOT pg_typeof(" +
                                 column + ")::text = " + txn.quote(type));

      size_t invalid_count = type_check[0][0].as<size_t>();
      if (invalid_count > 0) {
        type_mismatches[column] = {{"expected_type", type},
                                   {"invalid_count", invalid_count}};
        metrics.invalid_type_count += invalid_count;
      }
    }

    metrics.type_mismatch_details = type_mismatches;
    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error("DataQuality",
                  "Error checking data types: " + std::string(e.what()));
    return false;
  }
}

bool DataQuality::checkNullCounts(pqxx::connection &conn,
                                  QualityMetrics &metrics) {
  try {
    pqxx::work txn(conn);

    // Get total rows and null counts
    auto result = txn.exec(
        "SELECT COUNT(*) as total, "
        "SUM(CASE WHEN EXISTS (SELECT 1 FROM json_each_text(to_json(t)) j "
        "WHERE j.value IS NULL) THEN 1 ELSE 0 END) as null_rows "
        "FROM " +
        metrics.schema_name + "." + metrics.table_name + " t");

    metrics.total_rows = result[0][0].as<size_t>();
    metrics.null_count = result[0][1].as<size_t>();

    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error("DataQuality",
                  "Error checking null counts: " + std::string(e.what()));
    return false;
  }
}

bool DataQuality::checkDuplicates(pqxx::connection &conn,
                                  QualityMetrics &metrics) {
  try {
    pqxx::work txn(conn);

    // Count duplicates across all columns
    auto result = txn.exec("WITH duplicates AS ("
                           "  SELECT COUNT(*) - COUNT(DISTINCT *) as dup_count "
                           "  FROM " +
                           metrics.schema_name + "." + metrics.table_name +
                           ") SELECT COALESCE(dup_count, 0) FROM duplicates");

    metrics.duplicate_count = result[0][0].as<size_t>();

    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error("DataQuality",
                  "Error checking duplicates: " + std::string(e.what()));
    return false;
  }
}

bool DataQuality::checkConstraints(pqxx::connection &conn,
                                   QualityMetrics &metrics) {
  try {
    pqxx::work txn(conn);
    json constraint_issues;

    // Check foreign key constraints
    auto fk_result =
        txn.exec("SELECT COUNT(*) "
                 "FROM information_schema.referential_constraints rc "
                 "JOIN information_schema.key_column_usage kcu "
                 "ON rc.constraint_name = kcu.constraint_name "
                 "WHERE kcu.table_schema = " +
                 txn.quote(metrics.schema_name) +
                 " AND kcu.table_name = " + txn.quote(metrics.table_name));

    if (fk_result[0][0].as<int>() > 0) {
      // If there are foreign keys, check for violations
      auto violations =
          txn.exec("SELECT COUNT(*) "
                   "FROM " +
                   metrics.schema_name + "." + metrics.table_name +
                   " WHERE NOT EXISTS (SELECT 1 FROM referenced_table WHERE id "
                   "= foreign_key)");
      metrics.referential_integrity_errors = violations[0][0].as<size_t>();
    }

    metrics.integrity_check_details = constraint_issues;
    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error("DataQuality",
                  "Error checking constraints: " + std::string(e.what()));
    return false;
  }
}

std::string DataQuality::calculateChecksum(pqxx::connection &conn,
                                           const std::string &schema,
                                           const std::string &table) {
  try {
    pqxx::work txn(conn);

    // Calculate MD5 hash of the entire table content
    auto result =
        txn.exec("SELECT MD5(CAST((SELECT string_agg(CAST(t AS text), '') "
                 "FROM (SELECT * FROM " +
                 schema + "." + table + " ORDER BY 1) t) AS text))");

    txn.commit();
    return result[0][0].as<std::string>();
  } catch (const std::exception &e) {
    Logger::error("DataQuality",
                  "Error calculating checksum: " + std::string(e.what()));
    return "";
  }
}

void DataQuality::calculateQualityScore(QualityMetrics &metrics) {
  double score = 100.0;

  // Deduct points for various issues
  if (metrics.total_rows > 0) {
    // Deduct for null values
    score -=
        (static_cast<double>(metrics.null_count) / metrics.total_rows) * 20.0;

    // Deduct for duplicates
    score -=
        (static_cast<double>(metrics.duplicate_count) / metrics.total_rows) *
        20.0;

    // Deduct for type mismatches
    score -=
        (static_cast<double>(metrics.invalid_type_count) / metrics.total_rows) *
        30.0;

    // Deduct for integrity errors
    score -= (static_cast<double>(metrics.referential_integrity_errors) /
              metrics.total_rows) *
             30.0;
  }

  // Ensure score stays within 0-100 range
  metrics.quality_score = std::max(0.0, std::min(100.0, score));
}

std::string
DataQuality::determineValidationStatus(const QualityMetrics &metrics) {
  if (metrics.quality_score >= 90.0) {
    return "PASSED";
  } else if (metrics.quality_score >= 70.0) {
    return "WARNING";
  } else {
    return "FAILED";
  }
}

bool DataQuality::saveMetrics(pqxx::connection &conn,
                              const QualityMetrics &metrics) {
  try {
    pqxx::work txn(conn);

    txn.exec_params(
        "INSERT INTO metadata.data_quality ("
        "schema_name, table_name, source_db_engine, "
        "total_rows, null_count, duplicate_count, data_checksum, "
        "invalid_type_count, type_mismatch_details, out_of_range_count, "
        "referential_integrity_errors, constraint_violation_count, "
        "integrity_check_details, "
        "validation_status, error_details, quality_score, check_duration_ms"
        ") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, "
        "$14, $15, $16, $17)",
        metrics.schema_name, metrics.table_name, metrics.source_db_engine,
        static_cast<int64_t>(metrics.total_rows),
        static_cast<int64_t>(metrics.null_count),
        static_cast<int64_t>(metrics.duplicate_count), metrics.data_checksum,
        static_cast<int64_t>(metrics.invalid_type_count),
        metrics.type_mismatch_details.dump(),
        static_cast<int64_t>(metrics.out_of_range_count),
        static_cast<int64_t>(metrics.referential_integrity_errors),
        static_cast<int64_t>(metrics.constraint_violation_count),
        metrics.integrity_check_details.dump(), metrics.validation_status,
        metrics.error_details, metrics.quality_score,
        metrics.check_duration_ms);

    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error("DataQuality",
                  "Error saving metrics: " + std::string(e.what()));
    return false;
  }
}

std::vector<DataQuality::QualityMetrics>
DataQuality::getLatestMetrics(pqxx::connection &conn,
                              const std::string &status) {

  std::vector<QualityMetrics> results;

  try {
    pqxx::work txn(conn);

    std::string query =
        "WITH latest_checks AS ("
        "  SELECT DISTINCT ON (schema_name, table_name) "
        "    schema_name, table_name, source_db_engine, "
        "    total_rows, null_count, duplicate_count, data_checksum, "
        "    invalid_type_count, type_mismatch_details, out_of_range_count, "
        "    referential_integrity_errors, constraint_violation_count, "
        "    integrity_check_details, validation_status, error_details, "
        "    quality_score, check_duration_ms "
        "  FROM metadata.data_quality "
        "  ORDER BY schema_name, table_name, check_timestamp DESC"
        ") SELECT * FROM latest_checks ";

    if (!status.empty()) {
      query += "WHERE validation_status = " + txn.quote(status);
    }

    query += " ORDER BY schema_name, table_name";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      QualityMetrics metrics;
      metrics.schema_name = row["schema_name"].as<std::string>();
      metrics.table_name = row["table_name"].as<std::string>();
      metrics.source_db_engine = row["source_db_engine"].as<std::string>();
      metrics.total_rows = row["total_rows"].as<size_t>();
      metrics.null_count = row["null_count"].as<size_t>();
      metrics.duplicate_count = row["duplicate_count"].as<size_t>();
      metrics.data_checksum = row["data_checksum"].as<std::string>();
      metrics.invalid_type_count = row["invalid_type_count"].as<size_t>();
      metrics.type_mismatch_details =
          json::parse(row["type_mismatch_details"].as<std::string>());
      metrics.out_of_range_count = row["out_of_range_count"].as<size_t>();
      metrics.referential_integrity_errors =
          row["referential_integrity_errors"].as<size_t>();
      metrics.constraint_violation_count =
          row["constraint_violation_count"].as<size_t>();
      metrics.integrity_check_details =
          json::parse(row["integrity_check_details"].as<std::string>());
      metrics.validation_status = row["validation_status"].as<std::string>();
      metrics.error_details = row["error_details"].is_null()
                                  ? ""
                                  : row["error_details"].as<std::string>();
      metrics.quality_score = row["quality_score"].as<double>();
      metrics.check_duration_ms = row["check_duration_ms"].as<int64_t>();

      results.push_back(metrics);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error("DataQuality",
                  "Error getting latest metrics: " + std::string(e.what()));
  }

  return results;
}
