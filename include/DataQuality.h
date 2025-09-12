#ifndef DATAQUALITY_H
#define DATAQUALITY_H

#include "Config.h"
#include "json.hpp"
#include "logger.h"
#include <chrono>
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;

class DataQuality {
public:
  DataQuality() = default;
  ~DataQuality() = default;

  struct QualityMetrics {
    std::string schema_name;
    std::string table_name;
    std::string source_db_engine;

    // Data Metrics
    size_t total_rows{0};
    size_t null_count{0};
    size_t duplicate_count{0};
    std::string data_checksum;

    // Type Validation
    size_t invalid_type_count{0};
    json type_mismatch_details;
    size_t out_of_range_count{0};

    // Integrity Checks
    size_t referential_integrity_errors{0};
    size_t constraint_violation_count{0};
    json integrity_check_details;

    // Status & Results
    std::string validation_status;
    std::string error_details;
    double quality_score{100.0};

    // Performance
    int64_t check_duration_ms{0};
  };

  // Main functions
  bool validateTable(pqxx::connection &conn, const std::string &schema,
                     const std::string &table, const std::string &engine);
  QualityMetrics collectMetrics(pqxx::connection &conn,
                                const std::string &schema,
                                const std::string &table);
  bool saveMetrics(pqxx::connection &conn, const QualityMetrics &metrics);
  std::vector<QualityMetrics> getLatestMetrics(pqxx::connection &conn,
                                               const std::string &status = "");

private:
  // Validation functions
  bool checkDataTypes(pqxx::connection &conn, QualityMetrics &metrics);
  bool checkNullCounts(pqxx::connection &conn, QualityMetrics &metrics);
  bool checkDuplicates(pqxx::connection &conn, QualityMetrics &metrics);
  bool checkConstraints(pqxx::connection &conn, QualityMetrics &metrics);
  std::string calculateChecksum(pqxx::connection &conn,
                                const std::string &schema,
                                const std::string &table);

  // Helper functions
  void calculateQualityScore(QualityMetrics &metrics);
  std::string determineValidationStatus(const QualityMetrics &metrics);
};

#endif // DATAQUALITY_H
