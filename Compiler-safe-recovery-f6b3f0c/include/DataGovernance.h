#ifndef DATAGOVERNANCE_H
#define DATAGOVERNANCE_H

#include "Config.h"
#include "logger.h"
#include <map>
#include <pqxx/pqxx>
#include <string>
#include <vector>

struct TableMetadata {
  std::string schema_name;
  std::string table_name;
  int total_columns = 0;
  long long total_rows = 0;
  double table_size_mb = 0.0;

  std::string primary_key_columns;
  int index_count = 0;
  int constraint_count = 0;

  double data_quality_score = 0.0;
  double null_percentage = 0.0;
  double duplicate_percentage = 0.0;

  std::string inferred_source_engine;
  std::string last_analyzed;

  std::string last_accessed;
  std::string access_frequency;
  int query_count_daily = 0;

  std::string data_category;
  std::string business_domain;
  std::string sensitivity_level;

  std::string health_status;
  std::string last_vacuum;
  double fragmentation_percentage = 0.0;
};

class DataGovernance {
public:
  DataGovernance() = default;
  ~DataGovernance() = default;

  void initialize();
  void runDiscovery();
  void generateReport();

private:
  void createGovernanceTable();
  std::vector<TableMetadata> discoverTables();
  TableMetadata extractTableMetadata(const std::string &schema_name,
                                     const std::string &table_name);
  void analyzeTableStructure(pqxx::connection &conn,
                             const std::string &schema_name,
                             const std::string &table_name,
                             TableMetadata &metadata);
  void analyzeDataQuality(pqxx::connection &conn,
                          const std::string &schema_name,
                          const std::string &table_name,
                          TableMetadata &metadata);
  void analyzeUsageStatistics(pqxx::connection &conn,
                              const std::string &schema_name,
                              const std::string &table_name,
                              TableMetadata &metadata);
  void analyzeHealthStatus(pqxx::connection &conn,
                           const std::string &schema_name,
                           const std::string &table_name,
                           TableMetadata &metadata);
  void classifyTable(TableMetadata &metadata);
  void inferSourceEngine(TableMetadata &metadata);
  void storeMetadata(const TableMetadata &metadata);
  void updateExistingMetadata(const TableMetadata &metadata);

  std::string escapeSQL(const std::string &value);
  std::string getCurrentTimestamp();
  double calculateDataQualityScore(const TableMetadata &metadata);
  std::string determineAccessFrequency(int query_count);
  std::string determineHealthStatus(const TableMetadata &metadata);
  std::string determineDataCategory(const std::string &table_name,
                                    const std::string &schema_name);
  std::string determineBusinessDomain(const std::string &table_name,
                                      const std::string &schema_name);
  std::string determineSensitivityLevel(const std::string &table_name,
                                        const std::string &schema_name);
};

#endif // DATAGOVERNANCE_H
