#ifndef METRICSCOLLECTOR_H
#define METRICSCOLLECTOR_H

#include "core/Config.h"
#include "core/logger.h"
#include <string>
#include <vector>

struct TransferMetrics {
  std::string schema_name;
  std::string table_name;
  std::string db_engine;

  long long records_transferred = 0;
  long long bytes_transferred = 0;
  double memory_used_mb = 0.0;
  int io_operations_per_second = 0;

  // Metadatos
  std::string transfer_type;
  std::string status;
  std::string error_message;

  // Timestamps
  std::string started_at;
  std::string completed_at;
};

class MetricsCollector {
public:
  MetricsCollector() = default;
  ~MetricsCollector() = default;

  void collectAllMetrics();
  void createMetricsTable();

private:
  void collectTransferMetrics();
  void collectPerformanceMetrics();
  void collectMetadataMetrics();
  void collectTimestampMetrics();
  void saveMetricsToDatabase(const std::string& runId);
  void generateMetricsReport();

  std::string getEstimatedStartTime(const std::string &completedAt);

  std::vector<TransferMetrics> metrics;
};

#endif // METRICSCOLLECTOR_H
