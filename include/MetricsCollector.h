#ifndef METRICSCOLLECTOR_H
#define METRICSCOLLECTOR_H

#include "Config.h"
#include "logger.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>

struct TransferMetrics {
  std::string schema_name;
  std::string table_name;
  std::string db_engine;

  // Métricas de Transferencia
  long long records_transferred = 0;
  long long bytes_transferred = 0;
  int transfer_duration_ms = 0;
  double transfer_rate_per_second = 0.0;

  // Métricas de Rendimiento
  int chunk_size = 0;
  double memory_used_mb = 0.0;
  double cpu_usage_percent = 0.0;
  int io_operations_per_second = 0;

  // Métricas de Latencia
  double avg_latency_ms = 0.0;
  double min_latency_ms = 0.0;
  double max_latency_ms = 0.0;
  double p95_latency_ms = 0.0;
  double p99_latency_ms = 0.0;
  int latency_samples = 0;

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
  void collectLatencyMetrics();
  void saveMetricsToDatabase();
  void generateMetricsReport();

  std::string escapeSQL(const std::string &value);
  std::string getCurrentTimestamp();
  double calculateTransferRate(long long records, int duration_ms);
  long long calculateBytesTransferred(const std::string &schema_name,
                                      const std::string &table_name);
  double calculatePercentile(const std::vector<double> &values,
                             double percentile);
  void measureQueryLatency(const std::string &query, double &latency_ms);

  std::vector<TransferMetrics> metrics;
};

#endif // METRICSCOLLECTOR_H
