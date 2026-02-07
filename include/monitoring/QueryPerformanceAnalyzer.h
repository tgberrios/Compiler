#ifndef QUERY_PERFORMANCE_ANALYZER_H
#define QUERY_PERFORMANCE_ANALYZER_H

#include "core/logger.h"
#include "third_party/json.hpp"
#include <string>
#include <vector>

using json = nlohmann::json;

// QueryPerformanceAnalyzer: Análisis profundo de queries y optimizaciones
class QueryPerformanceAnalyzer {
public:
  struct QueryAnalysis {
    std::string queryId;
    std::string queryText;
    std::string queryFingerprint;
    json explainPlan;
    double executionTime;
    int rowsExamined;
    int rowsReturned;
    std::vector<std::string> issues; // seq_scan, missing_index, n_plus_one, seq_scan_large, nested_loop_many_loops, filter_on_seq_scan, sort_spill_disk, temp_file_usage, estimate_mismatch, bitmap_heap_large, high_buffer_read, index_scan_slow, hash_join_large, merge_join_expensive, cte_materialized, subquery_scan, subplan_correlated, limit_full_scan, hash_spill_disk, large_plan_width
    std::vector<std::string> recommendations;
    std::chrono::system_clock::time_point analyzedAt;
  };

  struct Regression {
    std::string queryFingerprint;
    double previousAvgTime;
    double currentAvgTime;
    double regressionPercent;
    std::chrono::system_clock::time_point detectedAt;
  };

  struct OptimizationSuggestion {
    std::string id;
    std::string queryFingerprint;
    std::string type; // "missing_index", "query_rewrite", "partitioning"
    std::string description;
    std::string sqlSuggestion;
    double estimatedImprovement; // percentage
    std::chrono::system_clock::time_point suggestedAt;
  };

  explicit QueryPerformanceAnalyzer(const std::string& connectionString);
  ~QueryPerformanceAnalyzer() = default;

  // Analizar query profundo (EXPLAIN ANALYZE)
  std::unique_ptr<QueryAnalysis> analyzeQuery(const std::string& queryId,
                                               const std::string& queryText);

  // Detectar regresiones
  std::vector<Regression> detectRegressions(int days = 7);

  // Generar sugerencias de optimización
  std::vector<OptimizationSuggestion> generateSuggestions(const std::string& queryFingerprint = "");

  // Obtener análisis
  std::unique_ptr<QueryAnalysis> getAnalysis(const std::string& queryId);

  // Obtener sugerencias
  std::vector<OptimizationSuggestion> getSuggestions(const std::string& queryFingerprint = "");

private:
  std::string connectionString_;

  std::string normalizeQuery(const std::string& queryText);
  std::string generateFingerprint(const std::string& queryText);
  json executeExplainAnalyze(const std::string& queryText);
  std::vector<std::string> detectIssues(const json& explainPlan);
  std::vector<std::string> generateRecommendations(const json& explainPlan,
                                                    const std::vector<std::string>& issues);
  void ensureTablesExist();
  bool saveAnalysisToDatabase(const QueryAnalysis& analysis);
  bool saveSuggestionToDatabase(const OptimizationSuggestion& suggestion);

  /** Build optimization suggestions from an analysis (issues + recommendations + plan). */
  std::vector<OptimizationSuggestion> buildSuggestionsFromAnalysis(const QueryAnalysis& analysis);
};

#endif // QUERY_PERFORMANCE_ANALYZER_H
