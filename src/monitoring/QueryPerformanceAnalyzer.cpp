#include "monitoring/QueryPerformanceAnalyzer.h"
#include "core/database_config.h"
#include "core/logger.h"
#include <pqxx/pqxx>
#include <sstream>
#include <regex>
#include <algorithm>
#include <cmath>
#include <cstdint>

namespace {
struct PlanNodeInfo {
  std::string nodeType;
  std::string relationName;
  std::string schema;
  std::string alias;
  double actualRows = 0;
  double actualTotalTimeMs = 0;
  int64_t actualLoops = 1;
  double planRows = 0;
  double totalCost = 0;
  int planWidth = 0;
  int64_t sharedHitBlocks = 0;
  int64_t sharedReadBlocks = 0;
  int64_t tempReadBlocks = 0;
  int64_t tempWrittenBlocks = 0;
  std::string sortMethod;
  std::string filter;
  std::string indexCond;
};

void collectPlanNodesImpl(const json& node, std::vector<PlanNodeInfo>& out) {
  if (!node.is_object()) return;
  PlanNodeInfo info;
  if (node.contains("Node Type") && node["Node Type"].is_string())
    info.nodeType = node["Node Type"].get<std::string>();
  if (node.contains("Relation Name") && node["Relation Name"].is_string())
    info.relationName = node["Relation Name"].get<std::string>();
  if (node.contains("Schema") && node["Schema"].is_string())
    info.schema = node["Schema"].get<std::string>();
  if (node.contains("Alias") && node["Alias"].is_string())
    info.alias = node["Alias"].get<std::string>();
  if (node.contains("Actual Rows") && node["Actual Rows"].is_number())
    info.actualRows = node["Actual Rows"].get<double>();
  if (node.contains("Actual Total Time") && node["Actual Total Time"].is_number())
    info.actualTotalTimeMs = node["Actual Total Time"].get<double>();
  if (node.contains("Actual Loops") && node["Actual Loops"].is_number())
    info.actualLoops = static_cast<int64_t>(node["Actual Loops"].get<double>());
  if (node.contains("Plan Rows") && node["Plan Rows"].is_number())
    info.planRows = node["Plan Rows"].get<double>();
  if (node.contains("Total Cost") && node["Total Cost"].is_number())
    info.totalCost = node["Total Cost"].get<double>();
  if (node.contains("Plan Width") && node["Plan Width"].is_number())
    info.planWidth = static_cast<int>(node["Plan Width"].get<double>());
  if (node.contains("Sort Method") && node["Sort Method"].is_string())
    info.sortMethod = node["Sort Method"].get<std::string>();
  if (node.contains("Filter") && node["Filter"].is_string())
    info.filter = node["Filter"].get<std::string>();
  if (node.contains("Index Cond") && node["Index Cond"].is_string())
    info.indexCond = node["Index Cond"].get<std::string>();
  if (node.contains("Buffers") && node["Buffers"].is_object()) {
    const auto& buf = node["Buffers"];
    if (buf.contains("Shared Hit Blocks") && buf["Shared Hit Blocks"].is_number())
      info.sharedHitBlocks = static_cast<int64_t>(buf["Shared Hit Blocks"].get<double>());
    if (buf.contains("Shared Read Blocks") && buf["Shared Read Blocks"].is_number())
      info.sharedReadBlocks = static_cast<int64_t>(buf["Shared Read Blocks"].get<double>());
    if (buf.contains("Temp Read Blocks") && buf["Temp Read Blocks"].is_number())
      info.tempReadBlocks = static_cast<int64_t>(buf["Temp Read Blocks"].get<double>());
    if (buf.contains("Temp Written Blocks") && buf["Temp Written Blocks"].is_number())
      info.tempWrittenBlocks = static_cast<int64_t>(buf["Temp Written Blocks"].get<double>());
  }
  out.push_back(info);
  if (node.contains("Plans") && node["Plans"].is_array()) {
    for (const auto& child : node["Plans"]) {
      collectPlanNodesImpl(child, out);
    }
  }
}

std::vector<PlanNodeInfo> collectPlanNodes(const json& explainPlan) {
  std::vector<PlanNodeInfo> out;
  if (explainPlan.empty() || !explainPlan.is_array()) return out;
  const auto& root = explainPlan[0];
  if (root.is_object() && root.contains("Plan")) {
    collectPlanNodesImpl(root["Plan"], out);
  }
  return out;
}

double getExecutionTimeMs(const json& explainPlan) {
  if (explainPlan.empty() || !explainPlan.is_array()) return 0;
  const auto& root = explainPlan[0];
  if (root.is_object() && root.contains("Execution Time") && root["Execution Time"].is_number())
    return root["Execution Time"].get<double>();
  return 0;
}

bool planContains(const std::vector<PlanNodeInfo>& nodes, const std::string& nodeTypeSubstr) {
  return std::any_of(nodes.begin(), nodes.end(), [&nodeTypeSubstr](const PlanNodeInfo& n) {
    return n.nodeType.find(nodeTypeSubstr) != std::string::npos;
  });
}
} // namespace

QueryPerformanceAnalyzer::QueryPerformanceAnalyzer(const std::string& connectionString)
    : connectionString_(connectionString) {
  ensureTablesExist();
}

void QueryPerformanceAnalyzer::ensureTablesExist() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.query_performance_analysis ("
        "query_id TEXT PRIMARY KEY,"
        "query_text TEXT NOT NULL,"
        "query_fingerprint TEXT NOT NULL,"
        "explain_plan JSONB DEFAULT '[]'::jsonb,"
        "execution_time DOUBLE PRECISION DEFAULT 0,"
        "rows_examined INTEGER DEFAULT 0,"
        "rows_returned INTEGER DEFAULT 0,"
        "issues JSONB DEFAULT '[]'::jsonb,"
        "recommendations JSONB DEFAULT '[]'::jsonb,"
        "analyzed_at TIMESTAMP DEFAULT NOW()"
        ")");

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.query_optimization_suggestions ("
        "suggestion_id TEXT PRIMARY KEY,"
        "query_fingerprint TEXT NOT NULL,"
        "type TEXT NOT NULL,"
        "description TEXT,"
        "sql_suggestion TEXT,"
        "estimated_improvement DOUBLE PRECISION DEFAULT 0,"
        "created_at TIMESTAMP DEFAULT NOW()"
        ")");

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.query_performance_history ("
        "query_fingerprint TEXT NOT NULL,"
        "execution_time_ms DOUBLE PRECISION NOT NULL,"
        "sampled_at TIMESTAMP DEFAULT NOW()"
        ")");
    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_qph_fingerprint_sampled "
        "ON metadata.query_performance_history (query_fingerprint, sampled_at)");

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "QueryPerformanceAnalyzer",
                  "Error creating tables: " + std::string(e.what()));
  }
}

std::unique_ptr<QueryPerformanceAnalyzer::QueryAnalysis> QueryPerformanceAnalyzer::analyzeQuery(
    const std::string& queryId, const std::string& queryText) {
  auto analysis = std::make_unique<QueryAnalysis>();
  analysis->queryId = queryId;
  analysis->queryText = queryText;
  analysis->queryFingerprint = generateFingerprint(queryText);
  analysis->explainPlan = executeExplainAnalyze(queryText);
  analysis->executionTime = getExecutionTimeMs(analysis->explainPlan);
  analysis->issues = detectIssues(analysis->explainPlan);
  analysis->recommendations = generateRecommendations(analysis->explainPlan, analysis->issues);
  analysis->analyzedAt = std::chrono::system_clock::now();

  saveAnalysisToDatabase(*analysis);
  // Generate and persist optimization suggestions so the UI can show them without a separate call.
  for (const auto& s : buildSuggestionsFromAnalysis(*analysis)) {
    saveSuggestionToDatabase(s);
  }
  return analysis;
}

std::string QueryPerformanceAnalyzer::normalizeQuery(const std::string& queryText) {
  std::string normalized = queryText;
  // Remove extra whitespace
  normalized = std::regex_replace(normalized, std::regex("\\s+"), " ");
  // Normalize case
  std::transform(normalized.begin(), normalized.end(), normalized.begin(), ::tolower);
  // Replace parameter values with placeholders
  normalized = std::regex_replace(normalized, std::regex("\\d+"), "?");
  normalized = std::regex_replace(normalized, std::regex("'[^']*'"), "'?'");
  return normalized;
}

std::string QueryPerformanceAnalyzer::generateFingerprint(const std::string& queryText) {
  std::string normalized = normalizeQuery(queryText);
  // Simple hash (in production, use proper hash function)
  std::hash<std::string> hasher;
  return std::to_string(hasher(normalized));
}

json QueryPerformanceAnalyzer::executeExplainAnalyze(const std::string& queryText) {
  json plan;
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string explainQuery = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + queryText;
    auto result = txn.exec(explainQuery);

    if (!result.empty()) {
      plan = json::parse(result[0][0].as<std::string>());
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "QueryPerformanceAnalyzer",
                  "Error executing EXPLAIN ANALYZE: " + std::string(e.what()));
  }

  return plan;
}

std::vector<std::string> QueryPerformanceAnalyzer::detectIssues(const json& explainPlan) {
  std::vector<std::string> issues;
  if (explainPlan.empty()) return issues;

  std::vector<PlanNodeInfo> nodes = collectPlanNodes(explainPlan);
  std::string planStr = explainPlan.dump();

  const double LARGE_ROW_THRESHOLD = 1000.0;
  const int64_t MANY_LOOPS_THRESHOLD = 100;
  const double ESTIMATE_MISMATCH_RATIO = 10.0;
  const double SLOW_NODE_MS = 100.0;
  const int64_t HIGH_READ_BLOCKS = 100;

  bool hasSeqScan = planContains(nodes, "Seq Scan");
  bool hasIndexScan = planContains(nodes, "Index Scan") || planContains(nodes, "Index Only Scan");
  bool hasNestedLoop = planContains(nodes, "Nested Loop");

  // 1. seq_scan
  if (hasSeqScan) issues.push_back("seq_scan");

  // 2. missing_index (Seq Scan but no index usage)
  if (hasSeqScan && !hasIndexScan) issues.push_back("missing_index");

  // 3. n_plus_one (Nested Loop)
  if (hasNestedLoop) issues.push_back("n_plus_one");

  for (const auto& n : nodes) {
    // 4. seq_scan_large
    if (n.nodeType.find("Seq Scan") != std::string::npos && n.actualRows > LARGE_ROW_THRESHOLD)
      issues.push_back("seq_scan_large");

    // 5. nested_loop_many_loops
    if (n.nodeType.find("Nested Loop") != std::string::npos && n.actualLoops > MANY_LOOPS_THRESHOLD)
      issues.push_back("nested_loop_many_loops");

    // 6. filter_on_seq_scan (Seq Scan with Filter)
    if (n.nodeType.find("Seq Scan") != std::string::npos && !n.filter.empty())
      issues.push_back("filter_on_seq_scan");

    // 7. sort_spill_disk
    if (n.nodeType.find("Sort") != std::string::npos &&
        (n.sortMethod.find("external") != std::string::npos || n.sortMethod.find("disk") != std::string::npos))
      issues.push_back("sort_spill_disk");

    // 8. temp_file_usage
    if (n.tempReadBlocks > 0 || n.tempWrittenBlocks > 0) issues.push_back("temp_file_usage");

    // 9. estimate_mismatch (actual vs plan rows)
    if (n.planRows > 0 && n.actualRows > 0) {
      double ratio = n.actualRows / n.planRows;
      if (ratio > ESTIMATE_MISMATCH_RATIO || ratio < 1.0 / ESTIMATE_MISMATCH_RATIO)
        issues.push_back("estimate_mismatch");
    }

    // 10. bitmap_heap_large
    if (n.nodeType.find("Bitmap Heap Scan") != std::string::npos && n.actualRows > LARGE_ROW_THRESHOLD)
      issues.push_back("bitmap_heap_large");

    // 11. high_buffer_read (cache miss)
    if (n.sharedReadBlocks > HIGH_READ_BLOCKS && n.sharedHitBlocks + n.sharedReadBlocks > 0) {
      double total = n.sharedHitBlocks + n.sharedReadBlocks;
      if (total > 0 && (n.sharedReadBlocks / total) > 0.2) issues.push_back("high_buffer_read");
    }

    // 12. index_scan_slow
    if ((n.nodeType.find("Index Scan") != std::string::npos || n.nodeType.find("Index Only Scan") != std::string::npos) &&
        n.actualTotalTimeMs > SLOW_NODE_MS)
      issues.push_back("index_scan_slow");

    // 13. hash_join_large (Hash Join with high row count on one side)
    if (n.nodeType.find("Hash Join") != std::string::npos && n.actualRows > LARGE_ROW_THRESHOLD)
      issues.push_back("hash_join_large");

    // 14. merge_join_expensive (Merge Join present; often implies sort cost)
    if (n.nodeType.find("Merge Join") != std::string::npos) issues.push_back("merge_join_expensive");

    // 15. cte_scan (CTE Scan = materialized CTE)
    if (n.nodeType.find("CTE Scan") != std::string::npos) issues.push_back("cte_materialized");

    // 16. subquery_scan
    if (n.nodeType.find("Subquery Scan") != std::string::npos) issues.push_back("subquery_scan");
  }

  // 17. subplan_correlated (from plan text)
  if (planStr.find("SubPlan") != std::string::npos) issues.push_back("subplan_correlated");

  // 18. limit_with_full_scan (Limit + Seq Scan in plan)
  if (planStr.find("Limit") != std::string::npos && hasSeqScan) issues.push_back("limit_full_scan");

  // 19. hash_spill (any Hash node with temp blocks)
  for (const auto& n : nodes) {
    if (n.nodeType.find("Hash") != std::string::npos && (n.tempReadBlocks > 0 || n.tempWrittenBlocks > 0)) {
      issues.push_back("hash_spill_disk");
      break;
    }
  }

  // 20. large_plan_width
  for (const auto& n : nodes) {
    if (n.planWidth > 1000) {
      issues.push_back("large_plan_width");
      break;
    }
  }

  // Deduplicate (same issue may be added from multiple nodes)
  std::sort(issues.begin(), issues.end());
  issues.erase(std::unique(issues.begin(), issues.end()), issues.end());
  return issues;
}

static std::string relationLabel(const PlanNodeInfo& n) {
  if (!n.relationName.empty()) {
    if (!n.schema.empty()) return n.schema + "." + n.relationName;
    return n.relationName;
  }
  if (!n.alias.empty()) return n.alias;
  return "";
}

std::vector<std::string> QueryPerformanceAnalyzer::generateRecommendations(
    const json& explainPlan, const std::vector<std::string>& issues) {
  std::vector<std::string> recommendations;
  std::vector<PlanNodeInfo> nodes = collectPlanNodes(explainPlan);

  auto firstNodeOf = [&nodes](const std::string& typeSubstr) -> const PlanNodeInfo* {
    for (const auto& n : nodes)
      if (n.nodeType.find(typeSubstr) != std::string::npos) return &n;
    return nullptr;
  };

  for (const auto& issue : issues) {
    if (issue == "seq_scan") {
      const PlanNodeInfo* seq = firstNodeOf("Seq Scan");
      std::string msg = "Consider adding an index on the filtered columns";
      if (seq) {
        std::string rel = relationLabel(*seq);
        if (!rel.empty()) msg += " (Seq Scan on " + rel + ")";
      }
      recommendations.push_back(msg);
    } else if (issue == "missing_index") {
      recommendations.push_back("Add indexes to improve query performance; no index scans were used.");
    } else if (issue == "n_plus_one") {
      recommendations.push_back("Consider using JOINs or batch lookups instead of nested loops.");
    } else if (issue == "seq_scan_large") {
      const PlanNodeInfo* seq = firstNodeOf("Seq Scan");
      std::string msg = "Sequential scan read many rows (>1000). Add an index or reduce the scan scope.";
      if (seq) {
        std::string rel = relationLabel(*seq);
        if (!rel.empty()) msg += " Table: " + rel + " (" + std::to_string(static_cast<int64_t>(seq->actualRows)) + " rows).";
      }
      recommendations.push_back(msg);
    } else if (issue == "nested_loop_many_loops") {
      recommendations.push_back("Nested loop executed many iterations. Prefer Hash Join or Merge Join, or add indexes on join keys.");
    } else if (issue == "filter_on_seq_scan") {
      const PlanNodeInfo* seq = firstNodeOf("Seq Scan");
      std::string msg = "Seq Scan applies a Filter in memory. Add an index on the filtered columns to push the filter into the index.";
      if (seq && !seq->filter.empty()) msg += " Filter present on this node.";
      recommendations.push_back(msg);
    } else if (issue == "sort_spill_disk") {
      recommendations.push_back("Sort spilled to disk (external merge). Increase work_mem or reduce sort input (e.g. LIMIT, better filters).");
    } else if (issue == "temp_file_usage") {
      recommendations.push_back("Temporary file I/O detected. Consider increasing work_mem or optimizing the plan to reduce memory pressure.");
    } else if (issue == "estimate_mismatch") {
      recommendations.push_back("Planner row estimates differ significantly from actual rows. Run ANALYZE on involved tables to update statistics.");
    } else if (issue == "bitmap_heap_large") {
      recommendations.push_back("Bitmap Heap Scan processed many rows. Ensure indexes exist and match the query predicates.");
    } else if (issue == "high_buffer_read") {
      recommendations.push_back("High volume of blocks read from disk (cache miss). Consider increasing shared_buffers or warming the cache for this workload.");
    } else if (issue == "index_scan_slow") {
      recommendations.push_back("Index scan took significant time. Check if the right index is used, or if a different access path (e.g. bitmap) would be better.");
    } else if (issue == "hash_join_large") {
      recommendations.push_back("Hash Join with large input. Ensure the smaller relation is used as inner; add indexes to reduce the larger side.");
    } else if (issue == "merge_join_expensive") {
      recommendations.push_back("Merge Join may require sorted inputs. Consider indexes that match the sort order, or Hash Join if order is not needed.");
    } else if (issue == "cte_materialized") {
      recommendations.push_back("CTE is materialized. If the CTE is referenced once, use a subquery or add NOT MATERIALIZED (PostgreSQL 12+) to allow inlining.");
    } else if (issue == "subquery_scan") {
      recommendations.push_back("Subquery scan in plan. Consider rewriting as a JOIN or using EXISTS/IN with an indexed column.");
    } else if (issue == "subplan_correlated") {
      recommendations.push_back("Correlated subplan detected. Rewrite to use JOINs or lateral joins to avoid re-executing the subplan per row.");
    } else if (issue == "limit_full_scan") {
      recommendations.push_back("LIMIT with full table scan. Add an index that supports ORDER BY + LIMIT to stop early.");
    } else if (issue == "hash_spill_disk") {
      recommendations.push_back("Hash operation spilled to disk. Increase work_mem or reduce the size of the hash input.");
    } else if (issue == "large_plan_width") {
      recommendations.push_back("Plan has very wide rows. Select only needed columns instead of SELECT * to reduce I/O and memory.");
    } else {
      recommendations.push_back("Review this part of the plan for optimization opportunities.");
    }
  }

  return recommendations;
}

bool QueryPerformanceAnalyzer::saveAnalysisToDatabase(const QueryAnalysis& analysis) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    json issuesJson = json::array();
    for (const auto& issue : analysis.issues) {
      issuesJson.push_back(issue);
    }

    json recommendationsJson = json::array();
    for (const auto& rec : analysis.recommendations) {
      recommendationsJson.push_back(rec);
    }

    txn.exec_params(
        "INSERT INTO metadata.query_performance_analysis "
        "(query_id, query_text, query_fingerprint, explain_plan, execution_time, rows_examined, "
        "rows_returned, issues, recommendations) "
        "VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8::jsonb, $9::jsonb) "
        "ON CONFLICT (query_id) DO UPDATE SET "
        "query_text = EXCLUDED.query_text, explain_plan = EXCLUDED.explain_plan, "
        "execution_time = EXCLUDED.execution_time, issues = EXCLUDED.issues, "
        "recommendations = EXCLUDED.recommendations, analyzed_at = NOW()",
        analysis.queryId, analysis.queryText, analysis.queryFingerprint, analysis.explainPlan.dump(),
        analysis.executionTime, analysis.rowsExamined, analysis.rowsReturned, issuesJson.dump(),
        recommendationsJson.dump());

    if (analysis.executionTime > 0 && !analysis.queryFingerprint.empty()) {
      txn.exec_params(
          "INSERT INTO metadata.query_performance_history (query_fingerprint, execution_time_ms) "
          "VALUES ($1, $2)",
          analysis.queryFingerprint, analysis.executionTime);
    }

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "QueryPerformanceAnalyzer",
                  "Error saving analysis: " + std::string(e.what()));
    return false;
  }
}

std::vector<QueryPerformanceAnalyzer::Regression> QueryPerformanceAnalyzer::detectRegressions(
    int days) {
  std::vector<Regression> regressions;
  if (days < 1) days = 7;
  const int periodDays = days;
  const double REGRESSION_THRESHOLD = 1.2; // 20% slower = regression

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Compare current period (last N days) vs previous period (N to 2N days ago).
    // Only consider fingerprints that have at least one sample in each period.
    auto result = txn.exec_params(
        R"(
        WITH current_period AS (
          SELECT query_fingerprint, AVG(execution_time_ms) AS avg_time
          FROM metadata.query_performance_history
          WHERE sampled_at >= NOW() - INTERVAL '1 day' * $1
            AND sampled_at < NOW()
          GROUP BY query_fingerprint
        ),
        previous_period AS (
          SELECT query_fingerprint, AVG(execution_time_ms) AS avg_time
          FROM metadata.query_performance_history
          WHERE sampled_at >= NOW() - INTERVAL '1 day' * $2
            AND sampled_at < NOW() - INTERVAL '1 day' * $1
          GROUP BY query_fingerprint
        )
        SELECT
          c.query_fingerprint,
          p.avg_time AS previous_avg_time,
          c.avg_time AS current_avg_time,
          CASE WHEN p.avg_time > 0 THEN
            ((c.avg_time - p.avg_time) / p.avg_time * 100.0) ELSE 0 END AS regression_pct
        FROM current_period c
        JOIN previous_period p ON c.query_fingerprint = p.query_fingerprint
        WHERE p.avg_time > 0 AND c.avg_time > p.avg_time * $3
        ORDER BY regression_pct DESC
        LIMIT 100
        )",
        periodDays, periodDays * 2, REGRESSION_THRESHOLD);

    auto now = std::chrono::system_clock::now();
    for (const auto& row : result) {
      Regression r;
      r.queryFingerprint = row["query_fingerprint"].as<std::string>();
      r.previousAvgTime = row["previous_avg_time"].as<double>();
      r.currentAvgTime = row["current_avg_time"].as<double>();
      r.regressionPercent = row["regression_pct"].as<double>();
      r.detectedAt = now;
      regressions.push_back(r);
    }
    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "QueryPerformanceAnalyzer",
                  "Error detecting regressions: " + std::string(e.what()));
  }
  return regressions;
}

// Heuristic estimated improvement (percentage) per issue type for suggestion prioritization.
static double estimatedImprovementForIssue(const std::string& issue) {
  if (issue == "missing_index" || issue == "seq_scan" || issue == "seq_scan_large" ||
      issue == "filter_on_seq_scan" || issue == "limit_full_scan")
    return 25.0;
  if (issue == "n_plus_one" || issue == "nested_loop_many_loops" || issue == "subplan_correlated" ||
      issue == "subquery_scan")
    return 20.0;
  if (issue == "sort_spill_disk" || issue == "temp_file_usage" || issue == "hash_spill_disk")
    return 15.0;
  if (issue == "estimate_mismatch")
    return 12.0;
  if (issue == "bitmap_heap_large" || issue == "index_scan_slow" || issue == "hash_join_large" ||
      issue == "merge_join_expensive")
    return 10.0;
  if (issue == "cte_materialized" || issue == "large_plan_width" || issue == "high_buffer_read")
    return 8.0;
  return 5.0;
}

// Build CREATE INDEX placeholder for index-related issues using plan node (relation + optional filter).
static std::string buildIndexSuggestionSql(const std::vector<PlanNodeInfo>& nodes,
                                           const std::string& /*issue*/) {
  const PlanNodeInfo* seq = nullptr;
  for (const auto& n : nodes) {
    if (n.nodeType.find("Seq Scan") != std::string::npos) {
      seq = &n;
      break;
    }
  }
  if (!seq) return "";
  std::string rel = relationLabel(*seq);
  if (rel.empty()) return "";
  return "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_" + seq->relationName + "_suggested "
         "ON " + rel + " (...);  -- add columns from WHERE / ORDER BY used in the query";
}

std::vector<QueryPerformanceAnalyzer::OptimizationSuggestion>
QueryPerformanceAnalyzer::buildSuggestionsFromAnalysis(const QueryAnalysis& analysis) {
  std::vector<OptimizationSuggestion> suggestions;
  std::vector<PlanNodeInfo> nodes = collectPlanNodes(analysis.explainPlan);
  const size_t n = std::min(analysis.issues.size(), analysis.recommendations.size());
  auto now = std::chrono::system_clock::now();

  for (size_t i = 0; i < n; ++i) {
    const std::string& issue = analysis.issues[i];
    const std::string& description = analysis.recommendations[i];
    OptimizationSuggestion s;
    s.id = analysis.queryFingerprint + "|" + issue + "|" + std::to_string(i);
    s.queryFingerprint = analysis.queryFingerprint;
    s.type = issue;
    s.description = description;
    s.estimatedImprovement = estimatedImprovementForIssue(issue);
    s.suggestedAt = now;

    if (issue == "missing_index" || issue == "seq_scan" || issue == "seq_scan_large" ||
        issue == "filter_on_seq_scan" || issue == "limit_full_scan") {
      s.sqlSuggestion = buildIndexSuggestionSql(nodes, issue);
    } else if (issue == "sort_spill_disk" || issue == "temp_file_usage" || issue == "hash_spill_disk") {
      s.sqlSuggestion =
          "SET work_mem = '64MB';  -- or higher for this session; adjust server-wide in postgresql.conf";
    } else if (issue == "estimate_mismatch") {
      s.sqlSuggestion = "ANALYZE;  -- run on the involved tables to refresh statistics";
    } else if (issue == "cte_materialized") {
      s.sqlSuggestion =
          "Consider rewriting CTE as a subquery or use NOT MATERIALIZED (PostgreSQL 12+).";
    } else {
      s.sqlSuggestion = "";
    }
    suggestions.push_back(s);
  }
  return suggestions;
}

std::vector<QueryPerformanceAnalyzer::OptimizationSuggestion>
QueryPerformanceAnalyzer::generateSuggestions(const std::string& queryFingerprint) {
  std::vector<OptimizationSuggestion> allSuggestions;
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);
    std::string sql =
        "SELECT query_id, query_text, query_fingerprint, explain_plan, execution_time, "
        "       rows_examined, rows_returned, issues, recommendations FROM metadata.query_performance_analysis";
    if (!queryFingerprint.empty()) {
      sql += " WHERE query_fingerprint = $1";
    }
    sql += " ORDER BY analyzed_at DESC LIMIT 50";
    pqxx::result result =
        queryFingerprint.empty() ? txn.exec(sql) : txn.exec_params(sql, queryFingerprint);
    txn.commit();

    for (const auto& row : result) {
      QueryAnalysis a;
      a.queryId = row["query_id"].as<std::string>();
      a.queryText = row["query_text"].as<std::string>();
      a.queryFingerprint = row["query_fingerprint"].as<std::string>();
      a.executionTime = row["execution_time"].as<double>();
      a.rowsExamined = row["rows_examined"].as<int>();
      a.rowsReturned = row["rows_returned"].as<int>();
      if (!row["explain_plan"].is_null()) {
        try {
          a.explainPlan = json::parse(row["explain_plan"].as<std::string>());
        } catch (...) {
          a.explainPlan = json::array();
        }
      }
      for (const auto& j : json::parse(row["issues"].as<std::string>()))
        a.issues.push_back(j.get<std::string>());
      for (const auto& j : json::parse(row["recommendations"].as<std::string>()))
        a.recommendations.push_back(j.get<std::string>());

      std::vector<OptimizationSuggestion> fromAnalysis = buildSuggestionsFromAnalysis(a);
      for (auto& sug : fromAnalysis) {
        saveSuggestionToDatabase(sug);
        allSuggestions.push_back(std::move(sug));
      }
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "QueryPerformanceAnalyzer",
                  "Error generating suggestions: " + std::string(e.what()));
  }
  return allSuggestions;
}

std::unique_ptr<QueryPerformanceAnalyzer::QueryAnalysis> QueryPerformanceAnalyzer::getAnalysis(
    const std::string& queryId) {
  // TODO: Implement
  return nullptr;
}

std::vector<QueryPerformanceAnalyzer::OptimizationSuggestion>
QueryPerformanceAnalyzer::getSuggestions(const std::string& queryFingerprint) {
  std::vector<OptimizationSuggestion> suggestions;
  // TODO: Implement
  return suggestions;
}

bool QueryPerformanceAnalyzer::saveSuggestionToDatabase(
    const OptimizationSuggestion& suggestion) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec_params(
        "INSERT INTO metadata.query_optimization_suggestions "
        "(suggestion_id, query_fingerprint, type, description, sql_suggestion, estimated_improvement) "
        "VALUES ($1, $2, $3, $4, $5, $6) "
        "ON CONFLICT (suggestion_id) DO UPDATE SET "
        "description = EXCLUDED.description, sql_suggestion = EXCLUDED.sql_suggestion, "
        "estimated_improvement = EXCLUDED.estimated_improvement",
        suggestion.id, suggestion.queryFingerprint, suggestion.type, suggestion.description,
        suggestion.sqlSuggestion, suggestion.estimatedImprovement);

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "QueryPerformanceAnalyzer",
                  "Error saving suggestion: " + std::string(e.what()));
    return false;
  }
}
