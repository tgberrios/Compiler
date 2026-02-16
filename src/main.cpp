#include "core/Config.h"
#include "core/database_config.h"
#include "sync/StreamingData.h"
#include "sync/DBTExecutor.h"
#include "governance/DynamicMaskingEngine.h"
#include "governance/TokenizationManager.h"
#include "governance/AnonymizationEngine.h"
#include "governance/FineGrainedPermissions.h"
#include "monitoring/DistributedTracingManager.h"
#include "monitoring/APMManager.h"
#include "monitoring/BottleneckDetector.h"
#include "monitoring/ResourceTracker.h"
#include "monitoring/CostTracker.h"
#include "monitoring/LogAggregator.h"
#include "monitoring/AdvancedAlertingManager.h"
#include "monitoring/QueryPerformanceAnalyzer.h"
#include "catalog/DataLakeMappingManager.h"
#include "maintenance/CDCCleanupManager.h"
#include "catalog/UnusedObjectsDetector.h"
#include "third_party/json.hpp"
#include <atomic>
#include <csignal>
#include <iostream>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <thread>

namespace {
constexpr int EXIT_SUCCESS_CODE = 0;
constexpr int EXIT_INIT_ERROR = 2;
constexpr int EXIT_EXECUTION_ERROR = 3;
constexpr int EXIT_CRITICAL_ERROR = 4;
constexpr int EXIT_UNKNOWN_ERROR = 5;
constexpr int EXIT_CONFIG_ERROR = 6;
constexpr int EXIT_SIGNAL_ERROR = 7;

std::atomic<bool> g_shutdownRequested{false};
std::atomic<StreamingData *> g_streamingData{nullptr};

void cleanupLogger() {
  try {
    Logger::shutdown();
  } catch (...) {
  }
}

void signalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    static std::atomic<bool> shutdownInProgress{false};
    if (shutdownInProgress.exchange(true)) {
      std::cerr << "\nForce shutdown requested, exiting immediately...\n";
      cleanupLogger();
      std::exit(EXIT_SUCCESS_CODE);
      return;
    }
    
    std::cerr << "\n\nShutdown signal received (Ctrl+C). Initiating graceful shutdown...\n";
    g_shutdownRequested.store(true);
    
    StreamingData *sd = g_streamingData.load();
    if (sd != nullptr) {
      Logger::info(LogCategory::SYSTEM, "signalHandler",
                   "Shutdown signal received, initiating graceful shutdown...");
      try {
        sd->shutdown();
      } catch (...) {
        std::cerr << "Error during shutdown, forcing exit...\n";
        cleanupLogger();
        std::exit(EXIT_SUCCESS_CODE);
      }
    }
  }
}
} // namespace

int handleDBTCommand(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "Usage: DataSync --execute-dbt-model <modelName>" << std::endl;
    std::cerr << "       DataSync --run-dbt-tests <modelName>" << std::endl;
    std::cerr << "       DataSync --compile-dbt-model <modelName>" << std::endl;
    return 1;
  }
  
  std::string command = argv[1];
  std::string modelName = argv[2];
  
  try {
    DatabaseConfig::loadFromFile("config.json");
    if (!DatabaseConfig::isInitialized()) {
      std::cerr << "Error: Database configuration failed to initialize." << std::endl;
      return 1;
    }
    
    Logger::initialize();
    
    std::string metadataConnStr = DatabaseConfig::getPostgresConnectionString();
    DBTExecutor executor(metadataConnStr);
    
    if (command == "--execute-dbt-model") {
      executor.executeModel(modelName);
      nlohmann::json output;
      output["success"] = true;
      output["message"] = "Model executed successfully";
      std::cout << output.dump(2) << std::endl;
      Logger::shutdown();
      return 0;
      
    } else if (command == "--run-dbt-tests") {
      executor.runAllTests(modelName);
      nlohmann::json output;
      output["success"] = true;
      output["message"] = "Tests executed successfully";
      std::cout << output.dump(2) << std::endl;
      Logger::shutdown();
      return 0;
      
    } else if (command == "--compile-dbt-model") {
      std::string compiledSQL = executor.compileModel(modelName);
      nlohmann::json output;
      output["success"] = true;
      output["compiled_sql"] = compiledSQL;
      std::cout << output.dump(2) << std::endl;
      Logger::shutdown();
      return 0;
      
    } else {
      std::cerr << "Unknown DBT command: " << command << std::endl;
      Logger::shutdown();
      return 1;
    }
    
  } catch (const std::exception& e) {
    nlohmann::json output;
    output["success"] = false;
    output["error"] = e.what();
    std::cerr << output.dump(2) << std::endl;
    Logger::shutdown();
    return 1;
  }
}

int handleSecurityCommand() {
  try {
    DatabaseConfig::loadFromFile("config.json");
    if (!DatabaseConfig::isInitialized()) {
      nlohmann::json output;
      output["success"] = false;
      output["error"] = "Database configuration failed to initialize";
      std::cerr << output.dump(2) << std::endl;
      return 1;
    }
    
    Logger::initialize();
    
    std::string metadataConnStr = DatabaseConfig::getPostgresConnectionString();
    
    // Read JSON from stdin
    std::string input;
    std::string line;
    while (std::getline(std::cin, line)) {
      input += line + "\n";
    }
    
    if (input.empty()) {
      nlohmann::json output;
      output["success"] = false;
      output["error"] = "No input provided";
      std::cerr << output.dump(2) << std::endl;
      Logger::shutdown();
      return 1;
    }
    
    nlohmann::json request = nlohmann::json::parse(input);
    std::string operation = request.value("operation", "");
    nlohmann::json output;
    
    if (operation == "masking_apply") {
      DynamicMaskingEngine engine(metadataConnStr);
      std::string value = request["value"];
      std::string schemaName = request["schema_name"];
      std::string tableName = request["table_name"];
      std::string columnName = request["column_name"];
      std::string username = request.value("username", "anonymous");
      std::vector<std::string> userRoles;
      if (request.contains("user_roles")) {
        for (const auto& role : request["user_roles"]) {
          userRoles.push_back(role);
        }
      }
      
      std::string maskedValue = engine.applyMasking(value, schemaName, tableName, columnName, username, userRoles);
      output["success"] = true;
      output["masked_value"] = maskedValue;
      output["original_value"] = value;
      
    } else if (operation == "tokenize") {
      TokenizationManager manager(metadataConnStr);
      std::string value = request["value"];
      std::string tokenTypeStr = request.value("token_type", "reversible");
      std::string schemaName = request.value("schema_name", "");
      std::string tableName = request.value("table_name", "");
      std::string columnName = request.value("column_name", "");
      
      // Convert token type string to enum
      TokenizationManager::TokenType tokenType = TokenizationManager::TokenType::REVERSIBLE;
      bool reversible = true;
      if (tokenTypeStr == "irreversible") {
        tokenType = TokenizationManager::TokenType::IRREVERSIBLE;
        reversible = false;
      } else if (tokenTypeStr == "fpe") {
        tokenType = TokenizationManager::TokenType::FPE;
        reversible = true;
      }
      
      std::string token = manager.tokenize(value, schemaName, tableName, columnName, reversible, tokenType);
      output["success"] = true;
      output["token"] = token;
      
    } else if (operation == "detokenize") {
      TokenizationManager manager(metadataConnStr);
      std::string token = request["token"];
      std::string username = request.value("username", "anonymous");
      std::string schemaName = request.value("schema_name", "");
      std::string tableName = request.value("table_name", "");
      std::string columnName = request.value("column_name", "");
      std::string reason = request.value("reason", "");
      
      std::string originalValue = manager.detokenize(token, schemaName, tableName, columnName, username, reason);
      output["success"] = true;
      output["original_value"] = originalValue;
      
    } else if (operation == "anonymize") {
      AnonymizationEngine engine(metadataConnStr);
      std::string profileName = request["profile_name"];
      nlohmann::json data = request["data"];
      
      AnonymizationEngine::AnonymizationResult result = engine.anonymizeDataset(profileName, data);
      output["success"] = true;
      output["anonymized_data"] = result.anonymizedDataset;
      output["information_loss"] = result.informationLoss;
      
    } else if (operation == "validate_anonymization") {
      AnonymizationEngine engine(metadataConnStr);
      nlohmann::json data = request["dataset"];
      int k = request.value("k", 0);
      int l = request.value("l", 0);
      std::vector<std::string> quasiIdentifiers;
      if (request.contains("quasi_identifiers")) {
        for (const auto& qi : request["quasi_identifiers"]) {
          quasiIdentifiers.push_back(qi);
        }
      }
      std::string sensitiveAttribute = request.value("sensitive_attribute", "");
      
      bool kAnonymity = false;
      bool lDiversity = false;
      
      if (k > 0) {
        kAnonymity = engine.validateKAnonymity(data, k, quasiIdentifiers);
      }
      if (l > 0 && !sensitiveAttribute.empty()) {
        lDiversity = engine.validateLDiversity(data, k, l, quasiIdentifiers, sensitiveAttribute);
      }
      
      output["success"] = true;
      output["k_anonymity_achieved"] = kAnonymity;
      output["l_diversity_achieved"] = lDiversity;
      
    } else if (operation == "check_permission") {
      FineGrainedPermissions permissions(metadataConnStr);
      std::string username = request["username"];
      std::string schemaName = request["schema_name"];
      std::string tableName = request["table_name"];
      std::string columnName = request.value("column_name", "");
      std::string operationType = request["operation_type"];
      std::vector<std::string> userRoles;
      if (request.contains("user_roles")) {
        for (const auto& role : request["user_roles"]) {
          userRoles.push_back(role);
        }
      }
      
      bool allowed = permissions.checkColumnPermission(username, userRoles, schemaName, tableName, columnName, operationType);
      output["success"] = true;
      output["allowed"] = allowed;
      
    } else if (operation == "get_accessible_columns") {
      FineGrainedPermissions permissions(metadataConnStr);
      std::string username = request["username"];
      std::string schemaName = request["schema_name"];
      std::string tableName = request["table_name"];
      std::vector<std::string> userRoles;
      if (request.contains("user_roles")) {
        for (const auto& role : request["user_roles"]) {
          userRoles.push_back(role);
        }
      }
      
      std::vector<std::string> columns = permissions.getAccessibleColumns(username, userRoles, schemaName, tableName);
      output["success"] = true;
      output["columns"] = columns;
      
    } else if (operation == "get_row_filter") {
      FineGrainedPermissions permissions(metadataConnStr);
      std::string username = request["username"];
      std::string schemaName = request["schema_name"];
      std::string tableName = request["table_name"];
      std::vector<std::string> userRoles;
      if (request.contains("user_roles")) {
        for (const auto& role : request["user_roles"]) {
          userRoles.push_back(role);
        }
      }
      
      std::string filter = permissions.generateRowFilter(username, userRoles, schemaName, tableName);
      output["success"] = true;
      output["filter"] = filter;
      
    } else {
      output["success"] = false;
      output["error"] = "Unknown operation: " + operation;
    }
    
    std::cout << output.dump(2) << std::endl;
    Logger::shutdown();
    return output.value("success", false) ? 0 : 1;
    
  } catch (const std::exception& e) {
    nlohmann::json output;
    output["success"] = false;
    output["error"] = e.what();
    std::cerr << output.dump(2) << std::endl;
    Logger::shutdown();
    return 1;
  }
}

int handleMonitoringCommand() {
  try {
    DatabaseConfig::loadFromFile("config.json");
    if (!DatabaseConfig::isInitialized()) {
      nlohmann::json output;
      output["success"] = false;
      output["error"] = "Database configuration failed to initialize";
      std::cout << output.dump(2) << std::endl;
      return 1;
    }
    
    std::string metadataConnStr = DatabaseConfig::getPostgresConnectionString();
    std::string input;
    std::string line;
    while (std::getline(std::cin, line)) {
      input += line + "\n";
    }
    
    if (input.empty()) {
      nlohmann::json output;
      output["success"] = false;
      output["error"] = "No input provided";
      std::cout << output.dump(2) << std::endl;
      Logger::shutdown();
      return 1;
    }
    
    nlohmann::json request = nlohmann::json::parse(input);
    std::string operation = request.value("operation", "");
    nlohmann::json output;
    
    if (operation == "start_span") {
      DistributedTracingManager tracing(metadataConnStr);
      std::string traceId = request.value("trace_id", tracing.generateTraceId());
      std::string parentSpanId = request.value("parent_span_id", "");
      std::string operationName = request["operation_name"];
      std::string serviceName = request.value("service_name", "datasync");
      
      std::string spanId = tracing.startSpan(traceId, parentSpanId, operationName, serviceName);
      output["success"] = true;
      output["span_id"] = spanId;
      output["trace_id"] = traceId;
      
    } else if (operation == "end_span") {
      DistributedTracingManager tracing(metadataConnStr);
      std::string spanId = request["span_id"];
      std::string status = request.value("status", "ok");
      std::string errorMessage = request.value("error_message", "");
      
      bool success = tracing.endSpan(spanId, status, errorMessage);
      output["success"] = success;
      
    } else if (operation == "list_traces") {
      DistributedTracingManager tracing(metadataConnStr);
      std::string serviceName = request.value("service_name", "");
      int limit = request.value("limit", 100);
      
      auto traces = tracing.listTraces(serviceName, limit);
      output["success"] = true;
      output["traces"] = nlohmann::json::array();
      for (const auto& t : traces) {
        nlohmann::json traceJson;
        traceJson["trace_id"] = t.traceId;
        traceJson["service_name"] = t.serviceName;
        traceJson["span_count"] = t.spanCount;
        traceJson["duration_microseconds"] = t.durationMicroseconds;
        output["traces"].push_back(traceJson);
      }
      
    } else if (operation == "get_trace") {
      DistributedTracingManager tracing(metadataConnStr);
      std::string traceId = request["trace_id"];
      auto trace = tracing.getTrace(traceId);
      if (trace) {
        output["success"] = true;
        output["trace_id"] = trace->traceId;
        output["service_name"] = trace->serviceName;
        output["span_count"] = trace->spanCount;
        output["duration_microseconds"] = trace->durationMicroseconds;
        output["spans"] = nlohmann::json::array();
        for (const auto& span : trace->spans) {
          nlohmann::json spanJson;
          spanJson["span_id"] = span.spanId;
          spanJson["operation_name"] = span.operationName;
          spanJson["service_name"] = span.serviceName;
          spanJson["duration_microseconds"] = span.durationMicroseconds;
          spanJson["status"] = span.status;
          output["spans"].push_back(spanJson);
        }
      } else {
        output["success"] = false;
        output["error"] = "Trace not found";
      }
      
    } else if (operation == "record_request") {
      APMManager apm(metadataConnStr);
      std::string operationName = request["operation_name"];
      std::string serviceName = request.value("service_name", "datasync");
      int64_t latencyMs = request["latency_ms"];
      bool isError = request.value("is_error", false);
      
      apm.recordRequest(operationName, serviceName, latencyMs, isError);
      output["success"] = true;
      
    } else if (operation == "get_metrics") {
      APMManager apm(metadataConnStr);
      std::string operationName = request.value("operation_name", "");
      std::string timeWindow = request.value("time_window", "1min");
      
      auto metrics = apm.getMetrics(operationName, timeWindow);
      output["success"] = true;
      output["metrics"] = nlohmann::json::array();
      for (const auto& m : metrics) {
        nlohmann::json metricJson;
        metricJson["operation_name"] = m.operationName;
        metricJson["service_name"] = m.serviceName;
        metricJson["latency_p50"] = m.latencyP50;
        metricJson["latency_p95"] = m.latencyP95;
        metricJson["latency_p99"] = m.latencyP99;
        metricJson["throughput"] = m.throughput;
        metricJson["error_rate"] = m.errorRate;
        output["metrics"].push_back(metricJson);
      }
      
    } else if (operation == "analyze_bottlenecks") {
      BottleneckDetector detector(metadataConnStr);
      auto bottlenecks = detector.analyze();
      output["success"] = true;
      output["bottlenecks"] = nlohmann::json::array();
      for (const auto& b : bottlenecks) {
        nlohmann::json bottleneckJson;
        bottleneckJson["id"] = b.id;
        bottleneckJson["resource_type"] = b.resourceType;
        bottleneckJson["severity"] = b.severity;
        if (!b.component.empty()) bottleneckJson["component"] = b.component;
        bottleneckJson["description"] = b.description;
        bottleneckJson["recommendations"] = nlohmann::json::array();
        for (const auto& rec : b.recommendations) {
          bottleneckJson["recommendations"].push_back(rec);
        }
        std::time_t t = std::chrono::system_clock::to_time_t(b.detectedAt);
        std::tm* tm = std::gmtime(&t);
        char buf[32];
        if (tm && std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", tm)) {
          bottleneckJson["detected_at"] = std::string(buf);
        }
        output["bottlenecks"].push_back(bottleneckJson);
      }
      
    } else if (operation == "collect_resources") {
      ResourceTracker tracker(metadataConnStr);
      auto metrics = tracker.collectCurrentMetrics();
      tracker.saveMetrics(metrics);
      output["success"] = true;
      output["cpu_percent"] = metrics.cpuPercent;
      output["memory_percent"] = metrics.memoryPercent;
      output["db_connections"] = metrics.dbConnections;
      
    } else if (operation == "calculate_cost") {
      CostTracker costTracker(metadataConnStr);
      std::string workflowId = request.value("workflow_id", "");
      std::string operationName = request.value("operation_name", "");
      ResourceTracker tracker(metadataConnStr);
      auto metrics = tracker.collectCurrentMetrics();
      
      auto cost = costTracker.calculateOperationCost(workflowId, operationName, metrics);
      costTracker.saveCost(cost);
      output["success"] = true;
      output["total_cost"] = cost.totalCost;
      output["compute_cost"] = cost.computeCost;
      output["storage_cost"] = cost.storageCost;
      output["network_cost"] = cost.networkCost;
      
    } else if (operation == "export_logs") {
      LogAggregator aggregator(metadataConnStr);
      std::string configId = request["config_id"];
      int limit = request.value("limit", 1000);
      
      int exported = aggregator.exportLogs(configId, limit);
      output["success"] = true;
      output["logs_exported"] = exported;
      
    } else if (operation == "trigger_alert") {
      AdvancedAlertingManager alerting(metadataConnStr);
      std::string integrationId = request["integration_id"];
      Alert alert;
      alert.id = request.value("alert_id", 0);
      alert.alert_type = AlertType::CUSTOM;
      alert.severity = AlertSeverity::WARNING;
      alert.title = request["title"];
      alert.message = request.value("message", "");
      alert.source = request.value("source", "datasync");
      
      std::string incidentId = alerting.triggerAlert(integrationId, alert);
      output["success"] = !incidentId.empty();
      output["incident_id"] = incidentId;
      
    } else if (operation == "analyze_query") {
      QueryPerformanceAnalyzer analyzer(metadataConnStr);
      std::string queryId = request["query_id"];
      std::string queryText = request["query_text"];
      
      auto analysis = analyzer.analyzeQuery(queryId, queryText);
      if (analysis) {
        output["success"] = true;
        output["query_fingerprint"] = analysis->queryFingerprint;
        output["issues"] = nlohmann::json::array();
        for (const auto& issue : analysis->issues) {
          output["issues"].push_back(issue);
        }
        output["recommendations"] = nlohmann::json::array();
        for (const auto& rec : analysis->recommendations) {
          output["recommendations"].push_back(rec);
        }
      } else {
        output["success"] = false;
        output["error"] = "Failed to analyze query";
      }
      
    } else if (operation == "get_regressions") {
      QueryPerformanceAnalyzer analyzer(metadataConnStr);
      int days = request.value("days", 7);
      auto regs = analyzer.detectRegressions(days);
      output["success"] = true;
      output["regressions"] = nlohmann::json::array();
      for (const auto& r : regs) {
        nlohmann::json regJson;
        regJson["query_fingerprint"] = r.queryFingerprint;
        regJson["previous_avg_time"] = r.previousAvgTime;
        regJson["current_avg_time"] = r.currentAvgTime;
        regJson["regression_percent"] = r.regressionPercent;
        output["regressions"].push_back(regJson);
      }
      
    } else {
      output["success"] = false;
      output["error"] = "Unknown operation: " + operation;
    }
    
    std::cout << output.dump(2) << std::endl;
    Logger::shutdown();
    return output.value("success", false) ? 0 : 1;
    
  } catch (const std::exception& e) {
    nlohmann::json output;
    output["success"] = false;
    output["error"] = e.what();
    std::cout << output.dump(2) << std::endl;
    Logger::shutdown();
    return 1;
  }
}

int handleCatalogCommand() {
  try {
    DatabaseConfig::loadFromFile("config.json");
    if (!DatabaseConfig::isInitialized()) {
      nlohmann::json output;
      output["success"] = false;
      output["error"] = "Database configuration failed to initialize";
      std::cerr << output.dump(2) << std::endl;
      return 1;
    }
    
    std::string metadataConnStr = DatabaseConfig::getPostgresConnectionString();
    std::string input;
    std::string line;
    while (std::getline(std::cin, line)) {
      input += line + "\n";
    }
    
    if (input.empty()) {
      nlohmann::json output;
      output["success"] = false;
      output["error"] = "No input provided";
      std::cerr << output.dump(2) << std::endl;
      Logger::shutdown();
      return 1;
    }
    
    nlohmann::json request = nlohmann::json::parse(input);
    std::string operation = request.value("operation", "");
    nlohmann::json output;
    
    if (operation == "create_mapping") {
      DataLakeMappingManager mapping(metadataConnStr);
      DataLakeMappingManager::Mapping map;
      map.targetSchema = request["target_schema"];
      map.targetTable = request["target_table"];
      map.sourceSystem = request["source_system"];
      map.sourceConnection = request.value("source_connection", "");
      map.sourceSchema = request.value("source_schema", "");
      map.sourceTable = request.value("source_table", "");
      
      std::string refreshTypeStr = request.value("refresh_rate_type", "manual");
      if (refreshTypeStr == "scheduled") {
        map.refreshRateType = DataLakeMappingManager::RefreshRateType::SCHEDULED;
      } else if (refreshTypeStr == "real-time") {
        map.refreshRateType = DataLakeMappingManager::RefreshRateType::REAL_TIME;
      } else if (refreshTypeStr == "on-demand") {
        map.refreshRateType = DataLakeMappingManager::RefreshRateType::ON_DEMAND;
      } else {
        map.refreshRateType = DataLakeMappingManager::RefreshRateType::MANUAL;
      }
      
      map.refreshSchedule = request.value("refresh_schedule", "");
      map.refreshDurationAvg = 0.0;
      map.refreshSuccessCount = 0;
      map.refreshFailureCount = 0;
      map.refreshSuccessRate = 0.0;
      
      int mappingId = mapping.createOrUpdateMapping(map);
      output["success"] = mappingId > 0;
      output["mapping_id"] = mappingId;
      
    } else if (operation == "get_mapping") {
      DataLakeMappingManager mapping(metadataConnStr);
      std::string targetSchema = request["target_schema"];
      std::string targetTable = request["target_table"];
      
      auto map = mapping.getMapping(targetSchema, targetTable);
      if (map) {
        output["success"] = true;
        output["mapping_id"] = map->mappingId;
        output["target_schema"] = map->targetSchema;
        output["target_table"] = map->targetTable;
        output["source_system"] = map->sourceSystem;
        output["source_connection"] = map->sourceConnection;
        output["source_schema"] = map->sourceSchema;
        output["source_table"] = map->sourceTable;
        // refreshRateTypeToString is private, convert manually
        std::string refreshTypeStr = "manual";
        if (map->refreshRateType == DataLakeMappingManager::RefreshRateType::SCHEDULED) {
          refreshTypeStr = "scheduled";
        } else if (map->refreshRateType == DataLakeMappingManager::RefreshRateType::REAL_TIME) {
          refreshTypeStr = "real-time";
        } else if (map->refreshRateType == DataLakeMappingManager::RefreshRateType::ON_DEMAND) {
          refreshTypeStr = "on-demand";
        }
        output["refresh_rate_type"] = refreshTypeStr;
        output["refresh_schedule"] = map->refreshSchedule;
        output["refresh_success_rate"] = map->refreshSuccessRate;
      } else {
        output["success"] = false;
        output["error"] = "Mapping not found";
      }
      
    } else if (operation == "list_mappings") {
      DataLakeMappingManager mapping(metadataConnStr);
      std::string sourceSystem = request.value("source_system", "");
      std::string refreshTypeStr = request.value("refresh_rate_type", "");
      
      DataLakeMappingManager::RefreshRateType refreshType = DataLakeMappingManager::RefreshRateType::MANUAL;
      if (refreshTypeStr == "scheduled") {
        refreshType = DataLakeMappingManager::RefreshRateType::SCHEDULED;
      } else if (refreshTypeStr == "real-time") {
        refreshType = DataLakeMappingManager::RefreshRateType::REAL_TIME;
      } else if (refreshTypeStr == "on-demand") {
        refreshType = DataLakeMappingManager::RefreshRateType::ON_DEMAND;
      }
      
      auto mappings = mapping.listMappings(sourceSystem, refreshType);
      output["success"] = true;
      output["mappings"] = nlohmann::json::array();
      for (const auto& m : mappings) {
        nlohmann::json mapJson;
        mapJson["mapping_id"] = m.mappingId;
        mapJson["target_schema"] = m.targetSchema;
        mapJson["target_table"] = m.targetTable;
        mapJson["source_system"] = m.sourceSystem;
        std::string refreshTypeStr2 = "manual";
        if (m.refreshRateType == DataLakeMappingManager::RefreshRateType::SCHEDULED) {
          refreshTypeStr2 = "scheduled";
        } else if (m.refreshRateType == DataLakeMappingManager::RefreshRateType::REAL_TIME) {
          refreshTypeStr2 = "real-time";
        } else if (m.refreshRateType == DataLakeMappingManager::RefreshRateType::ON_DEMAND) {
          refreshTypeStr2 = "on-demand";
        }
        mapJson["refresh_rate_type"] = refreshTypeStr2;
        output["mappings"].push_back(mapJson);
      }
      
    } else if (operation == "record_refresh") {
      DataLakeMappingManager mapping(metadataConnStr);
      std::string targetSchema = request["target_schema"];
      std::string targetTable = request["target_table"];
      bool success = request.value("success", true);
      int64_t durationMs = request.value("duration_ms", 0);
      
      bool result = mapping.recordRefresh(targetSchema, targetTable, success, durationMs);
      output["success"] = result;
      
    } else if (operation == "track_access") {
      UnusedObjectsDetector detector(metadataConnStr);
      std::string objectTypeStr = request["object_type"];
      std::string schemaName = request["schema_name"];
      std::string objectName = request["object_name"];
      std::string accessType = request["access_type"];
      std::string userName = request.value("user_name", "");
      
      UnusedObjectsDetector::ObjectType objectType = UnusedObjectsDetector::ObjectType::TABLE;
      if (objectTypeStr == "view") {
        objectType = UnusedObjectsDetector::ObjectType::VIEW;
      } else if (objectTypeStr == "materialized_view") {
        objectType = UnusedObjectsDetector::ObjectType::MATERIALIZED_VIEW;
      }
      
      detector.trackAccess(objectType, schemaName, objectName, accessType, userName);
      output["success"] = true;
      
    } else if (operation == "detect_unused") {
      UnusedObjectsDetector detector(metadataConnStr);
      int daysThreshold = request.value("days_threshold", 90);
      std::string generatedBy = request.value("generated_by", "");
      
      auto report = detector.detectUnusedObjects(daysThreshold, generatedBy);
      output["success"] = true;
      output["report_id"] = report.reportId;
      output["total_unused_count"] = report.totalUnusedCount;
      output["unused_objects"] = nlohmann::json::array();
      for (const auto& obj : report.unusedObjects) {
        nlohmann::json objJson;
        // objectTypeToString is private, use string conversion
        std::string objTypeStr = "table";
        if (obj.objectType == UnusedObjectsDetector::ObjectType::VIEW) {
          objTypeStr = "view";
        } else if (obj.objectType == UnusedObjectsDetector::ObjectType::MATERIALIZED_VIEW) {
          objTypeStr = "materialized_view";
        }
        objJson["object_type"] = objTypeStr;
        objJson["schema_name"] = obj.schemaName;
        objJson["object_name"] = obj.objectName;
        objJson["days_since_last_access"] = obj.daysSinceLastAccess;
        objJson["dependencies"] = nlohmann::json::array();
        for (const auto& dep : obj.dependencies) {
          objJson["dependencies"].push_back(dep);
        }
        output["unused_objects"].push_back(objJson);
      }
      
    } else {
      output["success"] = false;
      output["error"] = "Unknown operation: " + operation;
    }
    
    std::cout << output.dump(2) << std::endl;
    Logger::shutdown();
    return output.value("success", false) ? 0 : 1;
    
  } catch (const std::exception& e) {
    nlohmann::json output;
    output["success"] = false;
    output["error"] = e.what();
    std::cerr << output.dump(2) << std::endl;
    Logger::shutdown();
    return 1;
  }
}

int handleMaintenanceCommand() {
  try {
    DatabaseConfig::loadFromFile("config.json");
    if (!DatabaseConfig::isInitialized()) {
      nlohmann::json output;
      output["success"] = false;
      output["error"] = "Database configuration failed to initialize";
      std::cerr << output.dump(2) << std::endl;
      return 1;
    }
    
    std::string metadataConnStr = DatabaseConfig::getPostgresConnectionString();
    std::string input;
    std::string line;
    while (std::getline(std::cin, line)) {
      input += line + "\n";
    }
    
    if (input.empty()) {
      nlohmann::json output;
      output["success"] = false;
      output["error"] = "No input provided";
      std::cerr << output.dump(2) << std::endl;
      Logger::shutdown();
      return 1;
    }
    
    nlohmann::json request = nlohmann::json::parse(input);
    std::string operation = request.value("operation", "");
    nlohmann::json output;
    
    if (operation == "create_cleanup_policy") {
      CDCCleanupManager cleanup(metadataConnStr);
      CDCCleanupManager::CleanupPolicy policy;
      policy.connectionString = request["connection_string"];
      policy.dbEngine = request["db_engine"];
      policy.retentionDays = request.value("retention_days", 30);
      policy.batchSize = request.value("batch_size", 10000);
      policy.enabled = request.value("enabled", true);
      
      int policyId = cleanup.createOrUpdatePolicy(policy);
      output["success"] = policyId > 0;
      output["policy_id"] = policyId;
      
    } else if (operation == "execute_cleanup") {
      CDCCleanupManager cleanup(metadataConnStr);
      std::string connectionString = request.value("connection_string", "");
      std::string dbEngine = request.value("db_engine", "");
      int policyId = request.value("policy_id", 0);
      
      CDCCleanupManager::CleanupResult result;
      if (policyId > 0) {
        result = cleanup.executeCleanup(policyId);
      } else if (!connectionString.empty() && !dbEngine.empty()) {
        result = cleanup.executeCleanup(connectionString, dbEngine);
      } else {
        output["success"] = false;
        output["error"] = "Must provide policy_id or connection_string+db_engine";
        std::cout << output.dump(2) << std::endl;
        Logger::shutdown();
        return 1;
      }
      
      output["success"] = result.status == "completed";
      output["cleanup_id"] = result.cleanupId;
      output["rows_deleted"] = result.rowsDeleted;
      output["tables_cleaned"] = result.tablesCleaned;
      output["status"] = result.status;
      
    } else if (operation == "get_cleanup_history") {
      CDCCleanupManager cleanup(metadataConnStr);
      std::string connectionString = request.value("connection_string", "");
      int limit = request.value("limit", 100);
      
      auto history = cleanup.getCleanupHistory(connectionString, limit);
      output["success"] = true;
      output["history"] = nlohmann::json::array();
      for (const auto& h : history) {
        nlohmann::json histJson;
        histJson["cleanup_id"] = h.cleanupId;
        histJson["connection_string"] = h.connectionString;
        histJson["rows_deleted"] = h.rowsDeleted;
        histJson["status"] = h.status;
        output["history"].push_back(histJson);
      }
      
    } else {
      output["success"] = false;
      output["error"] = "Unknown operation: " + operation;
    }
    
    std::cout << output.dump(2) << std::endl;
    Logger::shutdown();
    return output.value("success", false) ? 0 : 1;
    
  } catch (const std::exception& e) {
    nlohmann::json output;
    output["success"] = false;
    output["error"] = e.what();
    std::cerr << output.dump(2) << std::endl;
    Logger::shutdown();
    return 1;
  }
}

int main(int argc, char* argv[]) {
  if (argc > 1 && (std::string(argv[1]) == "--execute-dbt-model" || 
                   std::string(argv[1]) == "--run-dbt-tests" || 
                   std::string(argv[1]) == "--compile-dbt-model")) {
    return handleDBTCommand(argc, argv);
  }
  
  if (argc > 1 && std::string(argv[1]) == "--security") {
    return handleSecurityCommand();
  }
  
  if (argc > 1 && std::string(argv[1]) == "--monitoring") {
    return handleMonitoringCommand();
  }
  
  if (argc > 1 && std::string(argv[1]) == "--catalog") {
    return handleCatalogCommand();
  }
  
  if (argc > 1 && std::string(argv[1]) == "--maintenance") {
    return handleMaintenanceCommand();
  }
  try {
    DatabaseConfig::loadFromFile("config.json");

    if (!DatabaseConfig::isInitialized()) {
      std::cerr << "Error: Database configuration failed to initialize. "
                   "Please check config.json or environment variables."
                << std::endl;
      return EXIT_CONFIG_ERROR;
    }

    std::string postgresPassword = DatabaseConfig::getPostgresPassword();
    if (postgresPassword.empty()) {
      std::cerr << "Warning: PostgreSQL password is empty. "
                   "Database connections may fail."
                << std::endl;
    }

    Logger::initialize();

    if (std::signal(SIGINT, signalHandler) == SIG_ERR) {
      std::cerr << "Error: Failed to register SIGINT handler" << std::endl;
      cleanupLogger();
      return EXIT_SIGNAL_ERROR;
    }

    if (std::signal(SIGTERM, signalHandler) == SIG_ERR) {
      std::cerr << "Error: Failed to register SIGTERM handler" << std::endl;
      cleanupLogger();
      return EXIT_SIGNAL_ERROR;
    }

    Logger::info(LogCategory::SYSTEM, "main", "DataSync started");
    Logger::info(LogCategory::SYSTEM, "main",
                 "Configuration loaded from config.json (DB: " +
                     DatabaseConfig::getPostgresDB() + "@" +
                     DatabaseConfig::getPostgresHost() + ")");

    std::cout << "\n";
    std::cout << "██████╗  █████╗ ████████╗ █████╗ ███████╗██╗   ██╗███╗   ██╗ "
                 "██████╗\n";
    std::cout << "██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔════╝╚██╗ ██╔╝████╗  "
                 "██║██╔════╝\n";
    std::cout << "██║  ██║███████║   ██║   ███████║███████╗ ╚████╔╝ ██╔██╗ "
                 "██║██║     \n";
    std::cout << "██║  ██║██╔══██║   ██║   ██╔══██║╚════██║  ╚██╔╝  "
                 "██║╚██╗██║██║     \n";
    std::cout << "██████╔╝██║  ██║   ██║   ██║  ██║███████║   ██║   ██║ "
                 "╚████║╚██████╗\n";
    std::cout << "╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═══╝ "
                 "╚═════╝\n";
    std::cout << "\n";
    std::cout
        << "          Enterprise Data Synchronization & Replication System\n";
    std::cout << "                            Version 2.0.0\n";
    std::cout << "\n";

    StreamingData sd;
    g_streamingData.store(&sd);

    bool initialized = false;
    try {
      sd.initialize();
      initialized = true;
      Logger::info(LogCategory::SYSTEM, "main",
                   "StreamingData initialized successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::SYSTEM, "main",
                    "Exception during StreamingData initialization: " +
                        std::string(e.what()));
      std::cerr << "Initialization error: " << e.what() << std::endl;
      g_streamingData.store(nullptr);
      cleanupLogger();
      return EXIT_INIT_ERROR;
    }

    if (!initialized) {
      g_streamingData.store(nullptr);
      cleanupLogger();
      return EXIT_INIT_ERROR;
    }

    try {
      sd.run([&]() { return g_shutdownRequested.load(); });
      
      if (g_shutdownRequested.load()) {
        Logger::info(LogCategory::SYSTEM, "main",
                     "DataSync shutdown requested, cleaning up...");
        sd.shutdown();
        Logger::info(LogCategory::SYSTEM, "main",
                     "DataSync shutdown completed successfully");
      } else {
        Logger::info(LogCategory::SYSTEM, "main",
                     "DataSync completed successfully");
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::SYSTEM, "main",
                    "Exception during DataSync execution: " +
                        std::string(e.what()));
      std::cerr << "Execution error: " << e.what() << std::endl;
      if (g_shutdownRequested.load()) {
        try {
          sd.shutdown();
        } catch (...) {
        }
      }
      g_streamingData.store(nullptr);
      cleanupLogger();
      return EXIT_EXECUTION_ERROR;
    }

    g_streamingData.store(nullptr);
    cleanupLogger();
    return EXIT_SUCCESS_CODE;

  } catch (const std::exception &e) {
    std::cerr << "Critical error in main: " << e.what() << std::endl;
    g_streamingData.store(nullptr);
    cleanupLogger();
    return EXIT_CRITICAL_ERROR;
  } catch (...) {
    std::cerr << "Unknown critical error in main" << std::endl;
    g_streamingData.store(nullptr);
    cleanupLogger();
    return EXIT_UNKNOWN_ERROR;
  }
}
