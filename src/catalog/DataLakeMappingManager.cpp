#include "catalog/DataLakeMappingManager.h"
#include "core/logger.h"
#include <pqxx/pqxx>
#include <sstream>
#include <iomanip>
#include <ctime>

DataLakeMappingManager::DataLakeMappingManager(const std::string& connectionString)
    : connectionString_(connectionString) {
  // Crear tabla si no existe
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.datalake_mapping ("
        "mapping_id SERIAL PRIMARY KEY,"
        "target_schema VARCHAR(100) NOT NULL,"
        "target_table VARCHAR(100) NOT NULL,"
        "source_system VARCHAR(100) NOT NULL,"
        "source_connection VARCHAR(500),"
        "source_schema VARCHAR(100),"
        "source_table VARCHAR(100),"
        "refresh_rate_type VARCHAR(50) NOT NULL,"
        "refresh_schedule VARCHAR(255),"
        "last_refresh_at TIMESTAMP,"
        "next_refresh_at TIMESTAMP,"
        "refresh_duration_avg DECIMAL(10,2),"
        "refresh_success_count INTEGER DEFAULT 0,"
        "refresh_failure_count INTEGER DEFAULT 0,"
        "refresh_success_rate DECIMAL(5,2),"
        "metadata JSONB DEFAULT '{}'::jsonb,"
        "created_at TIMESTAMP DEFAULT NOW(),"
        "updated_at TIMESTAMP DEFAULT NOW(),"
        "UNIQUE(target_schema, target_table)"
        ")");

    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_datalake_mapping_source_system "
        "ON metadata.datalake_mapping(source_system)");

    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_datalake_mapping_refresh_rate_type "
        "ON metadata.datalake_mapping(refresh_rate_type)");

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::DATABASE, "DataLakeMappingManager",
                  "Error creating tables: " + std::string(e.what()));
  }
}

std::string DataLakeMappingManager::refreshRateTypeToString(RefreshRateType type) {
  switch (type) {
    case RefreshRateType::MANUAL:
      return "manual";
    case RefreshRateType::SCHEDULED:
      return "scheduled";
    case RefreshRateType::REAL_TIME:
      return "real-time";
    case RefreshRateType::ON_DEMAND:
      return "on-demand";
    default:
      return "manual";
  }
}

DataLakeMappingManager::RefreshRateType DataLakeMappingManager::stringToRefreshRateType(
    const std::string& str) {
  if (str == "scheduled") {
    return RefreshRateType::SCHEDULED;
  } else if (str == "real-time") {
    return RefreshRateType::REAL_TIME;
  } else if (str == "on-demand") {
    return RefreshRateType::ON_DEMAND;
  }
  return RefreshRateType::MANUAL;
}

int DataLakeMappingManager::createOrUpdateMapping(const Mapping& mapping) {
  return saveMappingToDatabase(mapping) ? mapping.mappingId : 0;
}

bool DataLakeMappingManager::saveMappingToDatabase(const Mapping& mapping) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string lastRefreshStr;
    if (mapping.lastRefreshAt.time_since_epoch().count() > 0) {
      auto timeT = std::chrono::system_clock::to_time_t(mapping.lastRefreshAt);
      std::tm tm = *std::localtime(&timeT);
      std::ostringstream oss;
      oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
      lastRefreshStr = oss.str();
    }

    std::string nextRefreshStr;
    if (mapping.nextRefreshAt.time_since_epoch().count() > 0) {
      auto timeT = std::chrono::system_clock::to_time_t(mapping.nextRefreshAt);
      std::tm tm = *std::localtime(&timeT);
      std::ostringstream oss;
      oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
      nextRefreshStr = oss.str();
    }

    if (mapping.mappingId > 0) {
      // Update
      txn.exec_params(
          "UPDATE metadata.datalake_mapping SET "
          "source_system = $1, source_connection = $2, source_schema = $3, "
          "source_table = $4, refresh_rate_type = $5, refresh_schedule = $6, "
          "last_refresh_at = $7::timestamp, next_refresh_at = $8::timestamp, "
          "refresh_duration_avg = $9, refresh_success_count = $10, "
          "refresh_failure_count = $11, refresh_success_rate = $12, metadata = $13, "
          "updated_at = NOW() "
          "WHERE mapping_id = $14",
          mapping.sourceSystem,
          mapping.sourceConnection.empty() ? nullptr : mapping.sourceConnection,
          mapping.sourceSchema.empty() ? nullptr : mapping.sourceSchema,
          mapping.sourceTable.empty() ? nullptr : mapping.sourceTable,
          refreshRateTypeToString(mapping.refreshRateType),
          mapping.refreshSchedule.empty() ? nullptr : mapping.refreshSchedule,
          lastRefreshStr == "NULL" ? nullptr : lastRefreshStr,
          nextRefreshStr == "NULL" ? nullptr : nextRefreshStr,
          mapping.refreshDurationAvg,
          mapping.refreshSuccessCount,
          mapping.refreshFailureCount,
          mapping.refreshSuccessRate,
          mapping.metadata.dump(),
          mapping.mappingId);
    } else {
      // Insert
      auto result = txn.exec_params(
          "INSERT INTO metadata.datalake_mapping "
          "(target_schema, target_table, source_system, source_connection, "
          "source_schema, source_table, refresh_rate_type, refresh_schedule, "
          "last_refresh_at, next_refresh_at, refresh_duration_avg, "
          "refresh_success_count, refresh_failure_count, refresh_success_rate, metadata) "
          "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::timestamp, $10::timestamp, "
          "$11, $12, $13, $14, $15) "
          "ON CONFLICT (target_schema, target_table) DO UPDATE SET "
          "source_system = EXCLUDED.source_system, "
          "source_connection = EXCLUDED.source_connection, "
          "source_schema = EXCLUDED.source_schema, "
          "source_table = EXCLUDED.source_table, "
          "refresh_rate_type = EXCLUDED.refresh_rate_type, "
          "refresh_schedule = EXCLUDED.refresh_schedule, "
          "last_refresh_at = EXCLUDED.last_refresh_at, "
          "next_refresh_at = EXCLUDED.next_refresh_at, "
          "refresh_duration_avg = EXCLUDED.refresh_duration_avg, "
          "refresh_success_count = EXCLUDED.refresh_success_count, "
          "refresh_failure_count = EXCLUDED.refresh_failure_count, "
          "refresh_success_rate = EXCLUDED.refresh_success_rate, "
          "metadata = EXCLUDED.metadata, updated_at = NOW() "
          "RETURNING mapping_id",
          mapping.targetSchema, mapping.targetTable, mapping.sourceSystem,
          mapping.sourceConnection.empty() ? nullptr : mapping.sourceConnection,
          mapping.sourceSchema.empty() ? nullptr : mapping.sourceSchema,
          mapping.sourceTable.empty() ? nullptr : mapping.sourceTable,
          refreshRateTypeToString(mapping.refreshRateType),
          mapping.refreshSchedule.empty() ? nullptr : mapping.refreshSchedule,
          lastRefreshStr == "NULL" ? nullptr : lastRefreshStr,
          nextRefreshStr == "NULL" ? nullptr : nextRefreshStr,
          mapping.refreshDurationAvg,
          mapping.refreshSuccessCount,
          mapping.refreshFailureCount,
          mapping.refreshSuccessRate,
          mapping.metadata.dump());

      if (!result.empty()) {
        // mappingId will be set by caller if needed
      }
    }

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::DATABASE, "DataLakeMappingManager",
                  "Error saving mapping: " + std::string(e.what()));
    return false;
  }
}

std::unique_ptr<DataLakeMappingManager::Mapping> DataLakeMappingManager::getMapping(
    const std::string& targetSchema, const std::string& targetTable) {
  return loadMappingFromDatabase(targetSchema, targetTable);
}

std::unique_ptr<DataLakeMappingManager::Mapping> DataLakeMappingManager::loadMappingFromDatabase(
    const std::string& targetSchema, const std::string& targetTable) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params(
        "SELECT * FROM metadata.datalake_mapping "
        "WHERE target_schema = $1 AND target_table = $2",
        targetSchema, targetTable);

    if (result.empty()) {
      return nullptr;
    }

    auto row = result[0];
    auto mapping = std::make_unique<Mapping>();
    mapping->mappingId = row["mapping_id"].as<int>();
    mapping->targetSchema = row["target_schema"].as<std::string>();
    mapping->targetTable = row["target_table"].as<std::string>();
    mapping->sourceSystem = row["source_system"].as<std::string>();
    if (!row["source_connection"].is_null()) {
      mapping->sourceConnection = row["source_connection"].as<std::string>();
    }
    if (!row["source_schema"].is_null()) {
      mapping->sourceSchema = row["source_schema"].as<std::string>();
    }
    if (!row["source_table"].is_null()) {
      mapping->sourceTable = row["source_table"].as<std::string>();
    }
    mapping->refreshRateType = stringToRefreshRateType(row["refresh_rate_type"].as<std::string>());
    if (!row["refresh_schedule"].is_null()) {
      mapping->refreshSchedule = row["refresh_schedule"].as<std::string>();
    }
    if (!row["last_refresh_at"].is_null()) {
      auto lastRefreshStr = row["last_refresh_at"].as<std::string>();
      std::tm tm = {};
      std::istringstream ss(lastRefreshStr);
      ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
      mapping->lastRefreshAt = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    }
    if (!row["next_refresh_at"].is_null()) {
      auto nextRefreshStr = row["next_refresh_at"].as<std::string>();
      std::tm tm = {};
      std::istringstream ss(nextRefreshStr);
      ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
      mapping->nextRefreshAt = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    }
    if (!row["refresh_duration_avg"].is_null()) {
      mapping->refreshDurationAvg = row["refresh_duration_avg"].as<double>();
    }
    mapping->refreshSuccessCount = row["refresh_success_count"].as<int>();
    mapping->refreshFailureCount = row["refresh_failure_count"].as<int>();
    if (!row["refresh_success_rate"].is_null()) {
      mapping->refreshSuccessRate = row["refresh_success_rate"].as<double>();
    }
    if (!row["metadata"].is_null()) {
      mapping->metadata = json::parse(row["metadata"].as<std::string>());
    }

    return mapping;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::DATABASE, "DataLakeMappingManager",
                  "Error loading mapping: " + std::string(e.what()));
    return nullptr;
  }
}

std::vector<DataLakeMappingManager::Mapping> DataLakeMappingManager::listMappings(
    const std::string& sourceSystem, RefreshRateType refreshType) {
  std::vector<Mapping> mappings;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = "SELECT * FROM metadata.datalake_mapping WHERE 1=1";
    std::vector<std::string> params;

    if (!sourceSystem.empty()) {
      query += " AND source_system = $" + std::to_string(params.size() + 1);
      params.push_back(sourceSystem);
    }

    if (refreshType != RefreshRateType::MANUAL) {
      query += " AND refresh_rate_type = $" + std::to_string(params.size() + 1);
      params.push_back(refreshRateTypeToString(refreshType));
    }

    query += " ORDER BY target_schema, target_table";

    pqxx::result result;
    if (params.empty()) {
      result = txn.exec(query);
    } else {
      pqxx::params p;
      for (const auto& param : params) {
        p.append(param);
      }
      result = txn.exec(pqxx::zview(query), p);
    }

    for (const auto& row : result) {
      std::string targetSchema = row["target_schema"].as<std::string>();
      std::string targetTable = row["target_table"].as<std::string>();
      auto mapping = loadMappingFromDatabase(targetSchema, targetTable);
      if (mapping) {
        mappings.push_back(*mapping);
      }
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::DATABASE, "DataLakeMappingManager",
                  "Error listing mappings: " + std::string(e.what()));
  }

  return mappings;
}

bool DataLakeMappingManager::updateRefreshRate(const std::string& targetSchema,
                                               const std::string& targetTable,
                                               RefreshRateType refreshType,
                                               const std::string& refreshSchedule) {
  auto mapping = getMapping(targetSchema, targetTable);
  if (!mapping) {
    return false;
  }

  mapping->refreshRateType = refreshType;
  mapping->refreshSchedule = refreshSchedule;

  if (refreshType == RefreshRateType::SCHEDULED && !refreshSchedule.empty()) {
    mapping->nextRefreshAt = calculateNextRefresh(refreshSchedule);
  }

  return saveMappingToDatabase(*mapping);
}

bool DataLakeMappingManager::recordRefresh(const std::string& targetSchema,
                                          const std::string& targetTable,
                                          bool success,
                                          int64_t durationMs) {
  auto mapping = getMapping(targetSchema, targetTable);
  if (!mapping) {
    return false;
  }

  mapping->lastRefreshAt = std::chrono::system_clock::now();

  if (success) {
    mapping->refreshSuccessCount++;
  } else {
    mapping->refreshFailureCount++;
  }

  // Calcular promedio de duración (rolling average)
  int totalRefreshes = mapping->refreshSuccessCount + mapping->refreshFailureCount;
  if (totalRefreshes == 1) {
    mapping->refreshDurationAvg = durationMs;
  } else {
    mapping->refreshDurationAvg =
        (mapping->refreshDurationAvg * (totalRefreshes - 1) + durationMs) / totalRefreshes;
  }

  // Calcular success rate
  if (totalRefreshes > 0) {
    mapping->refreshSuccessRate =
        (static_cast<double>(mapping->refreshSuccessCount) / totalRefreshes) * 100.0;
  }

  // Calcular próxima actualización si es scheduled
  if (mapping->refreshRateType == RefreshRateType::SCHEDULED &&
      !mapping->refreshSchedule.empty()) {
    mapping->nextRefreshAt = calculateNextRefresh(mapping->refreshSchedule);
  }

  return saveMappingToDatabase(*mapping);
}

DataLakeMappingManager::RefreshStats DataLakeMappingManager::getRefreshStats() {
  RefreshStats stats = {};

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec("SELECT COUNT(*) as total, "
                          "COUNT(CASE WHEN refresh_rate_type = 'scheduled' THEN 1 END) as scheduled, "
                          "COUNT(CASE WHEN refresh_rate_type = 'real-time' THEN 1 END) as realtime, "
                          "AVG(refresh_success_rate) as avg_success_rate, "
                          "AVG(refresh_duration_avg) as avg_duration, "
                          "SUM(refresh_success_count + refresh_failure_count) as total_refreshes, "
                          "SUM(refresh_success_count) as successful, "
                          "SUM(refresh_failure_count) as failed "
                          "FROM metadata.datalake_mapping");

    if (!result.empty()) {
      auto row = result[0];
      stats.totalMappings = row["total"].as<int>();
      stats.scheduledMappings = row["scheduled"].as<int>();
      stats.realTimeMappings = row["realtime"].as<int>();
      if (!row["avg_success_rate"].is_null()) {
        stats.averageSuccessRate = row["avg_success_rate"].as<double>();
      }
      if (!row["avg_duration"].is_null()) {
        stats.averageDuration = row["avg_duration"].as<double>();
      }
      stats.totalRefreshes = row["total_refreshes"].as<int>();
      stats.successfulRefreshes = row["successful"].as<int>();
      stats.failedRefreshes = row["failed"].as<int>();
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::DATABASE, "DataLakeMappingManager",
                  "Error getting stats: " + std::string(e.what()));
  }

  return stats;
}

std::chrono::system_clock::time_point DataLakeMappingManager::calculateNextRefresh(
    const std::string& refreshSchedule) {
  // Por simplicidad, asumimos formato cron básico o intervalos simples
  // Para producción, usar librería de cron parsing
  auto now = std::chrono::system_clock::now();
  
  // Si es un número, asumimos que son horas
  try {
    int hours = std::stoi(refreshSchedule);
    return now + std::chrono::hours(hours);
  } catch (...) {
    // Si no es número, retornar now + 24 horas por defecto
    return now + std::chrono::hours(24);
  }
}
