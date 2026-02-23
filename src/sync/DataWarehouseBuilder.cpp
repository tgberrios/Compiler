#include "sync/DataWarehouseBuilder.h"
#include "engines/bigquery_engine.h"
#include "engines/mongodb_engine.h"
#ifdef HAVE_ORACLE
#include "engines/oracle_engine.h"
#endif
#include "engines/postgres_warehouse_engine.h"
#include "engines/redshift_engine.h"
#include "engines/snowflake_engine.h"
#include "governance/DataQuality.h"
#include "utils/connection_utils.h"
#include "utils/engine_factory.h"
#include "sync/MergeStrategyExecutor.h"
#include "sync/PartitioningManager.h"
#include "sync/DistributedProcessingManager.h"
#include "storage/ColumnarStorage.h"
#include "utils/MemoryManager.h"
#ifdef HAVE_SPARK
#include "engines/spark_engine.h"
#endif
#include <algorithm>
#include <bson/bson.h>
#include <chrono>
#include <iomanip>
#include <mongoc/mongoc.h>
#include <mysql/mysql.h>
#ifdef HAVE_ORACLE
#include <oci.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <unordered_set>

DataWarehouseBuilder::DataWarehouseBuilder(std::string metadataConnectionString)
    : metadataConnectionString_(std::move(metadataConnectionString)),
      warehouseRepo_(std::make_unique<DataWarehouseRepository>(
          metadataConnectionString_)) {
  warehouseRepo_->createTables();
  
  // Inicializar MergeStrategyExecutor y DistributedProcessingManager
#ifdef HAVE_SPARK
  auto sparkEngine = std::make_shared<SparkEngine>(SparkEngine::SparkConfig{});
  if (sparkEngine->initialize()) {
    mergeExecutor_ = std::make_unique<MergeStrategyExecutor>(sparkEngine);
    
    DistributedProcessingManager::ProcessingConfig distConfig;
    distConfig.sparkConfig.appName = "DataSync-Warehouse";
    distConfig.sparkConfig.masterUrl = "local[*]";
    distributedManager_ = std::make_unique<DistributedProcessingManager>(distConfig);
    distributedManager_->initialize();
  }
#endif

  // Initialize memory manager
  MemoryManager::MemoryLimit memLimit;
  memLimit.maxMemory = 4ULL * 1024 * 1024 * 1024;  // 4GB for warehouse operations
  memLimit.enableSpill = true;
  memLimit.spillDirectory = "/tmp/datasync_warehouse_spill";
  memoryManager_ = std::make_unique<MemoryManager>(memLimit);
  
  Logger::info(LogCategory::SYSTEM, "DataWarehouseBuilder",
               "DataWarehouseBuilder initialized with performance optimizations");
}

DataWarehouseBuilder::~DataWarehouseBuilder() = default;

void DataWarehouseBuilder::applyMergeStrategy(
    const DataWarehouseModel& warehouse,
    const std::string& tableName,
    const std::vector<json>& sourceData,
    MergeStrategyExecutor::MergeStrategy strategy) {
  
  if (!mergeExecutor_) {
    Logger::warning(LogCategory::TRANSFER, "applyMergeStrategy",
                    "MergeStrategyExecutor not available, using default merge");
    return;
  }
  
  MergeStrategyExecutor::MergeConfig mergeConfig;
  mergeConfig.targetTable = getLayerSchemaName(warehouse) + "." + tableName;
  mergeConfig.sourceTable = "source_data"; // Placeholder
  mergeConfig.strategy = strategy;
  mergeConfig.useDistributed = (distributedManager_ && distributedManager_->isSparkAvailable());
  
  // Extraer primary key columns desde warehouse model
  // Esto sería específico de cada dimensión/fact table
  
  mergeExecutor_->executeMerge(mergeConfig);
}

PartitioningManager::PartitionDetectionResult
DataWarehouseBuilder::detectWarehousePartitions(
    const DataWarehouseModel& warehouse,
    const std::vector<std::string>& columnNames,
    const std::vector<std::string>& columnTypes) {
  
  return PartitioningManager::detectPartitions(
    getLayerSchemaName(warehouse),
    "warehouse_table", // Placeholder
    columnNames,
    columnTypes
  );
}

void DataWarehouseBuilder::buildWarehouse(const std::string &warehouseName) {
  std::string errorMessage;

  try {
    DataWarehouseModel warehouse = warehouseRepo_->getWarehouse(warehouseName);
    if (warehouse.warehouse_name.empty()) {
      throw std::runtime_error("Warehouse not found: " + warehouseName);
    }

    if (!warehouse.active || !warehouse.enabled) {
      throw std::runtime_error("Warehouse is not active or enabled: " +
                               warehouseName);
    }

    validateWarehouseModel(warehouse);

    std::string layerStr = (warehouse.target_layer == DataLayer::BRONZE)
                               ? "BRONZE"
                               : (warehouse.target_layer == DataLayer::SILVER)
                                     ? "SILVER"
                                     : "GOLD";

    Logger::info(LogCategory::TRANSFER, "buildWarehouse",
                 "Starting warehouse build: " + warehouseName +
                     " (Layer: " + layerStr + ")");

    json logMetadata;
    logMetadata["warehouse_name"] = warehouseName;
    logMetadata["target_layer"] = layerStr;
    logToProcessLog(warehouseName, "IN_PROGRESS", 0, "", logMetadata);

    switch (warehouse.target_layer) {
    case DataLayer::BRONZE:
      buildBronzeLayer(warehouse);
      break;
    case DataLayer::SILVER:
      buildSilverLayer(warehouse);
      break;
    case DataLayer::GOLD:
      buildGoldLayer(warehouse);
      break;
    }

    auto endTime = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(endTime);
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    warehouseRepo_->updateBuildStatus(warehouseName, "SUCCESS", timeStr.str(),
                                      "");

    int64_t totalRowsProcessed =
        static_cast<int64_t>(warehouse.dimensions.size()) * 1000;
    totalRowsProcessed += static_cast<int64_t>(warehouse.facts.size()) * 1000;

    logMetadata["total_rows"] = totalRowsProcessed;
    logToProcessLog(warehouseName, "SUCCESS", totalRowsProcessed, "",
                    logMetadata);

    Logger::info(LogCategory::TRANSFER, "buildWarehouse",
                 "Warehouse build completed: " + warehouseName);

  } catch (const std::exception &e) {
    errorMessage = e.what();
    auto timeT =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    warehouseRepo_->updateBuildStatus(warehouseName, "ERROR", timeStr.str(),
                                      errorMessage);

    logToProcessLog(warehouseName, "ERROR", 0, errorMessage, json::object());

    Logger::error(LogCategory::TRANSFER, "buildWarehouse",
                  "Error building warehouse " + warehouseName + ": " +
                      errorMessage);
    throw;
  }
}

int64_t DataWarehouseBuilder::buildWarehouseAndGetLogId(
    const std::string &warehouseName) {
  auto startTime = std::chrono::high_resolution_clock::now();
  int64_t processLogId = 0;
  std::string errorMessage;
  int64_t totalRowsProcessed = 0;

  try {
    json logMetadata;
    logMetadata["warehouse_name"] = warehouseName;

    processLogId =
        logToProcessLog(warehouseName, "IN_PROGRESS", 0, "", logMetadata);

    buildWarehouse(warehouseName);

    DataWarehouseModel warehouse = warehouseRepo_->getWarehouse(warehouseName);
    totalRowsProcessed +=
        static_cast<int64_t>(warehouse.dimensions.size()) * 1000;
    totalRowsProcessed += static_cast<int64_t>(warehouse.facts.size()) * 1000;

    auto endTime = std::chrono::high_resolution_clock::now();
    auto timeT =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    warehouseRepo_->updateBuildStatus(warehouseName, "SUCCESS", timeStr.str(),
                                      "");
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime)
            .count();

    logMetadata["duration_seconds"] = duration;
    logMetadata["total_rows"] = totalRowsProcessed;

    processLogId = logToProcessLog(warehouseName, "SUCCESS", totalRowsProcessed,
                                   "", logMetadata);

    Logger::info(LogCategory::TRANSFER, "buildWarehouseAndGetLogId",
                 "Warehouse built successfully: " + warehouseName);

  } catch (const std::exception &e) {
    errorMessage = e.what();
    auto timeT =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    warehouseRepo_->updateBuildStatus(warehouseName, "ERROR", timeStr.str(),
                                      errorMessage);

    logToProcessLog(warehouseName, "ERROR", totalRowsProcessed, errorMessage,
                    json::object());
    Logger::error(LogCategory::TRANSFER, "buildWarehouseAndGetLogId",
                  "Error building warehouse: " + errorMessage);
    throw;
  }

  return processLogId;
}

void DataWarehouseBuilder::buildAllActiveWarehouses() {
  try {
    std::vector<DataWarehouseModel> warehouses =
        warehouseRepo_->getActiveWarehouses();
    Logger::info(LogCategory::TRANSFER, "buildAllActiveWarehouses",
                 "Found " + std::to_string(warehouses.size()) +
                     " active warehouses");

    for (const auto &warehouse : warehouses) {
      try {
        buildWarehouse(warehouse.warehouse_name);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "buildAllActiveWarehouses",
                      "Error building warehouse " + warehouse.warehouse_name +
                          ": " + std::string(e.what()));
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "buildAllActiveWarehouses",
                  "Error getting active warehouses: " + std::string(e.what()));
    throw;
  }
}

DataWarehouseModel
DataWarehouseBuilder::getWarehouse(const std::string &warehouseName) {
  return warehouseRepo_->getWarehouse(warehouseName);
}

std::unique_ptr<IWarehouseEngine> DataWarehouseBuilder::createWarehouseEngine(
    const std::string &targetDbEngine,
    const std::string &targetConnectionString) {
  if (targetDbEngine == "PostgreSQL") {
    return std::make_unique<PostgresWarehouseEngine>(targetConnectionString);
  } else if (targetDbEngine == "Redshift") {
    return std::make_unique<RedshiftEngine>(targetConnectionString);
  } else if (targetDbEngine == "Snowflake") {
    return std::make_unique<SnowflakeEngine>(targetConnectionString);
  } else if (targetDbEngine == "BigQuery") {
    return std::make_unique<BigQueryEngine>(targetConnectionString);
  } else {
    throw std::runtime_error("Unsupported target database engine: " +
                             targetDbEngine);
  }
}

void DataWarehouseBuilder::validateWarehouseModel(
    const DataWarehouseModel &warehouse) {
  if (warehouse.warehouse_name.empty()) {
    throw std::invalid_argument("Warehouse name cannot be empty");
  }

  if (warehouse.target_schema.empty()) {
    throw std::invalid_argument("Target schema cannot be empty");
  }

  if (warehouse.dimensions.empty() && warehouse.facts.empty()) {
    throw std::invalid_argument(
        "Warehouse must have at least one dimension or fact table");
  }

  for (const auto &dim : warehouse.dimensions) {
    if (dim.dimension_name.empty()) {
      throw std::invalid_argument("Dimension name cannot be empty");
    }
    if (dim.source_query.empty()) {
      throw std::invalid_argument("Dimension source_query cannot be empty");
    }
    if (dim.scd_type == DimensionType::TYPE_2) {
      if (dim.valid_from_column.empty() || dim.valid_to_column.empty() ||
          dim.is_current_column.empty()) {
        throw std::invalid_argument(
            "SCD Type 2 dimensions require valid_from, valid_to, and "
            "is_current columns");
      }
    }
  }

  for (const auto &fact : warehouse.facts) {
    if (fact.fact_name.empty()) {
      throw std::invalid_argument("Fact name cannot be empty");
    }
    if (fact.source_query.empty()) {
      throw std::invalid_argument("Fact source_query cannot be empty");
    }
  }
}

void DataWarehouseBuilder::buildDimensionTable(
    const DataWarehouseModel &warehouse, const DimensionTable &dimension) {

  Logger::info(LogCategory::TRANSFER, "buildDimensionTable",
               "Building dimension: " + dimension.dimension_name);

  createDimensionTableStructure(warehouse, dimension);

  std::vector<json> sourceData =
      executeSourceQuery(warehouse, dimension.source_query);

  switch (dimension.scd_type) {
  case DimensionType::TYPE_1:
    // TYPE_1 es equivalente a UPSERT - usar MergeStrategyExecutor si está disponible
    if (mergeExecutor_) {
      applyMergeStrategy(warehouse, dimension.target_table, sourceData, 
                        MergeStrategyExecutor::MergeStrategy::UPSERT);
    } else {
      applySCDType1(warehouse, dimension, sourceData);
    }
    break;
  case DimensionType::TYPE_2:
    applySCDType2(warehouse, dimension, sourceData);
    break;
  case DimensionType::TYPE_3:
    applySCDType3(warehouse, dimension, sourceData);
    break;
  // TYPE_4 y TYPE_6 se pueden agregar al enum DimensionType en el futuro
  // Por ahora, TYPE_1 con MergeStrategyExecutor proporciona funcionalidad similar
  }

  if (!dimension.index_columns.empty()) {
    createIndexes(warehouse, dimension.target_schema, dimension.target_table,
                  dimension.index_columns);
  }

  if (!dimension.partition_column.empty()) {
    createPartitions(warehouse, dimension.target_schema, dimension.target_table,
                     dimension.partition_column);
  }
}

void DataWarehouseBuilder::buildFactTable(const DataWarehouseModel &warehouse,
                                          const FactTable &fact) {

  Logger::info(LogCategory::TRANSFER, "buildFactTable",
               "Building fact table: " + fact.fact_name);

  createFactTableStructure(warehouse, fact);

  std::vector<json> sourceData =
      executeSourceQuery(warehouse, fact.source_query);

  loadFactFullLoad(warehouse, fact, sourceData);

  if (!fact.index_columns.empty()) {
    createIndexes(warehouse, fact.target_schema, fact.target_table,
                  fact.index_columns);
  }

  if (!fact.partition_column.empty()) {
    createPartitions(warehouse, fact.target_schema, fact.target_table,
                     fact.partition_column);
  }
}

void DataWarehouseBuilder::createDimensionTableStructure(
    const DataWarehouseModel &warehouse, const DimensionTable &dimension) {

  std::vector<json> sampleData =
      executeSourceQuery(warehouse, dimension.source_query);

  std::vector<std::string> columns;

  if (sampleData.empty()) {
    Logger::warning(
        LogCategory::TRANSFER, "createDimensionTableStructure",
        "No sample data for dimension: " + dimension.dimension_name +
            ", attempting to get schema from query");
    columns = getQueryColumnNames(warehouse, dimension.source_query);
    if (columns.empty()) {
      Logger::error(LogCategory::TRANSFER, "createDimensionTableStructure",
                    "Could not determine columns for dimension: " +
                        dimension.dimension_name);
      return;
    }
  } else {
    std::unordered_set<std::string> columnsSet;
    for (const auto &row : sampleData) {
      for (const auto &[key, value] : row.items()) {
        columnsSet.insert(key);
      }
    }
    columns = std::vector<std::string>(columnsSet.begin(), columnsSet.end());
  }

  try {
    auto engine = createWarehouseEngine(warehouse.target_db_engine,
                                        warehouse.target_connection_string);

    std::string schemaName = dimension.target_schema;
    engine->createSchema(schemaName);

    std::unordered_set<std::string> columnsSet(columns.begin(), columns.end());

    if (dimension.scd_type == DimensionType::TYPE_2) {
      if (columnsSet.find(dimension.valid_from_column) == columnsSet.end()) {
        columns.push_back(dimension.valid_from_column);
      }
      if (columnsSet.find(dimension.valid_to_column) == columnsSet.end()) {
        columns.push_back(dimension.valid_to_column);
      }
      if (columnsSet.find(dimension.is_current_column) == columnsSet.end()) {
        columns.push_back(dimension.is_current_column);
      }
    }

    std::vector<WarehouseColumnInfo> columnInfos;
    for (const auto &col : columns) {
      WarehouseColumnInfo colInfo;
      colInfo.name = col;
      colInfo.data_type = "TEXT";
      colInfo.is_nullable = true;
      colInfo.default_value = "";
      columnInfos.push_back(colInfo);
    }

    std::vector<std::string> primaryKeys;
    if (!dimension.business_keys.empty()) {
      primaryKeys = dimension.business_keys;
    }

    engine->createTable(schemaName, dimension.target_table, columnInfos,
                        primaryKeys);

    Logger::info(LogCategory::TRANSFER, "createDimensionTableStructure",
                 "Created/verified schema and table: " + schemaName + "." +
                     dimension.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createDimensionTableStructure",
                  "Error creating dimension table structure: " +
                      std::string(e.what()));
    throw;
  }
}

void DataWarehouseBuilder::createFactTableStructure(
    const DataWarehouseModel &warehouse, const FactTable &fact) {

  std::vector<json> sampleData =
      executeSourceQuery(warehouse, fact.source_query);

  std::vector<std::string> columns;

  if (sampleData.empty()) {
    Logger::warning(LogCategory::TRANSFER, "createFactTableStructure",
                    "No sample data for fact: " + fact.fact_name +
                        ", attempting to get schema from query");
    columns = getQueryColumnNames(warehouse, fact.source_query);
    if (columns.empty()) {
      Logger::error(LogCategory::TRANSFER, "createFactTableStructure",
                    "Could not determine columns for fact: " + fact.fact_name);
      return;
    }
  } else {
    std::unordered_set<std::string> columnsSet;
    for (const auto &row : sampleData) {
      for (const auto &[key, value] : row.items()) {
        columnsSet.insert(key);
      }
    }
    columns = std::vector<std::string>(columnsSet.begin(), columnsSet.end());
  }

  try {
    auto engine = createWarehouseEngine(warehouse.target_db_engine,
                                        warehouse.target_connection_string);

    std::string schemaName = fact.target_schema;
    engine->createSchema(schemaName);

    std::vector<WarehouseColumnInfo> columnInfos;
    for (const auto &col : columns) {
      WarehouseColumnInfo colInfo;
      colInfo.name = col;
      colInfo.data_type = "TEXT";
      colInfo.is_nullable = true;
      colInfo.default_value = "";
      columnInfos.push_back(colInfo);
    }

    std::vector<std::string> primaryKeys;

    engine->createTable(schemaName, fact.target_table, columnInfos,
                        primaryKeys);

    Logger::info(LogCategory::TRANSFER, "createFactTableStructure",
                 "Created/verified schema and table: " + schemaName + "." +
                     fact.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createFactTableStructure",
                  "Error creating fact table structure: " +
                      std::string(e.what()));
    throw;
  }
}

std::vector<json>
DataWarehouseBuilder::executeSourceQuery(const DataWarehouseModel &warehouse,
                                         const std::string &query) {

  if (warehouse.source_db_engine == "PostgreSQL") {
    return executeQueryPostgreSQL(warehouse.source_connection_string, query);
  } else if (warehouse.source_db_engine == "MariaDB") {
    return executeQueryMariaDB(warehouse.source_connection_string, query);
  } else if (warehouse.source_db_engine == "MSSQL") {
    return executeQueryMSSQL(warehouse.source_connection_string, query);
  } else if (warehouse.source_db_engine == "Oracle") {
#ifdef HAVE_ORACLE
    return executeQueryOracle(warehouse.source_connection_string, query);
#else
    throw std::runtime_error("Oracle support not compiled in");
#endif
  } else if (warehouse.source_db_engine == "MongoDB") {
    return executeQueryMongoDB(warehouse.source_connection_string, query);
  } else {
    throw std::runtime_error("Unsupported source database engine: " +
                             warehouse.source_db_engine);
  }
}

std::vector<std::string>
DataWarehouseBuilder::getQueryColumnNames(const DataWarehouseModel &warehouse,
                                          const std::string &query) {
  std::vector<std::string> columnNames;

  try {
    if (warehouse.source_db_engine == "PostgreSQL") {
      pqxx::connection conn(warehouse.source_connection_string);
      pqxx::work txn(conn);
      pqxx::result result = txn.exec(query + " LIMIT 0");
      if (result.columns() > 0) {
        for (int i = 0; i < result.columns(); ++i) {
          columnNames.push_back(result.column_name(i));
        }
      }
      txn.commit();
    } else if (warehouse.source_db_engine == "MariaDB") {
      auto params =
          ConnectionStringParser::parse(warehouse.source_connection_string);
      if (!params) {
        throw std::runtime_error("Failed to parse MariaDB connection string");
      }
      MYSQL *conn = mysql_init(nullptr);
      if (!conn) {
        throw std::runtime_error("Failed to initialize MySQL connection");
      }
      if (!mysql_real_connect(conn, params->host.c_str(), params->user.c_str(),
                              params->password.c_str(), params->db.c_str(),
                              std::stoi(params->port), nullptr, 0)) {
        std::string error = mysql_error(conn);
        mysql_close(conn);
        throw std::runtime_error("Failed to connect to MariaDB: " + error);
      }
      std::string queryWithLimit = query;
      if (query.find("LIMIT") == std::string::npos) {
        queryWithLimit += " LIMIT 0";
      }
      if (mysql_query(conn, queryWithLimit.c_str())) {
        std::string error = mysql_error(conn);
        mysql_close(conn);
        throw std::runtime_error("Failed to execute MariaDB query: " + error);
      }
      MYSQL_RES *result = mysql_store_result(conn);
      if (result) {
        int numFields = mysql_num_fields(result);
        MYSQL_FIELD *fields = mysql_fetch_fields(result);
        for (int i = 0; i < numFields; ++i) {
          columnNames.push_back(std::string(fields[i].name));
        }
        mysql_free_result(result);
      }
      mysql_close(conn);
    } else if (warehouse.source_db_engine == "MSSQL") {
      SQLHENV env = SQL_NULL_HENV;
      SQLHDBC dbc = SQL_NULL_HDBC;
      SQLHSTMT stmt = SQL_NULL_HSTMT;
      SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
      if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to allocate ODBC environment");
      }
      ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
      if (!SQL_SUCCEEDED(ret)) {
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        throw std::runtime_error("Failed to set ODBC version");
      }
      ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
      if (!SQL_SUCCEEDED(ret)) {
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        throw std::runtime_error("Failed to allocate ODBC connection");
      }
      std::vector<SQLCHAR> connStr(warehouse.source_connection_string.begin(),
                                   warehouse.source_connection_string.end());
      connStr.push_back('\0');
      SQLSMALLINT connStrLen = 0;
      ret = SQLDriverConnect(dbc, nullptr, connStr.data(), SQL_NTS, nullptr, 0,
                             &connStrLen, SQL_DRIVER_NOPROMPT);
      if (!SQL_SUCCEEDED(ret)) {
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        throw std::runtime_error("Failed to connect to MSSQL");
      }
      ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
      if (!SQL_SUCCEEDED(ret)) {
        SQLDisconnect(dbc);
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        throw std::runtime_error("Failed to allocate ODBC statement");
      }
      std::string queryWithLimit = query;
      if (query.find("TOP") == std::string::npos &&
          query.find("LIMIT") == std::string::npos) {
        size_t selectPos = query.find("SELECT");
        if (selectPos != std::string::npos) {
          queryWithLimit = query.substr(0, selectPos + 6) + " TOP 0 " +
                           query.substr(selectPos + 6);
        }
      }
      std::vector<SQLCHAR> queryVec(queryWithLimit.begin(),
                                    queryWithLimit.end());
      queryVec.push_back('\0');
      ret = SQLExecDirect(stmt, queryVec.data(), SQL_NTS);
      if (!SQL_SUCCEEDED(ret)) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        SQLDisconnect(dbc);
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        throw std::runtime_error("Failed to execute MSSQL query");
      }
      SQLSMALLINT numCols = 0;
      SQLNumResultCols(stmt, &numCols);
      for (SQLSMALLINT i = 1; i <= numCols; ++i) {
        SQLCHAR colName[256];
        SQLSMALLINT nameLen, dataType, decimalDigits, nullable;
        SQLULEN colSize;
        SQLDescribeCol(stmt, i, colName, sizeof(colName), &nameLen, &dataType,
                       &colSize, &decimalDigits, &nullable);
        columnNames.push_back(std::string((char *)colName, nameLen));
      }
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      SQLDisconnect(dbc);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
    } else {
      Logger::warning(LogCategory::TRANSFER, "getQueryColumnNames",
                      "Getting column names not yet implemented for: " +
                          warehouse.source_db_engine);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getQueryColumnNames",
                  "Error getting column names: " + std::string(e.what()));
  }

  return columnNames;
}

std::vector<json> DataWarehouseBuilder::executeQueryPostgreSQL(
    const std::string &connectionString, const std::string &query) {
  std::vector<json> results;
  try {
    pqxx::connection conn(connectionString);
    pqxx::work txn(conn);
    auto rows = txn.exec(query);

    if (rows.empty()) {
      return results;
    }

    std::vector<std::string> columnNames;
    for (const auto &field : rows[0]) {
      columnNames.push_back(field.name());
    }

    for (const auto &row : rows) {
      json rowObj = json::object();
      for (size_t i = 0; i < columnNames.size(); ++i) {
        if (row[i].is_null()) {
          rowObj[columnNames[i]] = nullptr;
        } else {
          std::string value = row[i].as<std::string>();
          if (value == "t" || value == "f" || value == "true" ||
              value == "false") {
            rowObj[columnNames[i]] = (value == "t" || value == "true");
          } else {
            try {
              if (value.find('.') != std::string::npos) {
                double numValue = std::stod(value);
                rowObj[columnNames[i]] = numValue;
              } else {
                int64_t numValue = std::stoll(value);
                rowObj[columnNames[i]] = numValue;
              }
            } catch (...) {
              rowObj[columnNames[i]] = value;
            }
          }
        }
      }
      results.push_back(rowObj);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryPostgreSQL",
                  "Error executing PostgreSQL query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::vector<json>
DataWarehouseBuilder::executeQueryMariaDB(const std::string &connectionString,
                                          const std::string &query) {
  std::vector<json> results;
  try {
    auto params = ConnectionStringParser::parse(connectionString);
    if (!params) {
      throw std::runtime_error("Failed to parse MariaDB connection string");
    }

    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
      throw std::runtime_error("Failed to initialize MySQL connection");
    }

    if (!mysql_real_connect(conn, params->host.c_str(), params->user.c_str(),
                            params->password.c_str(), params->db.c_str(),
                            std::stoi(params->port), nullptr, 0)) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to connect to MariaDB: " + error);
    }

    if (mysql_query(conn, query.c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Query failed: " + error);
    }

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res) {
      mysql_close(conn);
      return results;
    }

    unsigned int numFields = mysql_num_fields(res);
    MYSQL_FIELD *fields = mysql_fetch_fields(res);
    std::vector<std::string> columnNames;
    for (unsigned int i = 0; i < numFields; ++i) {
      columnNames.push_back(fields[i].name);
    }

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res))) {
      json rowObj = json::object();
      for (unsigned int i = 0; i < numFields; ++i) {
        if (row[i] == nullptr) {
          rowObj[columnNames[i]] = nullptr;
        } else {
          std::string value = row[i];
          unsigned long *lengths = mysql_fetch_lengths(res);
          if (lengths && lengths[i] > 0) {
            if (fields[i].type == MYSQL_TYPE_TINY ||
                fields[i].type == MYSQL_TYPE_SHORT ||
                fields[i].type == MYSQL_TYPE_INT24 ||
                fields[i].type == MYSQL_TYPE_LONG ||
                fields[i].type == MYSQL_TYPE_LONGLONG) {
              try {
                rowObj[columnNames[i]] = std::stoll(value);
              } catch (...) {
                rowObj[columnNames[i]] = value;
              }
            } else if (fields[i].type == MYSQL_TYPE_FLOAT ||
                       fields[i].type == MYSQL_TYPE_DOUBLE ||
                       fields[i].type == MYSQL_TYPE_DECIMAL ||
                       fields[i].type == MYSQL_TYPE_NEWDECIMAL) {
              try {
                rowObj[columnNames[i]] = std::stod(value);
              } catch (...) {
                rowObj[columnNames[i]] = value;
              }
            } else if (fields[i].type == MYSQL_TYPE_BIT) {
              rowObj[columnNames[i]] = (value == "1" || value == "true");
            } else {
              rowObj[columnNames[i]] = value;
            }
          } else {
            rowObj[columnNames[i]] = value;
          }
        }
      }
      results.push_back(rowObj);
    }

    mysql_free_result(res);
    mysql_close(conn);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryMariaDB",
                  "Error executing MariaDB query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::vector<json>
DataWarehouseBuilder::executeQueryMSSQL(const std::string &connectionString,
                                        const std::string &query) {
  std::vector<json> results;
  SQLHENV env = SQL_NULL_HENV;
  SQLHDBC dbc = SQL_NULL_HDBC;
  SQLRETURN ret;

  ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  if (!SQL_SUCCEEDED(ret)) {
    throw std::runtime_error("Failed to allocate ODBC environment");
  }

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    throw std::runtime_error("Failed to set ODBC version");
  }

  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    throw std::runtime_error("Failed to allocate ODBC connection");
  }

  std::vector<SQLCHAR> connStr(connectionString.begin(),
                               connectionString.end());
  connStr.push_back('\0');
  SQLCHAR outConnStr[1024];
  SQLSMALLINT outConnStrLen;

  ret =
      SQLDriverConnect(dbc, nullptr, connStr.data(), SQL_NTS, outConnStr,
                       sizeof(outConnStr), &outConnStrLen, SQL_DRIVER_NOPROMPT);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    throw std::runtime_error("Failed to connect to MSSQL");
  }

  SQLHSTMT stmt = SQL_NULL_HSTMT;
  ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  if (!SQL_SUCCEEDED(ret)) {
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    throw std::runtime_error("Failed to allocate ODBC statement");
  }

  std::vector<SQLCHAR> queryVec(query.begin(), query.end());
  queryVec.push_back('\0');
  ret = SQLExecDirect(stmt, queryVec.data(), SQL_NTS);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    throw std::runtime_error("Failed to execute MSSQL query");
  }

  SQLSMALLINT numCols = 0;
  SQLNumResultCols(stmt, &numCols);
  std::vector<std::string> columnNames;
  std::vector<SQLSMALLINT> columnTypes(numCols);

  for (SQLSMALLINT i = 1; i <= numCols; ++i) {
    SQLCHAR colName[256];
    SQLSMALLINT nameLen, dataType, decimalDigits, nullable;
    SQLULEN colSize;
    SQLDescribeCol(stmt, i, colName, sizeof(colName), &nameLen, &dataType,
                   &colSize, &decimalDigits, &nullable);
    columnNames.push_back(std::string((char *)colName, nameLen));
    columnTypes[i - 1] = dataType;
  }

  while (SQL_SUCCEEDED(SQLFetch(stmt))) {
    json rowObj = json::object();
    for (SQLSMALLINT i = 1; i <= numCols; ++i) {
      SQLCHAR buffer[4096];
      SQLLEN indicator;
      ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
      if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        if (indicator == SQL_NULL_DATA) {
          rowObj[columnNames[i - 1]] = nullptr;
        } else {
          std::string value((char *)buffer, indicator);
          if (columnTypes[i - 1] == SQL_INTEGER ||
              columnTypes[i - 1] == SQL_BIGINT ||
              columnTypes[i - 1] == SQL_SMALLINT ||
              columnTypes[i - 1] == SQL_TINYINT) {
            try {
              rowObj[columnNames[i - 1]] = std::stoll(value);
            } catch (...) {
              rowObj[columnNames[i - 1]] = value;
            }
          } else if (columnTypes[i - 1] == SQL_DOUBLE ||
                     columnTypes[i - 1] == SQL_FLOAT ||
                     columnTypes[i - 1] == SQL_REAL ||
                     columnTypes[i - 1] == SQL_DECIMAL ||
                     columnTypes[i - 1] == SQL_NUMERIC) {
            try {
              rowObj[columnNames[i - 1]] = std::stod(value);
            } catch (...) {
              rowObj[columnNames[i - 1]] = value;
            }
          } else if (columnTypes[i - 1] == SQL_BIT) {
            rowObj[columnNames[i - 1]] = (value == "1" || value == "true");
          } else {
            rowObj[columnNames[i - 1]] = value;
          }
        }
      }
    }
    results.push_back(rowObj);
  }

  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  SQLDisconnect(dbc);
  SQLFreeHandle(SQL_HANDLE_DBC, dbc);
  SQLFreeHandle(SQL_HANDLE_ENV, env);

  return results;
}

#ifdef HAVE_ORACLE
std::vector<json>
DataWarehouseBuilder::executeQueryOracle(const std::string &connectionString,
                                         const std::string &query) {
  std::vector<json> results;
  try {
    auto conn = std::make_unique<OCIConnection>(connectionString);
    if (!conn->isValid()) {
      throw std::runtime_error("Failed to connect to Oracle");
    }

    OCIStmt *stmt = nullptr;
    OCIError *err = conn->getErr();
    OCISvcCtx *svc = conn->getSvc();
    OCIEnv *env = conn->getEnv();

    struct StmtGuard {
      OCIStmt *stmt_;
      StmtGuard(OCIStmt *s) : stmt_(s) {}
      ~StmtGuard() {
        if (stmt_) {
          OCIHandleFree(stmt_, OCI_HTYPE_STMT);
        }
      }
    };

    sword status = OCIHandleAlloc((dvoid *)env, (dvoid **)&stmt, OCI_HTYPE_STMT,
                                  0, nullptr);
    if (status != OCI_SUCCESS) {
      throw std::runtime_error("OCIHandleAlloc(STMT) failed");
    }

    StmtGuard guard(stmt);

    status = OCIStmtPrepare(stmt, err, (OraText *)query.c_str(), query.length(),
                            OCI_NTV_SYNTAX, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
      throw std::runtime_error("OCIStmtPrepare failed");
    }

    status =
        OCIStmtExecute(svc, stmt, err, 0, 0, nullptr, nullptr, OCI_DEFAULT);
    if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
      throw std::runtime_error("OCIStmtExecute failed");
    }

    ub4 numCols = 0;
    OCIAttrGet(stmt, OCI_HTYPE_STMT, &numCols, nullptr, OCI_ATTR_PARAM_COUNT,
               err);

    if (numCols == 0) {
      return results;
    }

    std::vector<OCIDefine *> defines(numCols);
    std::vector<std::vector<char>> buffers(numCols);
    std::vector<ub2> lengths(numCols, 0);
    std::vector<sb2> inds(numCols, -1);

    for (ub4 i = 0; i < numCols; ++i) {
      buffers[i].resize(4000, 0);
      status =
          OCIDefineByPos(stmt, &defines[i], err, i + 1, buffers[i].data(), 4000,
                         SQLT_STR, &inds[i], &lengths[i], nullptr, OCI_DEFAULT);
      if (status != OCI_SUCCESS) {
        throw std::runtime_error("OCIDefineByPos failed for column " +
                                 std::to_string(i + 1));
      }
    }

    std::vector<std::string> columnNames;
    for (ub4 i = 0; i < numCols; ++i) {
      columnNames.push_back("col" + std::to_string(i + 1));
    }

    while (OCIStmtFetch(stmt, err, 1, OCI_FETCH_NEXT, OCI_DEFAULT) ==
           OCI_SUCCESS) {
      json rowObj = json::object();
      for (ub4 i = 0; i < numCols; ++i) {
        if (inds[i] == -1) {
          rowObj[columnNames[i]] = nullptr;
        } else if (lengths[i] > 0 && lengths[i] <= 4000) {
          std::string value(buffers[i].data(), lengths[i]);
          try {
            if (value.find('.') != std::string::npos) {
              double numValue = std::stod(value);
              rowObj[columnNames[i]] = numValue;
            } else {
              int64_t numValue = std::stoll(value);
              rowObj[columnNames[i]] = numValue;
            }
          } catch (...) {
            rowObj[columnNames[i]] = value;
          }
        } else {
          rowObj[columnNames[i]] = "";
        }
      }
      results.push_back(rowObj);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryOracle",
                  "Error executing Oracle query: " + std::string(e.what()));
    throw;
  }
  return results;
}
#endif

std::vector<json>
DataWarehouseBuilder::executeQueryMongoDB(const std::string &connectionString,
                                          const std::string &query) {
  std::vector<json> results;
  try {
    MongoDBEngine engine(connectionString);
    if (!engine.isValid()) {
      throw std::runtime_error("Failed to connect to MongoDB");
    }

    bson_error_t error;
    bson_t *bsonQuery = bson_new_from_json((const uint8_t *)query.c_str(),
                                           query.length(), &error);
    if (!bsonQuery) {
      throw std::runtime_error("Failed to parse MongoDB query as JSON: " +
                               std::string(error.message));
    }

    std::string dbName = engine.getDatabaseName();
    if (dbName.empty()) {
      dbName = "admin";
    }

    mongoc_collection_t *coll = mongoc_client_get_collection(
        engine.getClient(), dbName.c_str(), "test");
    if (!coll) {
      bson_destroy(bsonQuery);
      throw std::runtime_error("Failed to get MongoDB collection");
    }

    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(coll, bsonQuery, nullptr, nullptr);

    const bson_t *doc;
    while (mongoc_cursor_next(cursor, &doc)) {
      char *jsonStr = bson_as_canonical_extended_json(doc, nullptr);
      if (jsonStr) {
        try {
          json docJson = json::parse(jsonStr);
          results.push_back(docJson);
        } catch (const json::parse_error &e) {
          Logger::warning(LogCategory::TRANSFER, "executeQueryMongoDB",
                          "Failed to parse document JSON: " +
                              std::string(e.what()));
        }
        bson_free(jsonStr);
      }
    }

    bson_error_t cursorError;
    if (mongoc_cursor_error(cursor, &cursorError)) {
      Logger::error(LogCategory::TRANSFER, "executeQueryMongoDB",
                    "MongoDB cursor error: " +
                        std::string(cursorError.message));
    }

    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(coll);
    bson_destroy(bsonQuery);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryMongoDB",
                  "Error executing MongoDB query: " + std::string(e.what()));
    throw;
  }
  return results;
}

void DataWarehouseBuilder::applySCDType1(const DataWarehouseModel &warehouse,
                                         const DimensionTable &dimension,
                                         const std::vector<json> &sourceData) {
  auto engine = createWarehouseEngine(warehouse.target_db_engine,
                                      warehouse.target_connection_string);

  std::string schemaName = dimension.target_schema;
  std::string tableName = dimension.target_table;

  if (sourceData.empty()) {
    return;
  }

  std::unordered_set<std::string> columnsSet;
  for (const auto &row : sourceData) {
    for (const auto &[key, value] : row.items()) {
      columnsSet.insert(key);
    }
  }
  std::vector<std::string> columns(columnsSet.begin(), columnsSet.end());

  size_t batchSize = 1000;
  for (size_t i = 0; i < sourceData.size(); i += batchSize) {
    std::vector<std::vector<std::string>> batchRows;
    for (size_t j = i; j < std::min(i + batchSize, sourceData.size()); ++j) {
      std::vector<std::string> row;
      for (const auto &col : columns) {
        if (sourceData[j].contains(col) && !sourceData[j][col].is_null()) {
          std::string value = sourceData[j][col].is_string()
                                  ? sourceData[j][col].get<std::string>()
                                  : sourceData[j][col].dump();
          row.push_back(value);
        } else {
          row.push_back("");
        }
      }
      batchRows.push_back(row);
    }

    if (!dimension.business_keys.empty()) {
      engine->upsertData(schemaName, tableName, columns,
                         dimension.business_keys, batchRows);
    } else {
      engine->insertData(schemaName, tableName, columns, batchRows);
    }
  }
}

void DataWarehouseBuilder::applySCDType2(const DataWarehouseModel &warehouse,
                                         const DimensionTable &dimension,
                                         const std::vector<json> &sourceData) {
  auto engine = createWarehouseEngine(warehouse.target_db_engine,
                                      warehouse.target_connection_string);

  std::string schemaName = dimension.target_schema;
  std::string tableName = dimension.target_table;

  auto now = std::chrono::system_clock::now();
  auto timeT = std::chrono::system_clock::to_time_t(now);
  std::ostringstream timeStr;
  timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

  if (sourceData.empty()) {
    return;
  }

  std::unordered_set<std::string> columnsSet;
  for (const auto &row : sourceData) {
    for (const auto &[key, value] : row.items()) {
      columnsSet.insert(key);
    }
  }
  std::vector<std::string> columns(columnsSet.begin(), columnsSet.end());

  std::vector<std::vector<std::string>> rowsToInsert;

  for (const auto &row : sourceData) {
    std::string businessKeyWhere = "";
    for (size_t i = 0; i < dimension.business_keys.size(); ++i) {
      if (i > 0)
        businessKeyWhere += " AND ";
      std::string key = dimension.business_keys[i];
      std::string value =
          row.contains(key) && !row[key].is_null()
              ? (row[key].is_string() ? row[key].get<std::string>()
                                      : row[key].dump())
              : "NULL";
      businessKeyWhere +=
          engine->quoteIdentifier(key) + " = " + engine->quoteValue(value);
    }

    std::string checkQuery =
        "SELECT COUNT(*) as count FROM " + engine->quoteIdentifier(schemaName) +
        "." + engine->quoteIdentifier(tableName) + " WHERE " +
        businessKeyWhere + " AND " +
        engine->quoteIdentifier(dimension.is_current_column) + " = true";

    auto result = engine->executeQuery(checkQuery);
    int existingCount = 0;
    if (!result.empty()) {
      json firstRow = result[0];
      if (firstRow.contains("count")) {
        if (firstRow["count"].is_number()) {
          existingCount = firstRow["count"].get<int>();
        } else if (firstRow["count"].is_string()) {
          try {
            existingCount = std::stoi(firstRow["count"].get<std::string>());
          } catch (...) {
            existingCount = 0;
          }
        }
      }
    }

    if (existingCount > 0) {
      std::string updateQuery =
          "UPDATE " + engine->quoteIdentifier(schemaName) + "." +
          engine->quoteIdentifier(tableName) + " SET " +
          engine->quoteIdentifier(dimension.is_current_column) + " = false, " +
          engine->quoteIdentifier(dimension.valid_to_column) + " = " +
          engine->quoteValue(timeStr.str()) + " WHERE " + businessKeyWhere +
          " AND " + engine->quoteIdentifier(dimension.is_current_column) +
          " = true";
      engine->executeStatement(updateQuery);
    }

    json newRow = row;
    newRow[dimension.valid_from_column] = timeStr.str();
    newRow[dimension.valid_to_column] = "9999-12-31 23:59:59";
    newRow[dimension.is_current_column] = "true";

    std::vector<std::string> rowData;
    for (const auto &col : columns) {
      if (newRow.contains(col) && !newRow[col].is_null()) {
        std::string value = newRow[col].is_string()
                                ? newRow[col].get<std::string>()
                                : newRow[col].dump();
        rowData.push_back(value);
      } else {
        rowData.push_back("");
      }
    }
    rowData.push_back(timeStr.str());
    rowData.push_back("9999-12-31 23:59:59");
    rowData.push_back("true");
    rowsToInsert.push_back(rowData);
  }

  if (!rowsToInsert.empty()) {
    std::vector<std::string> allColumns = columns;
    allColumns.push_back(dimension.valid_from_column);
    allColumns.push_back(dimension.valid_to_column);
    allColumns.push_back(dimension.is_current_column);

    size_t batchSize = 1000;
    for (size_t i = 0; i < rowsToInsert.size(); i += batchSize) {
      std::vector<std::vector<std::string>> batch(
          rowsToInsert.begin() + i,
          rowsToInsert.begin() + std::min(i + batchSize, rowsToInsert.size()));
      engine->insertData(schemaName, tableName, allColumns, batch);
    }
  }
}

void DataWarehouseBuilder::applySCDType3(const DataWarehouseModel &warehouse,
                                         const DimensionTable &dimension,
                                         const std::vector<json> &sourceData) {

  applySCDType1(warehouse, dimension, sourceData);
}

void DataWarehouseBuilder::loadFactFullLoad(
    const DataWarehouseModel &warehouse, const FactTable &fact,
    const std::vector<json> &sourceData) {
  auto engine = createWarehouseEngine(warehouse.target_db_engine,
                                      warehouse.target_connection_string);

  std::string schemaName = fact.target_schema;
  std::string tableName = fact.target_table;

  if (sourceData.empty()) {
    return;
  }

  std::unordered_set<std::string> columnsSet;
  for (const auto &row : sourceData) {
    for (const auto &[key, value] : row.items()) {
      columnsSet.insert(key);
    }
  }
  std::vector<std::string> columns(columnsSet.begin(), columnsSet.end());

  size_t batchSize = 1000;
  for (size_t i = 0; i < sourceData.size(); i += batchSize) {
    std::vector<std::vector<std::string>> batchRows;
    for (size_t j = i; j < std::min(i + batchSize, sourceData.size()); ++j) {
      std::vector<std::string> row;
      for (const auto &col : columns) {
        if (sourceData[j].contains(col) && !sourceData[j][col].is_null()) {
          std::string value = sourceData[j][col].is_string()
                                  ? sourceData[j][col].get<std::string>()
                                  : sourceData[j][col].dump();
          row.push_back(value);
        } else {
          row.push_back("");
        }
      }
      batchRows.push_back(row);
    }

    engine->insertData(schemaName, tableName, columns, batchRows);
  }
}

void DataWarehouseBuilder::createIndexes(
    const DataWarehouseModel &warehouse, const std::string &schemaName,
    const std::string &tableName,
    const std::vector<std::string> &indexColumns) {

  if (indexColumns.empty()) {
    return;
  }

  try {
    auto engine = createWarehouseEngine(warehouse.target_db_engine,
                                        warehouse.target_connection_string);
    for (const auto &col : indexColumns) {
      std::string idxName = "idx_" + tableName + "_" + col;
      engine->createIndex(schemaName, tableName, {col}, idxName);
    }
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::TRANSFER, "createIndexes",
                    "Error creating indexes: " + std::string(e.what()));
  }
}

void DataWarehouseBuilder::createPartitions(
    const DataWarehouseModel &warehouse, const std::string &schemaName,
    const std::string &tableName, const std::string &partitionColumn) {

  if (partitionColumn.empty()) {
    return;
  }

  try {
    auto engine = createWarehouseEngine(warehouse.target_db_engine,
                                        warehouse.target_connection_string);
    engine->createPartition(schemaName, tableName, partitionColumn);
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::TRANSFER, "createPartitions",
                    "Error creating partitions: " + std::string(e.what()));
  }
}

int64_t DataWarehouseBuilder::logToProcessLog(const std::string &warehouseName,
                                              const std::string &status,
                                              int64_t totalRowsProcessed,
                                              const std::string &errorMessage,
                                              const json &metadata) {

  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    auto now = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(now);
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    std::string metadataStr = metadata.dump();

    pqxx::params params;
    params.append(std::string("DATA_WAREHOUSE"));
    params.append(warehouseName);
    params.append(status);
    params.append(timeStr.str());
    params.append(timeStr.str());
    params.append(totalRowsProcessed);
    params.append(errorMessage);
    params.append(metadataStr);
    auto result = txn.exec(
        pqxx::zview("INSERT INTO metadata.process_log "
        "(process_type, process_name, status, "
        "start_time, end_time, total_rows_processed, error_message, metadata) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb) RETURNING id"),
        params);

    if (result.empty()) {
      txn.commit();
      return 0;
    }
    int64_t logId = result[0][0].as<int64_t>();
    txn.commit();
    return logId;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "logToProcessLog",
                  "Error logging to process log: " + std::string(e.what()));
    return 0;
  }
}

bool DataWarehouseBuilder::validateDataQuality(
    const DataWarehouseModel &warehouse) {
  try {
    pqxx::connection targetConn(warehouse.target_connection_string);
    DataQuality dataQuality;

    // Use the layer schema name instead of target_schema from dimensions/facts
    // This ensures we validate the correct layer (bronze/silver/gold)
    std::string layerSchema = getLayerSchemaName(warehouse);
    std::transform(layerSchema.begin(), layerSchema.end(), layerSchema.begin(),
                   ::tolower);

    for (const auto &dimension : warehouse.dimensions) {
      std::string tableName = dimension.target_table;
      std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                     ::tolower);

      auto metrics = dataQuality.collectMetrics(
          targetConn, layerSchema, tableName, warehouse.target_db_engine);

      if (metrics.quality_score < 70.0 ||
          metrics.validation_status == "FAILED") {
        Logger::warning(LogCategory::TRANSFER, "validateDataQuality",
                        "Data quality check failed for " + layerSchema + "." +
                            tableName + ": score=" +
                            std::to_string(metrics.quality_score) +
                            ", status=" + metrics.validation_status);
        return false;
      }
    }

    for (const auto &fact : warehouse.facts) {
      std::string tableName = fact.target_table;
      std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                     ::tolower);

      auto metrics = dataQuality.collectMetrics(
          targetConn, layerSchema, tableName, warehouse.target_db_engine);

      if (metrics.quality_score < 70.0 ||
          metrics.validation_status == "FAILED") {
        Logger::warning(LogCategory::TRANSFER, "validateDataQuality",
                        "Data quality check failed for " + layerSchema + "." +
                            tableName + ": score=" +
                            std::to_string(metrics.quality_score) +
                            ", status=" + metrics.validation_status);
        return false;
      }
    }

    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "validateDataQuality",
                  "Error validating data quality: " + std::string(e.what()));
    return false;
  }
}

void DataWarehouseBuilder::promoteToSilver(const std::string &warehouseName) {
  try {
    DataWarehouseModel warehouse = warehouseRepo_->getWarehouse(warehouseName);
    if (warehouse.warehouse_name.empty()) {
      throw std::runtime_error("Warehouse not found: " + warehouseName);
    }

    if (warehouse.target_layer != DataLayer::BRONZE) {
      std::string layerStr = (warehouse.target_layer == DataLayer::SILVER)
                                 ? "SILVER"
                                 : (warehouse.target_layer == DataLayer::GOLD)
                                       ? "GOLD"
                                       : "BRONZE";
      throw std::runtime_error(
          "Can only promote from BRONZE layer. Current layer: " + layerStr);
    }

    Logger::info(LogCategory::TRANSFER, "promoteToSilver",
                 "Promoting warehouse to SILVER: " + warehouseName);

    if (!validateDataQuality(warehouse)) {
      throw std::runtime_error(
          "Data quality validation failed. Cannot promote to SILVER.");
    }

    // Update target_layer to SILVER before building, so buildWarehouse calls buildSilverLayer
    warehouse.target_layer = DataLayer::SILVER;
    warehouseRepo_->insertOrUpdateWarehouse(warehouse);

    // Now build the SILVER layer
    buildWarehouse(warehouseName);

    Logger::info(LogCategory::TRANSFER, "promoteToSilver",
                 "Warehouse promoted to SILVER successfully: " +
                     warehouseName);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "promoteToSilver",
                  "Error promoting warehouse to SILVER: " +
                      std::string(e.what()));
    throw;
  }
}

void DataWarehouseBuilder::promoteToGold(const std::string &warehouseName) {
  try {
    DataWarehouseModel warehouse = warehouseRepo_->getWarehouse(warehouseName);
    if (warehouse.warehouse_name.empty()) {
      throw std::runtime_error("Warehouse not found: " + warehouseName);
    }

    if (warehouse.target_layer != DataLayer::SILVER) {
      std::string layerStr = (warehouse.target_layer == DataLayer::BRONZE)
                                 ? "BRONZE"
                                 : (warehouse.target_layer == DataLayer::GOLD)
                                       ? "GOLD"
                                       : "SILVER";
      throw std::runtime_error(
          "Can only promote from SILVER layer. Current layer: " + layerStr);
    }

    Logger::info(LogCategory::TRANSFER, "promoteToGold",
                 "Promoting warehouse to GOLD: " + warehouseName);

    // Update target_layer to GOLD before building, so buildWarehouse calls buildGoldLayer
    warehouse.target_layer = DataLayer::GOLD;
    warehouseRepo_->insertOrUpdateWarehouse(warehouse);

    // Now build the GOLD layer
    buildWarehouse(warehouseName);

    Logger::info(LogCategory::TRANSFER, "promoteToGold",
                 "Warehouse promoted to GOLD successfully: " + warehouseName);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "promoteToGold",
                  "Error promoting warehouse to GOLD: " +
                      std::string(e.what()));
    throw;
  }
}

std::string DataWarehouseBuilder::getLayerSchemaName(
    const DataWarehouseModel &warehouse) {
  std::string layerPrefix;
  switch (warehouse.target_layer) {
  case DataLayer::BRONZE:
    layerPrefix = "bronze";
    break;
  case DataLayer::SILVER:
    layerPrefix = "silver";
    break;
  case DataLayer::GOLD:
    layerPrefix = "gold";
    break;
  }
  std::string schemaName = warehouse.warehouse_name + "_" + layerPrefix;
  std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                 ::tolower);
  // Replace spaces with underscores to avoid SQL syntax errors
  std::replace(schemaName.begin(), schemaName.end(), ' ', '_');
  return schemaName;
}

std::string DataWarehouseBuilder::getSourceSchemaName(
    const DataWarehouseModel &warehouse) {
  std::string sourceLayer;
  switch (warehouse.target_layer) {
  case DataLayer::BRONZE:
    return "";
  case DataLayer::SILVER:
    sourceLayer = "bronze";
    break;
  case DataLayer::GOLD:
    sourceLayer = "silver";
    break;
  }
  std::string schemaName = warehouse.warehouse_name + "_" + sourceLayer;
  std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                 ::tolower);
  // Replace spaces with underscores to avoid SQL syntax errors
  std::replace(schemaName.begin(), schemaName.end(), ' ', '_');
  return schemaName;
}

void DataWarehouseBuilder::buildBronzeLayer(
    const DataWarehouseModel &warehouse) {
  Logger::info(LogCategory::TRANSFER, "buildBronzeLayer",
               "Building BRONZE layer (raw data) for warehouse: " +
                   warehouse.warehouse_name);

  auto engine = createWarehouseEngine(warehouse.target_db_engine,
                                      warehouse.target_connection_string);
  std::string layerSchema = getLayerSchemaName(warehouse);
  engine->createSchema(layerSchema);

  int64_t totalRows = 0;

  for (const auto &dimension : warehouse.dimensions) {
    Logger::info(LogCategory::TRANSFER, "buildBronzeLayer",
                 "Processing dimension: " + dimension.dimension_name);

    std::vector<json> rawData =
        executeSourceQuery(warehouse, dimension.source_query);

    if (rawData.empty()) {
      Logger::warning(LogCategory::TRANSFER, "buildBronzeLayer",
                      "No data returned for dimension: " +
                          dimension.dimension_name);
      continue;
    }

    std::unordered_set<std::string> columnsSet;
    for (const auto &row : rawData) {
      for (const auto &[key, value] : row.items()) {
        columnsSet.insert(key);
      }
    }
    std::vector<std::string> columns(columnsSet.begin(), columnsSet.end());

    buildBronzeTable(warehouse, dimension.target_table,
                     dimension.source_query, columns);
    totalRows += static_cast<int64_t>(rawData.size());
  }

  for (const auto &fact : warehouse.facts) {
    Logger::info(LogCategory::TRANSFER, "buildBronzeLayer",
                 "Processing fact: " + fact.fact_name);

    std::vector<json> rawData =
        executeSourceQuery(warehouse, fact.source_query);

    if (rawData.empty()) {
      Logger::warning(LogCategory::TRANSFER, "buildBronzeLayer",
                      "No data returned for fact: " + fact.fact_name);
      continue;
    }

    std::unordered_set<std::string> columnsSet;
    for (const auto &row : rawData) {
      for (const auto &[key, value] : row.items()) {
        columnsSet.insert(key);
      }
    }
    std::vector<std::string> columns(columnsSet.begin(), columnsSet.end());

    buildBronzeTable(warehouse, fact.target_table, fact.source_query, columns);
    totalRows += static_cast<int64_t>(rawData.size());
  }

  Logger::info(LogCategory::TRANSFER, "buildBronzeLayer",
               "BRONZE layer build completed. Total rows: " +
                   std::to_string(totalRows));
}

void DataWarehouseBuilder::buildBronzeTable(
    const DataWarehouseModel &warehouse, const std::string &tableName,
    const std::string &sourceQuery, const std::vector<std::string> &columns) {
  auto engine = createWarehouseEngine(warehouse.target_db_engine,
                                      warehouse.target_connection_string);
  std::string layerSchema = getLayerSchemaName(warehouse);
  engine->createSchema(layerSchema);

  std::vector<WarehouseColumnInfo> columnInfos;
  for (const auto &col : columns) {
    WarehouseColumnInfo colInfo;
    colInfo.name = col;
    colInfo.data_type = "TEXT";
    colInfo.is_nullable = true;
    colInfo.default_value = "";
    columnInfos.push_back(colInfo);
  }

  engine->createTable(layerSchema, tableName, columnInfos, {});

  std::vector<json> rawData = executeSourceQuery(warehouse, sourceQuery);

  if (rawData.empty()) {
    return;
  }

  size_t batchSize = 1000;
  for (size_t i = 0; i < rawData.size(); i += batchSize) {
    std::vector<std::vector<std::string>> batchRows;
    for (size_t j = i; j < std::min(i + batchSize, rawData.size()); ++j) {
      std::vector<std::string> row;
      for (const auto &col : columns) {
        if (rawData[j].contains(col) && !rawData[j][col].is_null()) {
          std::string value = rawData[j][col].is_string()
                                  ? rawData[j][col].get<std::string>()
                                  : rawData[j][col].dump();
          row.push_back(value);
        } else {
          row.push_back("");
        }
      }
      batchRows.push_back(row);
    }
    engine->insertData(layerSchema, tableName, columns, batchRows);
  }
}

std::vector<json> DataWarehouseBuilder::cleanAndValidateData(
    const std::vector<json> &rawData, const std::vector<std::string> &columns) {
  std::vector<json> cleanedData;
  std::unordered_set<std::string> seenHashes;

  for (const auto &row : rawData) {
    json cleanedRow = row;

    for (const auto &col : columns) {
      if (cleanedRow.contains(col)) {
        if (cleanedRow[col].is_string()) {
          std::string value = cleanedRow[col].get<std::string>();
          std::string trimmed = value;
          trimmed.erase(0, trimmed.find_first_not_of(" \t\r\n"));
          trimmed.erase(trimmed.find_last_not_of(" \t\r\n") + 1);
          if (trimmed != value) {
            cleanedRow[col] = trimmed;
          }
        }
      }
    }

    std::string rowHash = cleanedRow.dump();
    if (seenHashes.find(rowHash) == seenHashes.end()) {
      seenHashes.insert(rowHash);
      cleanedData.push_back(cleanedRow);
    }
  }

  return cleanedData;
}

void DataWarehouseBuilder::buildSilverLayer(
    const DataWarehouseModel &warehouse) {
  Logger::info(LogCategory::TRANSFER, "buildSilverLayer",
               "Building SILVER layer (cleaned & validated) for warehouse: " +
                   warehouse.warehouse_name);

  std::string sourceSchema = getSourceSchemaName(warehouse);
  if (sourceSchema.empty()) {
    throw std::runtime_error(
        "SILVER layer requires BRONZE layer to exist. Please build BRONZE "
        "layer first.");
  }

  auto engine = createWarehouseEngine(warehouse.target_db_engine,
                                      warehouse.target_connection_string);
  std::string layerSchema = getLayerSchemaName(warehouse);
  engine->createSchema(layerSchema);

  pqxx::connection targetConn(warehouse.target_connection_string);
  DataQuality dataQuality;

  int64_t totalRows = 0;

  for (const auto &dimension : warehouse.dimensions) {
    Logger::info(LogCategory::TRANSFER, "buildSilverLayer",
                 "Processing dimension: " + dimension.dimension_name);

    try {
      std::string sourceTable = sourceSchema + "." + dimension.target_table;
      std::string lowerSourceTable = sourceTable;
      std::transform(lowerSourceTable.begin(), lowerSourceTable.end(),
                     lowerSourceTable.begin(), ::tolower);
      std::string selectQuery = "SELECT * FROM " + lowerSourceTable;

      std::vector<json> rawData;
      try {
        rawData = engine->executeQuery(selectQuery);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "buildSilverLayer",
                      "Error reading from BRONZE layer for dimension " +
                          dimension.dimension_name + ": " + std::string(e.what()));
        continue;
      }

      if (rawData.empty()) {
        Logger::warning(LogCategory::TRANSFER, "buildSilverLayer",
                        "No data in BRONZE layer for dimension: " +
                            dimension.dimension_name);
        continue;
      }

      std::unordered_set<std::string> columnsSet;
      for (const auto &row : rawData) {
        for (const auto &[key, value] : row.items()) {
          columnsSet.insert(key);
        }
      }
      std::vector<std::string> columns(columnsSet.begin(), columnsSet.end());

      std::vector<json> cleanedData = cleanAndValidateData(rawData, columns);

      try {
        buildSilverTable(warehouse, dimension.target_table, columns, cleanedData);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "buildSilverLayer",
                      "Error building SILVER table for dimension " +
                          dimension.dimension_name + ": " + std::string(e.what()));
        continue;
      }

      std::string schemaName = layerSchema;
      std::string tableName = dimension.target_table;
      std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                     ::tolower);
      std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                     ::tolower);

      try {
        auto metrics = dataQuality.collectMetrics(targetConn, schemaName, tableName,
                                                   warehouse.target_db_engine);

        if (metrics.quality_score < 70.0) {
          Logger::warning(LogCategory::TRANSFER, "buildSilverLayer",
                          "Data quality score below threshold for dimension: " +
                              dimension.dimension_name +
                              " (score: " + std::to_string(metrics.quality_score) +
                              ")");
        }
      } catch (const std::exception &e) {
        Logger::warning(LogCategory::TRANSFER, "buildSilverLayer",
                        "Could not validate data quality for dimension: " +
                            dimension.dimension_name + ": " + std::string(e.what()));
      }

      totalRows += static_cast<int64_t>(cleanedData.size());
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "buildSilverLayer",
                    "Unexpected error processing dimension " +
                        dimension.dimension_name + ": " + std::string(e.what()));
      continue;
    }
  }

  for (const auto &fact : warehouse.facts) {
    Logger::info(LogCategory::TRANSFER, "buildSilverLayer",
                 "Processing fact: " + fact.fact_name);

    try {
      std::string sourceTable = sourceSchema + "." + fact.target_table;
      std::string lowerSourceTable = sourceTable;
      std::transform(lowerSourceTable.begin(), lowerSourceTable.end(),
                     lowerSourceTable.begin(), ::tolower);
      std::string selectQuery = "SELECT * FROM " + lowerSourceTable;

      std::vector<json> rawData;
      try {
        rawData = engine->executeQuery(selectQuery);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "buildSilverLayer",
                      "Error reading from BRONZE layer for fact " +
                          fact.fact_name + ": " + std::string(e.what()));
        continue;
      }

      if (rawData.empty()) {
        Logger::warning(LogCategory::TRANSFER, "buildSilverLayer",
                        "No data in BRONZE layer for fact: " + fact.fact_name);
        continue;
      }

      std::unordered_set<std::string> columnsSet;
      for (const auto &row : rawData) {
        for (const auto &[key, value] : row.items()) {
          columnsSet.insert(key);
        }
      }
      std::vector<std::string> columns(columnsSet.begin(), columnsSet.end());

      std::vector<json> cleanedData = cleanAndValidateData(rawData, columns);

      try {
        buildSilverTable(warehouse, fact.target_table, columns, cleanedData);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "buildSilverLayer",
                      "Error building SILVER table for fact " +
                          fact.fact_name + ": " + std::string(e.what()));
        continue;
      }

      totalRows += static_cast<int64_t>(cleanedData.size());
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "buildSilverLayer",
                    "Unexpected error processing fact " +
                        fact.fact_name + ": " + std::string(e.what()));
      continue;
    }
  }

  Logger::info(LogCategory::TRANSFER, "buildSilverLayer",
               "SILVER layer build completed. Total rows: " +
                   std::to_string(totalRows));
}

void DataWarehouseBuilder::buildSilverTable(
    const DataWarehouseModel &warehouse, const std::string &tableName,
    const std::vector<std::string> &columns,
    const std::vector<json> &cleanedData) {
  auto engine = createWarehouseEngine(warehouse.target_db_engine,
                                      warehouse.target_connection_string);
  std::string layerSchema = getLayerSchemaName(warehouse);

  engine->createSchema(layerSchema);

  std::vector<WarehouseColumnInfo> columnInfos;
  for (const auto &col : columns) {
    WarehouseColumnInfo colInfo;
    colInfo.name = col;
    colInfo.data_type = "TEXT";
    colInfo.is_nullable = true;
    colInfo.default_value = "";
    columnInfos.push_back(colInfo);
  }

  engine->createTable(layerSchema, tableName, columnInfos, {});

  if (cleanedData.empty()) {
    return;
  }

  size_t batchSize = 1000;
  for (size_t i = 0; i < cleanedData.size(); i += batchSize) {
    std::vector<std::vector<std::string>> batchRows;
    for (size_t j = i; j < std::min(i + batchSize, cleanedData.size()); ++j) {
      std::vector<std::string> row;
      for (const auto &col : columns) {
        if (cleanedData[j].contains(col) && !cleanedData[j][col].is_null()) {
          std::string value = cleanedData[j][col].is_string()
                                  ? cleanedData[j][col].get<std::string>()
                                  : cleanedData[j][col].dump();
          row.push_back(value);
        } else {
          row.push_back("");
        }
      }
      batchRows.push_back(row);
    }
    engine->insertData(layerSchema, tableName, columns, batchRows);
  }
}

bool DataWarehouseBuilder::tableExistsInSchema(
    const std::string &connectionString, const std::string &schemaName,
    const std::string &tableName) {
  try {
    pqxx::connection conn(connectionString);
    pqxx::work txn(conn);

    std::string lowerSchema = schemaName;
    std::string lowerTable = tableName;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    std::string query =
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = $1 AND table_name = $2)";
    pqxx::result result = txn.exec_params(query, lowerSchema, lowerTable);

    if (!result.empty()) {
      return result[0][0].as<bool>();
    }
    return false;
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::TRANSFER, "tableExistsInSchema",
                    "Error checking table existence: " + std::string(e.what()));
    return false;
  }
}

void DataWarehouseBuilder::buildGoldLayer(
    const DataWarehouseModel &warehouse) {
  Logger::info(LogCategory::TRANSFER, "buildGoldLayer",
               "Building GOLD layer (business-ready) for warehouse: " +
                   warehouse.warehouse_name);

  std::string sourceSchema = getSourceSchemaName(warehouse);
  if (sourceSchema.empty()) {
    throw std::runtime_error(
        "GOLD layer requires SILVER layer to exist. Please build SILVER "
        "layer first.");
  }

  auto engine = createWarehouseEngine(warehouse.target_db_engine,
                                      warehouse.target_connection_string);
  std::string layerSchema = getLayerSchemaName(warehouse);
  engine->createSchema(layerSchema);

  DataWarehouseModel modifiedWarehouse = warehouse;
  modifiedWarehouse.target_schema = layerSchema;

  for (const auto &dimension : warehouse.dimensions) {
    std::string lowerTableName = dimension.target_table;
    std::transform(lowerTableName.begin(), lowerTableName.end(),
                   lowerTableName.begin(), ::tolower);

    if (!tableExistsInSchema(warehouse.target_connection_string, sourceSchema,
                             lowerTableName)) {
      Logger::warning(
          LogCategory::TRANSFER, "buildGoldLayer",
          "Table " + sourceSchema + "." + lowerTableName +
              " does not exist in SILVER layer. Skipping dimension: " +
              dimension.dimension_name);
      continue;
    }

    DimensionTable modifiedDimension = dimension;
    modifiedDimension.target_schema = layerSchema;
    std::string sourceTable = sourceSchema + "." + dimension.target_table;
    std::string lowerSourceTable = sourceTable;
    std::transform(lowerSourceTable.begin(), lowerSourceTable.end(),
                   lowerSourceTable.begin(), ::tolower);
    modifiedDimension.source_query = "SELECT * FROM " + lowerSourceTable;

    buildDimensionTable(modifiedWarehouse, modifiedDimension);
  }

  for (const auto &fact : warehouse.facts) {
    std::string lowerTableName = fact.target_table;
    std::transform(lowerTableName.begin(), lowerTableName.end(),
                   lowerTableName.begin(), ::tolower);

    if (!tableExistsInSchema(warehouse.target_connection_string, sourceSchema,
                             lowerTableName)) {
      Logger::warning(
          LogCategory::TRANSFER, "buildGoldLayer",
          "Table " + sourceSchema + "." + lowerTableName +
              " does not exist in SILVER layer. Skipping fact: " +
              fact.fact_name);
      continue;
    }

    FactTable modifiedFact = fact;
    modifiedFact.target_schema = layerSchema;
    std::string sourceTable = sourceSchema + "." + fact.target_table;
    std::string lowerSourceTable = sourceTable;
    std::transform(lowerSourceTable.begin(), lowerSourceTable.end(),
                   lowerSourceTable.begin(), ::tolower);
    modifiedFact.source_query = "SELECT * FROM " + lowerSourceTable;

    buildFactTable(modifiedWarehouse, modifiedFact);
  }

  Logger::info(LogCategory::TRANSFER, "buildGoldLayer",
               "GOLD layer build completed (Star/Snowflake Schema applied)");
}
