#include "utils/engine_factory.h"
#include "core/logger.h"
#include "utils/string_utils.h"
#include "engines/mariadb_engine.h"
#include "engines/mongodb_engine.h"
#include "engines/mssql_engine.h"
#include "engines/oracle_engine.h"
#include "engines/postgres_engine.h"
#include "engines/salesforce_engine.h"
#include "engines/sap_engine.h"
#include "engines/teradata_engine.h"
#include "engines/netezza_engine.h"
#include "engines/hive_engine.h"
#include "engines/cassandra_engine.h"
#include "engines/dynamodb_engine.h"
#include "engines/as400_engine.h"

namespace EngineFactory {
std::unique_ptr<IDatabaseEngine>
createEngine(const std::string &dbEngine, const std::string &connectionString) {
  std::string normalized = StringUtils::toLower(dbEngine);
  if (normalized == "mariadb")
    return std::make_unique<MariaDBEngine>(connectionString);
  else if (normalized == "mssql")
    return std::make_unique<MSSQLEngine>(connectionString);
  else if (normalized == "postgresql")
    return std::make_unique<PostgreSQLEngine>(connectionString);
  else if (normalized == "mongodb")
    return std::make_unique<MongoDBEngine>(connectionString);
  else if (normalized == "oracle")
    return std::make_unique<OracleEngine>(connectionString);
  else if (normalized == "salesforce")
    return std::make_unique<SalesforceEngine>(connectionString);
  else if (normalized == "sap")
    return std::make_unique<SAPEngine>(connectionString);
  else if (normalized == "teradata")
    return std::make_unique<TeradataEngine>(connectionString);
  else if (normalized == "netezza")
    return std::make_unique<NetezzaEngine>(connectionString);
  else if (normalized == "hive")
    return std::make_unique<HiveEngine>(connectionString);
  else if (normalized == "cassandra")
    return std::make_unique<CassandraEngine>(connectionString);
  else if (normalized == "dynamodb")
    return std::make_unique<DynamoDBEngine>(connectionString);
  else if (normalized == "as400")
    return std::make_unique<AS400Engine>(connectionString);
  else {
    Logger::warning(LogCategory::DATABASE, "EngineFactory",
                    "Unsupported database engine: " + dbEngine);
    return nullptr;
  }
}
} // namespace EngineFactory
