#include "utils/table_utils.h"
#include "core/logger.h"
#include "utils/string_utils.h"

// Checks if a table exists in PostgreSQL by querying information_schema.tables.
// Converts schema and table names to lowercase for case-insensitive matching.
// Uses parameterized queries (txn.exec_params) with $1 and $2 placeholders to
// prevent SQL injection. Returns true if the table exists (COUNT(*) > 0),
// false otherwise. Logs errors and returns false if the query fails or any
// exception occurs. This is a utility function used throughout the codebase
// to verify table existence before performing operations on them.
bool TableUtils::tableExistsInPostgres(pqxx::connection &conn,
                                       const std::string &schema,
                                       const std::string &table) {
  if (schema.empty() || table.empty()) {
    return false;
  }

  if (!conn.is_open()) {
    Logger::error(LogCategory::DATABASE, "TableUtils",
                  "Connection is not open");
    return false;
  }

  try {
    pqxx::work txn(conn);

    std::string lowerSchema = StringUtils::toLower(schema);
    std::string lowerTable = StringUtils::toLower(table);

    pqxx::params params;
    params.append(lowerSchema);
    params.append(lowerTable);
    auto result =
        txn.exec(pqxx::zview("SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = $1 AND table_name = $2"),
                        params);

    bool exists = !result.empty() && !result[0][0].is_null() &&
                  result[0][0].as<int>() > 0;
    txn.commit();

    return exists;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "TableUtils",
                  "Error checking table existence: " + std::string(e.what()));
    return false;
  }
}
