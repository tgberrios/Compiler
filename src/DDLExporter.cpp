#include "DDLExporter.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>

void DDLExporter::exportAllDDL() {
  Logger::info("DDLExporter", "Starting DDL export process");

  try {
    createFolderStructure();
    getSchemasFromCatalog();

    Logger::info("DDLExporter", "Found " + std::to_string(schemas.size()) +
                                    " schemas to export");

    for (const auto &schema : schemas) {
      try {
        Logger::info("DDLExporter",
                     "Exporting DDL for schema: " + schema.schema_name + " (" +
                         schema.db_engine + ")");
        exportSchemaDDL(schema);
      } catch (const std::exception &e) {
        Logger::error("DDLExporter", "Error exporting schema " +
                                         schema.schema_name + ": " +
                                         std::string(e.what()));
      }
    }

    Logger::info("DDLExporter", "DDL export process completed successfully");
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error in DDL export process: " + std::string(e.what()));
  }
}

void DDLExporter::createFolderStructure() {
  try {
    std::filesystem::create_directories(exportPath);
    Logger::info("DDLExporter", "Created base export directory: " + exportPath);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error creating folder structure: " + std::string(e.what()));
  }
}

void DDLExporter::getSchemasFromCatalog() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string query =
        "SELECT DISTINCT schema_name, db_engine, connection_string "
        "FROM metadata.catalog "
        "WHERE db_engine IS NOT NULL "
        "ORDER BY db_engine, schema_name;";

    auto result = txn.exec(query);
    txn.commit();

    schemas.clear();
    for (const auto &row : result) {
      SchemaInfo schema;
      schema.schema_name = row[0].as<std::string>();
      schema.db_engine = row[1].as<std::string>();
      schema.database_name =
          schema.schema_name; // Use schema_name as database_name for now
      schema.connection_string = row[2].as<std::string>();
      schemas.push_back(schema);
    }

    Logger::info("DDLExporter", "Retrieved " + std::to_string(schemas.size()) +
                                    " schemas from catalog");
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error getting schemas from catalog: " +
                                     std::string(e.what()));
  }
}

void DDLExporter::exportSchemaDDL(const SchemaInfo &schema) {
  try {
    createEngineFolder(schema.db_engine);
    createDatabaseFolder(schema.db_engine, schema.database_name);
    createSchemaFolder(schema.db_engine, schema.database_name,
                       schema.schema_name);

    if (schema.db_engine == "MariaDB") {
      exportMariaDBDDL(schema);
    } else if (schema.db_engine == "PostgreSQL") {
      exportPostgreSQLDDL(schema);
    } else if (schema.db_engine == "MongoDB") {
      exportMongoDBDDL(schema);
    } else if (schema.db_engine == "MSSQL") {
      exportMSSQLDDL(schema);
    } else {
      Logger::warning("DDLExporter",
                      "Unknown database engine: " + schema.db_engine);
    }
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error exporting schema DDL: " + std::string(e.what()));
  }
}

void DDLExporter::createEngineFolder(const std::string &engine) {
  try {
    std::string enginePath = exportPath + "/" + sanitizeFileName(engine);
    std::filesystem::create_directories(enginePath);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error creating engine folder: " + std::string(e.what()));
  }
}

void DDLExporter::createDatabaseFolder(const std::string &engine,
                                       const std::string &database) {
  try {
    std::string dbPath = exportPath + "/" + sanitizeFileName(engine) + "/" +
                         sanitizeFileName(database);
    std::filesystem::create_directories(dbPath);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error creating database folder: " + std::string(e.what()));
  }
}

void DDLExporter::createSchemaFolder(const std::string &engine,
                                     const std::string &database,
                                     const std::string &schema) {
  try {
    std::string schemaPath = exportPath + "/" + sanitizeFileName(engine) + "/" +
                             sanitizeFileName(database) + "/" +
                             sanitizeFileName(schema);
    std::filesystem::create_directories(schemaPath + "/tables");
    std::filesystem::create_directories(schemaPath + "/indexes");
    std::filesystem::create_directories(schemaPath + "/constraints");
    std::filesystem::create_directories(schemaPath + "/functions");
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error creating schema folder: " + std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBDDL(const SchemaInfo &schema) {
  try {
    std::string connStr = getConnectionString(schema);
    pqxx::connection conn(connStr);
    pqxx::work txn(conn);

    std::string tablesQuery =
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = '" +
        escapeSQL(schema.schema_name) +
        "' "
        "AND table_type = 'BASE TABLE';";

    auto tablesResult = txn.exec(tablesQuery);

    for (const auto &tableRow : tablesResult) {
      std::string tableName = tableRow[0].as<std::string>();

      std::string createTableQuery = "SHOW CREATE TABLE `" +
                                     escapeSQL(schema.schema_name) + "`.`" +
                                     escapeSQL(tableName) + "`;";
      auto createResult = txn.exec(createTableQuery);

      if (!createResult.empty()) {
        std::string ddl = createResult[0][1].as<std::string>();
        saveTableDDL(schema.db_engine, schema.database_name, schema.schema_name,
                     tableName, ddl);
      }

      std::string indexesQuery = "SHOW INDEX FROM `" +
                                 escapeSQL(schema.schema_name) + "`.`" +
                                 escapeSQL(tableName) + "`;";
      auto indexesResult = txn.exec(indexesQuery);

      for (const auto &indexRow : indexesResult) {
        std::string indexName = indexRow[2].as<std::string>();
        std::string columnName = indexRow[4].as<std::string>();
        std::string nonUnique = indexRow[1].as<std::string>();

        std::string indexDDL = "CREATE ";
        if (nonUnique == "0") {
          indexDDL += "UNIQUE ";
        }
        indexDDL += "INDEX `" + indexName + "` ON `" + schema.schema_name +
                    "`.`" + tableName + "` (`" + columnName + "`);";

        saveIndexDDL(schema.db_engine, schema.database_name, schema.schema_name,
                     tableName, indexDDL);
      }
    }

    txn.commit();
    Logger::info("DDLExporter",
                 "Exported MariaDB DDL for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error exporting MariaDB DDL: " + std::string(e.what()));
  }
}

void DDLExporter::exportPostgreSQLDDL(const SchemaInfo &schema) {
  try {
    std::string connStr = getConnectionString(schema);
    pqxx::connection conn(connStr);
    pqxx::work txn(conn);

    std::string tablesQuery =
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = '" +
        escapeSQL(schema.schema_name) +
        "' "
        "AND table_type = 'BASE TABLE';";

    auto tablesResult = txn.exec(tablesQuery);

    for (const auto &tableRow : tablesResult) {
      std::string tableName = tableRow[0].as<std::string>();

      std::string createTableQuery =
          "SELECT 'CREATE TABLE \"' || schemaname || '\".\"' || tablename || "
          "'\" (' || "
          "string_agg(column_name || ' ' || data_type || "
          "CASE WHEN character_maximum_length IS NOT NULL THEN '(' || "
          "character_maximum_length || ')' ELSE '' END || "
          "CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END, ', ') || "
          "');' as ddl "
          "FROM information_schema.columns c "
          "JOIN pg_tables pt ON c.table_name = pt.tablename AND c.table_schema "
          "= pt.schemaname "
          "WHERE c.table_schema = '" +
          escapeSQL(schema.schema_name) +
          "' "
          "AND c.table_name = '" +
          escapeSQL(tableName) +
          "' "
          "GROUP BY schemaname, tablename;";

      auto createResult = txn.exec(createTableQuery);

      if (!createResult.empty()) {
        std::string ddl = createResult[0][0].as<std::string>();
        saveTableDDL(schema.db_engine, schema.database_name, schema.schema_name,
                     tableName, ddl);
      }

      std::string indexesQuery = "SELECT indexname, indexdef FROM pg_indexes "
                                 "WHERE schemaname = '" +
                                 escapeSQL(schema.schema_name) +
                                 "' "
                                 "AND tablename = '" +
                                 escapeSQL(tableName) + "';";

      auto indexesResult = txn.exec(indexesQuery);

      for (const auto &indexRow : indexesResult) {
        std::string indexDDL = indexRow[1].as<std::string>();
        saveIndexDDL(schema.db_engine, schema.database_name, schema.schema_name,
                     tableName, indexDDL);
      }
    }

    txn.commit();
    Logger::info("DDLExporter",
                 "Exported PostgreSQL DDL for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error exporting PostgreSQL DDL: " + std::string(e.what()));
  }
}

void DDLExporter::exportMongoDBDDL(const SchemaInfo &schema) {
  try {
    Logger::info("DDLExporter",
                 "MongoDB DDL export not implemented yet for schema: " +
                     schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error exporting MongoDB DDL: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLDDL(const SchemaInfo &schema) {
  try {
    Logger::info("DDLExporter",
                 "MSSQL DDL export not implemented yet for schema: " +
                     schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error exporting MSSQL DDL: " + std::string(e.what()));
  }
}

void DDLExporter::saveTableDDL(const std::string &engine,
                               const std::string &database,
                               const std::string &schema,
                               const std::string &table_name,
                               const std::string &ddl) {
  try {
    std::string filePath = exportPath + "/" + sanitizeFileName(engine) + "/" +
                           sanitizeFileName(database) + "/" +
                           sanitizeFileName(schema) + "/tables/" +
                           sanitizeFileName(table_name) + ".sql";

    std::ofstream file(filePath);
    if (file.is_open()) {
      file << "-- Table DDL for " << schema << "." << table_name << std::endl;
      file << "-- Engine: " << engine << std::endl;
      file << "-- Database: " << database << std::endl;
      file << "-- Generated: " << std::time(nullptr) << std::endl;
      file << std::endl;
      file << ddl << std::endl;
      file.close();

      Logger::debug("DDLExporter", "Saved table DDL: " + filePath);
    }
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error saving table DDL: " + std::string(e.what()));
  }
}

void DDLExporter::saveIndexDDL(const std::string &engine,
                               const std::string &database,
                               const std::string &schema,
                               const std::string &table_name,
                               const std::string &index_ddl) {
  try {
    std::string filePath = exportPath + "/" + sanitizeFileName(engine) + "/" +
                           sanitizeFileName(database) + "/" +
                           sanitizeFileName(schema) + "/indexes/" +
                           sanitizeFileName(table_name) + "_indexes.sql";

    std::ofstream file(filePath, std::ios::app);
    if (file.is_open()) {
      file << index_ddl << std::endl;
      file.close();

      Logger::debug("DDLExporter", "Saved index DDL: " + filePath);
    }
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error saving index DDL: " + std::string(e.what()));
  }
}

void DDLExporter::saveConstraintDDL(const std::string &engine,
                                    const std::string &database,
                                    const std::string &schema,
                                    const std::string &table_name,
                                    const std::string &constraint_ddl) {
  try {
    std::string filePath = exportPath + "/" + sanitizeFileName(engine) + "/" +
                           sanitizeFileName(database) + "/" +
                           sanitizeFileName(schema) + "/constraints/" +
                           sanitizeFileName(table_name) + "_constraints.sql";

    std::ofstream file(filePath, std::ios::app);
    if (file.is_open()) {
      file << constraint_ddl << std::endl;
      file.close();

      Logger::debug("DDLExporter", "Saved constraint DDL: " + filePath);
    }
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error saving constraint DDL: " + std::string(e.what()));
  }
}

void DDLExporter::saveFunctionDDL(const std::string &engine,
                                  const std::string &database,
                                  const std::string &schema,
                                  const std::string &function_name,
                                  const std::string &function_ddl) {
  try {
    std::string filePath = exportPath + "/" + sanitizeFileName(engine) + "/" +
                           sanitizeFileName(database) + "/" +
                           sanitizeFileName(schema) + "/functions/" +
                           sanitizeFileName(function_name) + ".sql";

    std::ofstream file(filePath);
    if (file.is_open()) {
      file << "-- Function DDL for " << schema << "." << function_name
           << std::endl;
      file << "-- Engine: " << engine << std::endl;
      file << "-- Database: " << database << std::endl;
      file << "-- Generated: " << std::time(nullptr) << std::endl;
      file << std::endl;
      file << function_ddl << std::endl;
      file.close();

      Logger::debug("DDLExporter", "Saved function DDL: " + filePath);
    }
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error saving function DDL: " + std::string(e.what()));
  }
}

std::string DDLExporter::getConnectionString(const SchemaInfo &schema) {
  return schema.connection_string;
}

std::string DDLExporter::escapeSQL(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

std::string DDLExporter::sanitizeFileName(const std::string &name) {
  std::string sanitized = name;
  std::replace(sanitized.begin(), sanitized.end(), ' ', '_');
  std::replace(sanitized.begin(), sanitized.end(), '/', '_');
  std::replace(sanitized.begin(), sanitized.end(), '\\', '_');
  std::replace(sanitized.begin(), sanitized.end(), ':', '_');
  std::replace(sanitized.begin(), sanitized.end(), '*', '_');
  std::replace(sanitized.begin(), sanitized.end(), '?', '_');
  std::replace(sanitized.begin(), sanitized.end(), '"', '_');
  std::replace(sanitized.begin(), sanitized.end(), '<', '_');
  std::replace(sanitized.begin(), sanitized.end(), '>', '_');
  std::replace(sanitized.begin(), sanitized.end(), '|', '_');
  return sanitized;
}
