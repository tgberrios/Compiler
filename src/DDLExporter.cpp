#include "DDLExporter.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <mysql/mysql.h>
#include <mongoc/mongoc.h>
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
        //Logger::info("DDLExporter",
                     //"Exporting DDL for schema: " + schema.schema_name + " (" +
                         //schema.db_engine + ")");
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
    
    // Parse MariaDB connection string
    std::string host, user, password, db, port;
    std::istringstream ss(connStr);
    std::string token;
    while (std::getline(ss, token, ';')) {
      auto pos = token.find('=');
      if (pos == std::string::npos)
        continue;
      std::string key = token.substr(0, pos);
      std::string value = token.substr(pos + 1);
      
      // Trim whitespace from key and value
      key.erase(0, key.find_first_not_of(" \t\r\n"));
      key.erase(key.find_last_not_of(" \t\r\n") + 1);
      value.erase(0, value.find_first_not_of(" \t\r\n"));
      value.erase(value.find_last_not_of(" \t\r\n") + 1);
      
      if (key == "host")
        host = value;
      else if (key == "user")
        user = value;
      else if (key == "password")
        password = value;
      else if (key == "db")
        db = value;
      else if (key == "port")
        port = value;
    }
    
    // Connect to MariaDB using MySQL library
    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
      Logger::error("DDLExporter", "mysql_init() failed");
      return;
    }
    
    unsigned int portNum = 3306;
    if (!port.empty()) {
      try {
        portNum = std::stoul(port);
      } catch (...) {
        portNum = 3306;
      }
    }
    
    if (mysql_real_connect(conn, host.c_str(), user.c_str(), password.c_str(),
                           db.c_str(), portNum, nullptr, 0) == nullptr) {
      Logger::error("DDLExporter", "MariaDB connection failed: " + std::string(mysql_error(conn)));
      mysql_close(conn);
      return;
    }

    std::string tablesQuery =
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = '" +
        escapeSQL(schema.schema_name) +
        "' "
        "AND table_type = 'BASE TABLE';";

    if (mysql_query(conn, tablesQuery.c_str())) {
      Logger::error("DDLExporter", "Query failed: " + std::string(mysql_error(conn)));
      mysql_close(conn);
      return;
    }

    MYSQL_RES *tablesResult = mysql_store_result(conn);
    if (!tablesResult) {
      Logger::error("DDLExporter", "mysql_store_result() failed: " + std::string(mysql_error(conn)));
      mysql_close(conn);
      return;
    }

    MYSQL_ROW tableRow;
    while ((tableRow = mysql_fetch_row(tablesResult))) {
      std::string tableName = tableRow[0] ? tableRow[0] : "";

      std::string createTableQuery = "SHOW CREATE TABLE `" +
                                     escapeSQL(schema.schema_name) + "`.`" +
                                     escapeSQL(tableName) + "`;";
      
      if (mysql_query(conn, createTableQuery.c_str())) {
        Logger::error("DDLExporter", "SHOW CREATE TABLE failed: " + std::string(mysql_error(conn)));
        continue;
      }

      MYSQL_RES *createResult = mysql_store_result(conn);
      if (createResult && mysql_num_rows(createResult) > 0) {
        MYSQL_ROW createRow = mysql_fetch_row(createResult);
        if (createRow && createRow[1]) {
          std::string ddl = createRow[1];
          saveTableDDL(schema.db_engine, schema.database_name, schema.schema_name,
                       tableName, ddl);
        }
      }
      mysql_free_result(createResult);

      std::string indexesQuery = "SHOW INDEX FROM `" +
                                 escapeSQL(schema.schema_name) + "`.`" +
                                 escapeSQL(tableName) + "`;";
      
      if (mysql_query(conn, indexesQuery.c_str())) {
        Logger::error("DDLExporter", "SHOW INDEX failed: " + std::string(mysql_error(conn)));
        continue;
      }

      MYSQL_RES *indexesResult = mysql_store_result(conn);
      if (indexesResult) {
        MYSQL_ROW indexRow;
        while ((indexRow = mysql_fetch_row(indexesResult))) {
          if (indexRow[2] && indexRow[4] && indexRow[1]) {
            std::string indexName = indexRow[2];
            std::string columnName = indexRow[4];
            std::string nonUnique = indexRow[1];

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
        mysql_free_result(indexesResult);
      }
    }

    mysql_free_result(tablesResult);
    
    // Export Views
    exportMariaDBViews(conn, schema);
    
    // Export Stored Procedures
    exportMariaDBProcedures(conn, schema);
    
    // Export Functions
    exportMariaDBFunctions(conn, schema);
    
    // Export Triggers
    exportMariaDBTriggers(conn, schema);
    
    // Export Constraints
    exportMariaDBConstraints(conn, schema);
    
    // Export Events
    exportMariaDBEvents(conn, schema);
    
    mysql_close(conn);
    
    //Logger::info("DDLExporter",
                 //"Exported MariaDB DDL for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error exporting MariaDB DDL: " + std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBViews(MYSQL *conn, const SchemaInfo &schema) {
  try {
    std::string viewsQuery = "SHOW FULL TABLES FROM `" + escapeSQL(schema.schema_name) + "` WHERE Table_type = 'VIEW';";
    
    //Logger::debug("DDLExporter", "Querying for views in schema: " + schema.schema_name);
    
    if (mysql_query(conn, viewsQuery.c_str())) {
      Logger::debug("DDLExporter", "Views query failed: " + std::string(mysql_error(conn)));
      return;
    }
    
    MYSQL_RES *viewsResult = mysql_store_result(conn);
    if (!viewsResult) {
      Logger::debug("DDLExporter", "No views result set for schema: " + schema.schema_name);
      return;
    }
    
    int viewCount = mysql_num_rows(viewsResult);
    //Logger::debug("DDLExporter", "Found " + std::to_string(viewCount) + " views in schema: " + schema.schema_name);
    
    MYSQL_ROW viewRow;
    while ((viewRow = mysql_fetch_row(viewsResult))) {
      if (viewRow[0]) {
        std::string viewName = viewRow[0];
        //Logger::debug("DDLExporter", "Processing view: " + viewName);
        
        std::string createViewQuery = "SHOW CREATE VIEW `" + 
                                     escapeSQL(schema.schema_name) + "`.`" + 
                                     escapeSQL(viewName) + "`;";
        
        if (mysql_query(conn, createViewQuery.c_str())) {
          Logger::error("DDLExporter", "SHOW CREATE VIEW failed for " + viewName + ": " + std::string(mysql_error(conn)));
          continue;
        }
        
        MYSQL_RES *createResult = mysql_store_result(conn);
        if (createResult && mysql_num_rows(createResult) > 0) {
          MYSQL_ROW createRow = mysql_fetch_row(createResult);
          if (createRow && createRow[1]) {
            std::string ddl = createRow[1];
            saveTableDDL(schema.db_engine, schema.database_name, schema.schema_name,
                         viewName, ddl);
            //Logger::debug("DDLExporter", "Successfully exported view: " + viewName);
          } else {
            Logger::warning("DDLExporter", "No DDL found for view: " + viewName);
          }
        } else {
          Logger::warning("DDLExporter", "No CREATE VIEW result for: " + viewName);
        }
        mysql_free_result(createResult);
      }
    }
    
    mysql_free_result(viewsResult);
    //Logger::debug("DDLExporter", "Exported MariaDB views for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MariaDB views: " + std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBProcedures(MYSQL *conn, const SchemaInfo &schema) {
  try {
    std::string proceduresQuery = "SELECT routine_name FROM information_schema.routines "
                                 "WHERE routine_schema = '" + escapeSQL(schema.schema_name) + "' "
                                 "AND routine_type = 'PROCEDURE';";
    
    if (mysql_query(conn, proceduresQuery.c_str())) {
      Logger::debug("DDLExporter", "No procedures found or query failed: " + std::string(mysql_error(conn)));
      return;
    }
    
    MYSQL_RES *proceduresResult = mysql_store_result(conn);
    if (!proceduresResult) return;
    
    MYSQL_ROW procRow;
    while ((procRow = mysql_fetch_row(proceduresResult))) {
      std::string procName = procRow[0] ? procRow[0] : "";
      
      std::string createProcQuery = "SHOW CREATE PROCEDURE `" + 
                                   escapeSQL(schema.schema_name) + "`.`" + 
                                   escapeSQL(procName) + "`;";
      
      if (mysql_query(conn, createProcQuery.c_str())) {
        Logger::error("DDLExporter", "SHOW CREATE PROCEDURE failed: " + std::string(mysql_error(conn)));
        continue;
      }
      
      MYSQL_RES *createResult = mysql_store_result(conn);
      if (createResult && mysql_num_rows(createResult) > 0) {
        MYSQL_ROW createRow = mysql_fetch_row(createResult);
        if (createRow && createRow[2]) {
          std::string ddl = createRow[2];
          saveFunctionDDL(schema.db_engine, schema.database_name, schema.schema_name,
                         procName, ddl);
        }
      }
      mysql_free_result(createResult);
    }
    
    mysql_free_result(proceduresResult);
    //Logger::debug("DDLExporter", "Exported MariaDB procedures for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MariaDB procedures: " + std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBFunctions(MYSQL *conn, const SchemaInfo &schema) {
  try {
    std::string functionsQuery = "SELECT routine_name FROM information_schema.routines "
                                "WHERE routine_schema = '" + escapeSQL(schema.schema_name) + "' "
                                "AND routine_type = 'FUNCTION';";
    
    if (mysql_query(conn, functionsQuery.c_str())) {
      Logger::debug("DDLExporter", "No functions found or query failed: " + std::string(mysql_error(conn)));
      return;
    }
    
    MYSQL_RES *functionsResult = mysql_store_result(conn);
    if (!functionsResult) return;
    
    MYSQL_ROW funcRow;
    while ((funcRow = mysql_fetch_row(functionsResult))) {
      std::string funcName = funcRow[0] ? funcRow[0] : "";
      
      std::string createFuncQuery = "SHOW CREATE FUNCTION `" + 
                                   escapeSQL(schema.schema_name) + "`.`" + 
                                   escapeSQL(funcName) + "`;";
      
      if (mysql_query(conn, createFuncQuery.c_str())) {
        Logger::error("DDLExporter", "SHOW CREATE FUNCTION failed: " + std::string(mysql_error(conn)));
        continue;
      }
      
      MYSQL_RES *createResult = mysql_store_result(conn);
      if (createResult && mysql_num_rows(createResult) > 0) {
        MYSQL_ROW createRow = mysql_fetch_row(createResult);
        if (createRow && createRow[2]) {
          std::string ddl = createRow[2];
          saveFunctionDDL(schema.db_engine, schema.database_name, schema.schema_name,
                         funcName, ddl);
        }
      }
      mysql_free_result(createResult);
    }
    
    mysql_free_result(functionsResult);
    //Logger::debug("DDLExporter", "Exported MariaDB functions for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MariaDB functions: " + std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBTriggers(MYSQL *conn, const SchemaInfo &schema) {
  try {
    std::string triggersQuery = "SELECT trigger_name FROM information_schema.triggers "
                               "WHERE trigger_schema = '" + escapeSQL(schema.schema_name) + "';";
    
    if (mysql_query(conn, triggersQuery.c_str())) {
      Logger::debug("DDLExporter", "No triggers found or query failed: " + std::string(mysql_error(conn)));
      return;
    }
    
    MYSQL_RES *triggersResult = mysql_store_result(conn);
    if (!triggersResult) return;
    
    MYSQL_ROW triggerRow;
    while ((triggerRow = mysql_fetch_row(triggersResult))) {
      std::string triggerName = triggerRow[0] ? triggerRow[0] : "";
      
      std::string createTriggerQuery = "SHOW CREATE TRIGGER `" + 
                                      escapeSQL(schema.schema_name) + "`.`" + 
                                      escapeSQL(triggerName) + "`;";
      
      if (mysql_query(conn, createTriggerQuery.c_str())) {
        Logger::error("DDLExporter", "SHOW CREATE TRIGGER failed: " + std::string(mysql_error(conn)));
        continue;
      }
      
      MYSQL_RES *createResult = mysql_store_result(conn);
      if (createResult && mysql_num_rows(createResult) > 0) {
        MYSQL_ROW createRow = mysql_fetch_row(createResult);
        if (createRow && createRow[2]) {
          std::string ddl = createRow[2];
          saveFunctionDDL(schema.db_engine, schema.database_name, schema.schema_name,
                         triggerName, ddl);
        }
      }
      mysql_free_result(createResult);
    }
    
    mysql_free_result(triggersResult);
    //Logger::debug("DDLExporter", "Exported MariaDB triggers for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MariaDB triggers: " + std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBConstraints(MYSQL *conn, const SchemaInfo &schema) {
  try {
    std::string constraintsQuery = "SELECT table_name, constraint_name, constraint_type "
                                  "FROM information_schema.table_constraints "
                                  "WHERE table_schema = '" + escapeSQL(schema.schema_name) + "' "
                                  "AND constraint_type IN ('FOREIGN KEY', 'CHECK', 'UNIQUE');";
    
    if (mysql_query(conn, constraintsQuery.c_str())) {
      Logger::debug("DDLExporter", "No constraints found or query failed: " + std::string(mysql_error(conn)));
      return;
    }
    
    MYSQL_RES *constraintsResult = mysql_store_result(conn);
    if (!constraintsResult) return;
    
    MYSQL_ROW constraintRow;
    while ((constraintRow = mysql_fetch_row(constraintsResult))) {
      std::string tableName = constraintRow[0] ? constraintRow[0] : "";
      std::string constraintName = constraintRow[1] ? constraintRow[1] : "";
      std::string constraintType = constraintRow[2] ? constraintRow[2] : "";
      
      // For now, save constraint info as a simple DDL
      std::string ddl = "-- " + constraintType + " constraint: " + constraintName + 
                       " on table " + tableName;
      
      saveConstraintDDL(schema.db_engine, schema.database_name, schema.schema_name,
                       tableName, ddl);
    }
    
    mysql_free_result(constraintsResult);
    //Logger::debug("DDLExporter", "Exported MariaDB constraints for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MariaDB constraints: " + std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBEvents(MYSQL *conn, const SchemaInfo &schema) {
  try {
    std::string eventsQuery = "SELECT event_name FROM information_schema.events "
                             "WHERE event_schema = '" + escapeSQL(schema.schema_name) + "';";
    
    if (mysql_query(conn, eventsQuery.c_str())) {
      Logger::debug("DDLExporter", "No events found or query failed: " + std::string(mysql_error(conn)));
      return;
    }
    
    MYSQL_RES *eventsResult = mysql_store_result(conn);
    if (!eventsResult) return;
    
    MYSQL_ROW eventRow;
    while ((eventRow = mysql_fetch_row(eventsResult))) {
      std::string eventName = eventRow[0] ? eventRow[0] : "";
      
      std::string createEventQuery = "SHOW CREATE EVENT `" + 
                                    escapeSQL(schema.schema_name) + "`.`" + 
                                    escapeSQL(eventName) + "`;";
      
      if (mysql_query(conn, createEventQuery.c_str())) {
        Logger::error("DDLExporter", "SHOW CREATE EVENT failed: " + std::string(mysql_error(conn)));
        continue;
      }
      
      MYSQL_RES *createResult = mysql_store_result(conn);
      if (createResult && mysql_num_rows(createResult) > 0) {
        MYSQL_ROW createRow = mysql_fetch_row(createResult);
        if (createRow && createRow[3]) {
          std::string ddl = createRow[3];
          saveFunctionDDL(schema.db_engine, schema.database_name, schema.schema_name,
                         eventName, ddl);
        }
      }
      mysql_free_result(createResult);
    }
    
    mysql_free_result(eventsResult);
    //Logger::debug("DDLExporter", "Exported MariaDB events for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MariaDB events: " + std::string(e.what()));
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

    // Export Views
    exportPostgreSQLViews(conn, schema);
    
    // Export Functions
    exportPostgreSQLFunctions(conn, schema);
    
    // Export Triggers
    exportPostgreSQLTriggers(conn, schema);
    
    // Export Constraints
    exportPostgreSQLConstraints(conn, schema);
    
    // Export Sequences
    exportPostgreSQLSequences(conn, schema);
    
    // Export Types
    exportPostgreSQLTypes(conn, schema);

    txn.commit();
    Logger::info("DDLExporter",
                 "Exported PostgreSQL DDL for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter",
                  "Error exporting PostgreSQL DDL: " + std::string(e.what()));
  }
}

void DDLExporter::exportPostgreSQLViews(pqxx::connection &conn, const SchemaInfo &schema) {
  try {
    pqxx::work txn(conn);
    
    std::string viewsQuery = "SELECT table_name FROM information_schema.views "
                            "WHERE table_schema = '" + escapeSQL(schema.schema_name) + "';";
    
    auto viewsResult = txn.exec(viewsQuery);
    
    for (const auto &viewRow : viewsResult) {
      std::string viewName = viewRow[0].as<std::string>();
      
      std::string createViewQuery = "SELECT 'CREATE VIEW \"' || schemaname || '\".\"' || viewname || '\" AS ' || definition || ';' as ddl "
                                   "FROM pg_views WHERE schemaname = '" + escapeSQL(schema.schema_name) + "' "
                                   "AND viewname = '" + escapeSQL(viewName) + "';";
      
      auto createResult = txn.exec(createViewQuery);
      if (!createResult.empty()) {
        std::string ddl = createResult[0][0].as<std::string>();
        saveTableDDL(schema.db_engine, schema.database_name, schema.schema_name,
                     viewName, ddl);
      }
    }
    
    txn.commit();
    Logger::debug("DDLExporter", "Exported PostgreSQL views for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting PostgreSQL views: " + std::string(e.what()));
  }
}

void DDLExporter::exportPostgreSQLFunctions(pqxx::connection &conn, const SchemaInfo &schema) {
  try {
    pqxx::work txn(conn);
    
    std::string functionsQuery = "SELECT proname, pg_get_functiondef(oid) as definition "
                                "FROM pg_proc p "
                                "JOIN pg_namespace n ON p.pronamespace = n.oid "
                                "WHERE n.nspname = '" + escapeSQL(schema.schema_name) + "';";
    
    auto functionsResult = txn.exec(functionsQuery);
    
    for (const auto &funcRow : functionsResult) {
      std::string funcName = funcRow[0].as<std::string>();
      std::string definition = funcRow[1].as<std::string>();
      
      saveFunctionDDL(schema.db_engine, schema.database_name, schema.schema_name,
                     funcName, definition);
    }
    
    txn.commit();
    Logger::debug("DDLExporter", "Exported PostgreSQL functions for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting PostgreSQL functions: " + std::string(e.what()));
  }
}

void DDLExporter::exportPostgreSQLTriggers(pqxx::connection &conn, const SchemaInfo &schema) {
  try {
    pqxx::work txn(conn);
    
    std::string triggersQuery = "SELECT t.tgname, c.relname, pg_get_triggerdef(t.oid) as definition "
                               "FROM pg_trigger t "
                               "JOIN pg_class c ON t.tgrelid = c.oid "
                               "JOIN pg_namespace n ON c.relnamespace = n.oid "
                               "WHERE n.nspname = '" + escapeSQL(schema.schema_name) + "' "
                               "AND NOT t.tgisinternal;";
    
    auto triggersResult = txn.exec(triggersQuery);
    
    for (const auto &triggerRow : triggersResult) {
      std::string triggerName = triggerRow[0].as<std::string>();
      std::string tableName = triggerRow[1].as<std::string>();
      std::string definition = triggerRow[2].as<std::string>();
      
      saveFunctionDDL(schema.db_engine, schema.database_name, schema.schema_name,
                     triggerName, definition);
    }
    
    txn.commit();
    Logger::debug("DDLExporter", "Exported PostgreSQL triggers for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting PostgreSQL triggers: " + std::string(e.what()));
  }
}

void DDLExporter::exportPostgreSQLConstraints(pqxx::connection &conn, const SchemaInfo &schema) {
  try {
    pqxx::work txn(conn);
    
    std::string constraintsQuery = "SELECT tc.table_name, tc.constraint_name, tc.constraint_type, "
                                  "pg_get_constraintdef(cc.oid) as definition "
                                  "FROM information_schema.table_constraints tc "
                                  "LEFT JOIN pg_constraint cc ON tc.constraint_name = cc.conname "
                                  "WHERE tc.table_schema = '" + escapeSQL(schema.schema_name) + "' "
                                  "AND tc.constraint_type IN ('FOREIGN KEY', 'CHECK', 'UNIQUE', 'PRIMARY KEY');";
    
    auto constraintsResult = txn.exec(constraintsQuery);
    
    for (const auto &constraintRow : constraintsResult) {
      std::string tableName = constraintRow[0].as<std::string>();
      std::string constraintName = constraintRow[1].as<std::string>();
      std::string constraintType = constraintRow[2].as<std::string>();
      std::string definition = constraintRow[3].is_null() ? "" : constraintRow[3].as<std::string>();
      
      std::string ddl = "-- " + constraintType + " constraint: " + constraintName;
      if (!definition.empty()) {
        ddl += "\n" + definition;
      }
      
      saveConstraintDDL(schema.db_engine, schema.database_name, schema.schema_name,
                       tableName, ddl);
    }
    
    txn.commit();
    Logger::debug("DDLExporter", "Exported PostgreSQL constraints for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting PostgreSQL constraints: " + std::string(e.what()));
  }
}

void DDLExporter::exportPostgreSQLSequences(pqxx::connection &conn, const SchemaInfo &schema) {
  try {
    pqxx::work txn(conn);
    
    std::string sequencesQuery = "SELECT sequencename, pg_get_sequencedef(oid) as definition "
                                "FROM pg_sequences s "
                                "JOIN pg_namespace n ON s.schemaname = n.nspname "
                                "WHERE s.schemaname = '" + escapeSQL(schema.schema_name) + "';";
    
    auto sequencesResult = txn.exec(sequencesQuery);
    
    for (const auto &seqRow : sequencesResult) {
      std::string seqName = seqRow[0].as<std::string>();
      std::string definition = seqRow[1].as<std::string>();
      
      saveFunctionDDL(schema.db_engine, schema.database_name, schema.schema_name,
                     seqName, definition);
    }
    
    txn.commit();
    Logger::debug("DDLExporter", "Exported PostgreSQL sequences for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting PostgreSQL sequences: " + std::string(e.what()));
  }
}

void DDLExporter::exportPostgreSQLTypes(pqxx::connection &conn, const SchemaInfo &schema) {
  try {
    pqxx::work txn(conn);
    
    std::string typesQuery = "SELECT t.typname, pg_get_typedef(t.oid) as definition "
                            "FROM pg_type t "
                            "JOIN pg_namespace n ON t.typnamespace = n.oid "
                            "WHERE n.nspname = '" + escapeSQL(schema.schema_name) + "' "
                            "AND t.typtype = 'c';";
    
    auto typesResult = txn.exec(typesQuery);
    
    for (const auto &typeRow : typesResult) {
      std::string typeName = typeRow[0].as<std::string>();
      std::string definition = typeRow[1].as<std::string>();
      
      saveFunctionDDL(schema.db_engine, schema.database_name, schema.schema_name,
                     typeName, definition);
    }
    
    txn.commit();
    Logger::debug("DDLExporter", "Exported PostgreSQL types for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting PostgreSQL types: " + std::string(e.what()));
  }
}

void DDLExporter::exportMongoDBDDL(const SchemaInfo &schema) {
  try {
    std::string connStr = getConnectionString(schema);
    
    // Parse MongoDB connection string
    std::string host, port, database, username, password;
    std::istringstream ss(connStr);
    std::string token;
    while (std::getline(ss, token, ';')) {
      auto pos = token.find('=');
      if (pos == std::string::npos) continue;
      std::string key = token.substr(0, pos);
      std::string value = token.substr(pos + 1);
      
      // Trim whitespace
      key.erase(0, key.find_first_not_of(" \t\r\n"));
      key.erase(key.find_last_not_of(" \t\r\n") + 1);
      value.erase(0, value.find_first_not_of(" \t\r\n"));
      value.erase(value.find_last_not_of(" \t\r\n") + 1);
      
      if (key == "host") host = value;
      else if (key == "port") port = value;
      else if (key == "database") database = value;
      else if (key == "username") username = value;
      else if (key == "password") password = value;
    }
    
    if (host.empty()) host = "localhost";
    if (port.empty()) port = "27017";
    if (database.empty()) database = schema.schema_name;
    
    // Build MongoDB URI
    std::string uri = "mongodb://";
    if (!username.empty() && !password.empty()) {
      uri += username + ":" + password + "@";
    }
    uri += host + ":" + port + "/" + database;
    
    // Initialize MongoDB client
    mongoc_init();
    mongoc_client_t *client = mongoc_client_new(uri.c_str());
    if (!client) {
      Logger::error("DDLExporter", "Failed to create MongoDB client");
      return;
    }
    
    // Export Collections (equivalent to tables)
    exportMongoDBCollections(client, schema);
    
    // Export Views
    exportMongoDBViews(client, schema);
    
    // Export Functions
    exportMongoDBFunctions(client, schema);
    
    mongoc_client_destroy(client);
    mongoc_cleanup();
    
    Logger::info("DDLExporter", "Exported MongoDB DDL for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MongoDB DDL: " + std::string(e.what()));
  }
}

void DDLExporter::exportMongoDBCollections(mongoc_client_t *client, const SchemaInfo &schema) {
  try {
    mongoc_database_t *database = mongoc_client_get_database(client, schema.schema_name.c_str());
    mongoc_collection_t *collection = nullptr;
    
    // Get list of collections
    bson_t *command = BCON_NEW("listCollections", BCON_INT32(1));
    bson_t reply;
    bson_error_t error;
    
    if (mongoc_database_command_simple(database, command, nullptr, &reply, &error)) {
      bson_iter_t iter;
      if (bson_iter_init(&iter, &reply) && bson_iter_find(&iter, "cursor")) {
        bson_iter_t cursor_iter;
        if (bson_iter_recurse(&iter, &cursor_iter) && bson_iter_find(&cursor_iter, "firstBatch")) {
          bson_iter_t batch_iter;
          if (bson_iter_recurse(&cursor_iter, &batch_iter)) {
            while (bson_iter_next(&batch_iter)) {
              bson_iter_t doc_iter;
              if (bson_iter_recurse(&batch_iter, &doc_iter) && bson_iter_find(&doc_iter, "name")) {
                std::string collectionName = bson_iter_utf8(&doc_iter, nullptr);
                
                // Export collection schema
                std::string ddl = "-- MongoDB Collection: " + collectionName + "\n";
                ddl += "-- Schema: " + schema.schema_name + "\n";
                ddl += "-- Database: " + schema.database_name + "\n\n";
                ddl += "db.createCollection(\"" + collectionName + "\");\n";
                
                saveTableDDL(schema.db_engine, schema.database_name, schema.schema_name,
                           collectionName, ddl);
              }
            }
          }
        }
      }
    }
    
    bson_destroy(command);
    bson_destroy(&reply);
    mongoc_database_destroy(database);
    
    Logger::debug("DDLExporter", "Exported MongoDB collections for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MongoDB collections: " + std::string(e.what()));
  }
}

void DDLExporter::exportMongoDBViews(mongoc_client_t *client, const SchemaInfo &schema) {
  try {
    mongoc_database_t *database = mongoc_client_get_database(client, schema.schema_name.c_str());
    
    // Get list of views
    bson_t *command = BCON_NEW("listCollections", BCON_NEW("filter", "{", "type", "view", "}"));
    bson_t reply;
    bson_error_t error;
    
    if (mongoc_database_command_simple(database, command, nullptr, &reply, &error)) {
      bson_iter_t iter;
      if (bson_iter_init(&iter, &reply) && bson_iter_find(&iter, "cursor")) {
        bson_iter_t cursor_iter;
        if (bson_iter_recurse(&iter, &cursor_iter) && bson_iter_find(&cursor_iter, "firstBatch")) {
          bson_iter_t batch_iter;
          if (bson_iter_recurse(&cursor_iter, &batch_iter)) {
            while (bson_iter_next(&batch_iter)) {
              bson_iter_t doc_iter;
              if (bson_iter_recurse(&batch_iter, &doc_iter) && bson_iter_find(&doc_iter, "name")) {
                std::string viewName = bson_iter_utf8(&doc_iter, nullptr);
                
                // Get view definition
                bson_t *view_command = BCON_NEW("listCollections", BCON_NEW("filter", "{", "name", BCON_UTF8(viewName.c_str()), "}"));
                bson_t view_reply;
                if (mongoc_database_command_simple(database, view_command, nullptr, &view_reply, &error)) {
                  std::string ddl = "-- MongoDB View: " + viewName + "\n";
                  ddl += "-- Schema: " + schema.schema_name + "\n";
                  ddl += "-- Database: " + schema.database_name + "\n\n";
                  ddl += "db.createView(\"" + viewName + "\", \"source_collection\", []);\n";
                  
                  saveTableDDL(schema.db_engine, schema.database_name, schema.schema_name,
                             viewName, ddl);
                }
                bson_destroy(view_command);
                bson_destroy(&view_reply);
              }
            }
          }
        }
      }
    }
    
    bson_destroy(command);
    bson_destroy(&reply);
    mongoc_database_destroy(database);
    
    Logger::debug("DDLExporter", "Exported MongoDB views for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MongoDB views: " + std::string(e.what()));
  }
}

void DDLExporter::exportMongoDBFunctions(mongoc_client_t *client, const SchemaInfo &schema) {
  try {
    mongoc_database_t *database = mongoc_client_get_database(client, schema.schema_name.c_str());
    
    // Get system.js collection (where MongoDB stores functions)
    mongoc_collection_t *collection = mongoc_database_get_collection(database, "system.js");
    
    bson_t *query = bson_new();
    bson_t *opts = BCON_NEW("projection", "{", "value", BCON_INT32(1), "}");
    
    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(collection, query, opts, nullptr);
    const bson_t *doc;
    
    while (mongoc_cursor_next(cursor, &doc)) {
      bson_iter_t iter;
      if (bson_iter_init(&iter, doc) && bson_iter_find(&iter, "_id")) {
        std::string functionName = bson_iter_utf8(&iter, nullptr);
        
        if (bson_iter_find(&iter, "value")) {
          bson_iter_t value_iter;
          if (bson_iter_recurse(&iter, &value_iter)) {
            std::string ddl = "-- MongoDB Function: " + functionName + "\n";
            ddl += "-- Schema: " + schema.schema_name + "\n";
            ddl += "-- Database: " + schema.database_name + "\n\n";
            ddl += "db.system.js.insertOne({_id: \"" + functionName + "\", value: function() { /* function body */ }});\n";
            
            saveFunctionDDL(schema.db_engine, schema.database_name, schema.schema_name,
                           functionName, ddl);
          }
        }
      }
    }
    
    mongoc_cursor_destroy(cursor);
    bson_destroy(query);
    bson_destroy(opts);
    mongoc_collection_destroy(collection);
    mongoc_database_destroy(database);
    
    Logger::debug("DDLExporter", "Exported MongoDB functions for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MongoDB functions: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLDDL(const SchemaInfo &schema) {
  try {
    std::string connStr = getConnectionString(schema);
    
    // Parse MSSQL connection string
    std::string server, database, username, password, port;
    std::istringstream ss(connStr);
    std::string token;
    while (std::getline(ss, token, ';')) {
      auto pos = token.find('=');
      if (pos == std::string::npos) continue;
      std::string key = token.substr(0, pos);
      std::string value = token.substr(pos + 1);
      
      // Trim whitespace
      key.erase(0, key.find_first_not_of(" \t\r\n"));
      key.erase(key.find_last_not_of(" \t\r\n") + 1);
      value.erase(0, value.find_first_not_of(" \t\r\n"));
      value.erase(value.find_last_not_of(" \t\r\n") + 1);
      
      if (key == "server") server = value;
      else if (key == "database") database = value;
      else if (key == "username") username = value;
      else if (key == "password") password = value;
      else if (key == "port") port = value;
    }
    
    if (server.empty()) server = "localhost";
    if (port.empty()) port = "1433";
    if (database.empty()) database = schema.schema_name;
    
    // Build MSSQL connection string
    std::string odbcConnStr = "DRIVER={ODBC Driver 17 for SQL Server};";
    odbcConnStr += "SERVER=" + server + "," + port + ";";
    odbcConnStr += "DATABASE=" + database + ";";
    if (!username.empty()) {
      odbcConnStr += "UID=" + username + ";";
      odbcConnStr += "PWD=" + password + ";";
    } else {
      odbcConnStr += "Trusted_Connection=yes;";
    }
    
    // Initialize ODBC
    SQLHENV env;
    SQLHDBC conn;
    SQLRETURN ret;
    
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (ret != SQL_SUCCESS) {
      Logger::error("DDLExporter", "Failed to allocate ODBC environment");
      return;
    }
    
    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS) {
      Logger::error("DDLExporter", "Failed to set ODBC version");
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      return;
    }
    
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);
    if (ret != SQL_SUCCESS) {
      Logger::error("DDLExporter", "Failed to allocate ODBC connection");
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      return;
    }
    
    ret = SQLDriverConnect(conn, nullptr, (SQLCHAR*)odbcConnStr.c_str(), SQL_NTS, nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      Logger::error("DDLExporter", "Failed to connect to MSSQL server");
      SQLFreeHandle(SQL_HANDLE_DBC, conn);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      return;
    }
    
    // Export Tables (already implemented in existing code)
    // Export Views
    exportMSSQLViews(conn, schema);
    
    // Export Procedures
    exportMSSQLProcedures(conn, schema);
    
    // Export Functions
    exportMSSQLFunctions(conn, schema);
    
    // Export Triggers
    exportMSSQLTriggers(conn, schema);
    
    // Export Constraints
    exportMSSQLConstraints(conn, schema);
    
    SQLDisconnect(conn);
    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    
    Logger::info("DDLExporter", "Exported MSSQL DDL for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MSSQL DDL: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLViews(SQLHDBC conn, const SchemaInfo &schema) {
  try {
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error("DDLExporter", "Failed to allocate statement handle for views");
      return;
    }
    
    std::string viewsQuery = "SELECT name, definition FROM sys.views v "
                            "JOIN sys.sql_modules m ON v.object_id = m.object_id "
                            "WHERE SCHEMA_NAME(v.schema_id) = '" + escapeSQL(schema.schema_name) + "'";
    
    ret = SQLExecDirect(stmt, (SQLCHAR*)viewsQuery.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLCHAR viewName[256];
      SQLCHAR definition[4000];
      SQLLEN nameLen, defLen;
      
      while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLGetData(stmt, 1, SQL_C_CHAR, viewName, sizeof(viewName), &nameLen);
        SQLGetData(stmt, 2, SQL_C_CHAR, definition, sizeof(definition), &defLen);
        
        std::string ddl = "-- MSSQL View: " + std::string((char*)viewName) + "\n";
        ddl += "-- Schema: " + schema.schema_name + "\n";
        ddl += "-- Database: " + schema.database_name + "\n\n";
        ddl += "CREATE VIEW [" + schema.schema_name + "].[" + std::string((char*)viewName) + "] AS\n";
        ddl += std::string((char*)definition) + "\n";
        
        saveTableDDL(schema.db_engine, schema.database_name, schema.schema_name,
                     std::string((char*)viewName), ddl);
      }
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    Logger::debug("DDLExporter", "Exported MSSQL views for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MSSQL views: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLProcedures(SQLHDBC conn, const SchemaInfo &schema) {
  try {
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error("DDLExporter", "Failed to allocate statement handle for procedures");
      return;
    }
    
    std::string proceduresQuery = "SELECT name, definition FROM sys.procedures p "
                                 "JOIN sys.sql_modules m ON p.object_id = m.object_id "
                                 "WHERE SCHEMA_NAME(p.schema_id) = '" + escapeSQL(schema.schema_name) + "'";
    
    ret = SQLExecDirect(stmt, (SQLCHAR*)proceduresQuery.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLCHAR procName[256];
      SQLCHAR definition[4000];
      SQLLEN nameLen, defLen;
      
      while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLGetData(stmt, 1, SQL_C_CHAR, procName, sizeof(procName), &nameLen);
        SQLGetData(stmt, 2, SQL_C_CHAR, definition, sizeof(definition), &defLen);
        
        std::string ddl = "-- MSSQL Procedure: " + std::string((char*)procName) + "\n";
        ddl += "-- Schema: " + schema.schema_name + "\n";
        ddl += "-- Database: " + schema.database_name + "\n\n";
        ddl += std::string((char*)definition) + "\n";
        
        saveFunctionDDL(schema.db_engine, schema.database_name, schema.schema_name,
                       std::string((char*)procName), ddl);
      }
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    Logger::debug("DDLExporter", "Exported MSSQL procedures for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MSSQL procedures: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLFunctions(SQLHDBC conn, const SchemaInfo &schema) {
  try {
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error("DDLExporter", "Failed to allocate statement handle for functions");
      return;
    }
    
    std::string functionsQuery = "SELECT name, definition FROM sys.objects o "
                                "JOIN sys.sql_modules m ON o.object_id = m.object_id "
                                "WHERE o.type IN ('FN', 'IF', 'TF') "
                                "AND SCHEMA_NAME(o.schema_id) = '" + escapeSQL(schema.schema_name) + "'";
    
    ret = SQLExecDirect(stmt, (SQLCHAR*)functionsQuery.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLCHAR funcName[256];
      SQLCHAR definition[4000];
      SQLLEN nameLen, defLen;
      
      while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLGetData(stmt, 1, SQL_C_CHAR, funcName, sizeof(funcName), &nameLen);
        SQLGetData(stmt, 2, SQL_C_CHAR, definition, sizeof(definition), &defLen);
        
        std::string ddl = "-- MSSQL Function: " + std::string((char*)funcName) + "\n";
        ddl += "-- Schema: " + schema.schema_name + "\n";
        ddl += "-- Database: " + schema.database_name + "\n\n";
        ddl += std::string((char*)definition) + "\n";
        
        saveFunctionDDL(schema.db_engine, schema.database_name, schema.schema_name,
                       std::string((char*)funcName), ddl);
      }
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    Logger::debug("DDLExporter", "Exported MSSQL functions for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MSSQL functions: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLTriggers(SQLHDBC conn, const SchemaInfo &schema) {
  try {
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error("DDLExporter", "Failed to allocate statement handle for triggers");
      return;
    }
    
    std::string triggersQuery = "SELECT t.name, t.definition, o.name as table_name "
                               "FROM sys.triggers t "
                               "JOIN sys.objects o ON t.parent_id = o.object_id "
                               "WHERE SCHEMA_NAME(t.schema_id) = '" + escapeSQL(schema.schema_name) + "'";
    
    ret = SQLExecDirect(stmt, (SQLCHAR*)triggersQuery.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLCHAR triggerName[256];
      SQLCHAR definition[4000];
      SQLCHAR tableName[256];
      SQLLEN nameLen, defLen, tableLen;
      
      while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLGetData(stmt, 1, SQL_C_CHAR, triggerName, sizeof(triggerName), &nameLen);
        SQLGetData(stmt, 2, SQL_C_CHAR, definition, sizeof(definition), &defLen);
        SQLGetData(stmt, 3, SQL_C_CHAR, tableName, sizeof(tableName), &tableLen);
        
        std::string ddl = "-- MSSQL Trigger: " + std::string((char*)triggerName) + "\n";
        ddl += "-- Table: " + std::string((char*)tableName) + "\n";
        ddl += "-- Schema: " + schema.schema_name + "\n";
        ddl += "-- Database: " + schema.database_name + "\n\n";
        ddl += std::string((char*)definition) + "\n";
        
        saveFunctionDDL(schema.db_engine, schema.database_name, schema.schema_name,
                       std::string((char*)triggerName), ddl);
      }
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    Logger::debug("DDLExporter", "Exported MSSQL triggers for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MSSQL triggers: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLConstraints(SQLHDBC conn, const SchemaInfo &schema) {
  try {
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error("DDLExporter", "Failed to allocate statement handle for constraints");
      return;
    }
    
    std::string constraintsQuery = "SELECT tc.table_name, tc.constraint_name, tc.constraint_type, "
                                  "cc.definition "
                                  "FROM information_schema.table_constraints tc "
                                  "LEFT JOIN sys.check_constraints cc ON tc.constraint_name = cc.name "
                                  "WHERE tc.table_schema = '" + escapeSQL(schema.schema_name) + "' "
                                  "AND tc.constraint_type IN ('FOREIGN KEY', 'CHECK', 'UNIQUE', 'PRIMARY KEY')";
    
    ret = SQLExecDirect(stmt, (SQLCHAR*)constraintsQuery.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLCHAR tableName[256];
      SQLCHAR constraintName[256];
      SQLCHAR constraintType[50];
      SQLCHAR definition[4000];
      SQLLEN tableLen, nameLen, typeLen, defLen;
      
      while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLGetData(stmt, 1, SQL_C_CHAR, tableName, sizeof(tableName), &tableLen);
        SQLGetData(stmt, 2, SQL_C_CHAR, constraintName, sizeof(constraintName), &nameLen);
        SQLGetData(stmt, 3, SQL_C_CHAR, constraintType, sizeof(constraintType), &typeLen);
        SQLGetData(stmt, 4, SQL_C_CHAR, definition, sizeof(definition), &defLen);
        
        std::string ddl = "-- " + std::string((char*)constraintType) + " constraint: " + 
                         std::string((char*)constraintName) + " on table " + 
                         std::string((char*)tableName);
        if (defLen > 0) {
          ddl += "\n" + std::string((char*)definition);
        }
        
        saveConstraintDDL(schema.db_engine, schema.database_name, schema.schema_name,
                         std::string((char*)tableName), ddl);
      }
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    Logger::debug("DDLExporter", "Exported MSSQL constraints for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error("DDLExporter", "Error exporting MSSQL constraints: " + std::string(e.what()));
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

    // Create directory if it doesn't exist
    std::filesystem::create_directories(std::filesystem::path(filePath).parent_path());

    std::ofstream file(filePath);
    if (file.is_open()) {
      file << "-- Table DDL for " << schema << "." << table_name << std::endl;
      file << "-- Engine: " << engine << std::endl;
      file << "-- Database: " << database << std::endl;
      file << "-- Generated: " << std::time(nullptr) << std::endl;
      file << std::endl;
      file << ddl << std::endl;
      file.close();

      //Logger::debug("DDLExporter", "Saved table DDL: " + filePath);
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

    // Create directory if it doesn't exist
    std::filesystem::create_directories(std::filesystem::path(filePath).parent_path());

    std::ofstream file(filePath, std::ios::app);
    if (file.is_open()) {
      file << index_ddl << std::endl;
      file.close();

      //Logger::debug("DDLExporter", "Saved index DDL: " + filePath);
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

    // Create directory if it doesn't exist
    std::filesystem::create_directories(std::filesystem::path(filePath).parent_path());

    std::ofstream file(filePath, std::ios::app);
    if (file.is_open()) {
      file << constraint_ddl << std::endl;
      file.close();

      //Logger::debug("DDLExporter", "Saved constraint DDL: " + filePath);
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

    // Create directory if it doesn't exist
    std::filesystem::create_directories(std::filesystem::path(filePath).parent_path());

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

      //Logger::debug("DDLExporter", "Saved function DDL: " + filePath);
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
