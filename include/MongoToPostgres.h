#ifndef MONGOTOPOSTGRES_H
#define MONGOTOPOSTGRES_H

#include "Config.h"
#include "logger.h"
#include <bson/bson.h>
#include <json/json.h>
#include <mongoc/mongoc.h>
#include <pqxx/pqxx>
#include <string>
#include <vector>

class MongoToPostgres {
public:
  MongoToPostgres() = default;
  ~MongoToPostgres() = default;

  void setupTableTargetMongoToPostgres() {
    try {
      Logger::info("setupTableTargetMongoToPostgres",
                   "Starting MongoDB target table setup");
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      pqxx::work txn(pgConn);
      auto results = txn.exec("SELECT schema_name, table_name, "
                              "connection_string FROM metadata.catalog "
                              "WHERE db_engine='MongoDB' AND active=true;");

      for (const auto &row : results) {
        if (row.size() < 3)
          continue;

        std::string schemaName = row[0].as<std::string>();
        std::string tableName = row[1].as<std::string>();
        std::string mongoConnStr = row[2].as<std::string>();

        Logger::debug("setupTableTargetMongoToPostgres",
                      "Setting up table: " + schemaName + "." + tableName);

        try {
          auto mongoConn = connectMongoDB(mongoConnStr);
          if (!mongoConn) {
            Logger::error("setupTableTargetMongoToPostgres",
                          "Failed to connect to MongoDB");
            continue;
          }

          std::string lowerSchemaName = toLowerCase(schemaName);
          createSchemaIfNotExists(txn, lowerSchemaName);

          std::string createTableQuery = buildCreateTableQuery(
              *mongoConn, schemaName, tableName, lowerSchemaName);
          if (!createTableQuery.empty()) {
            txn.exec(createTableQuery);
            Logger::info("setupTableTargetMongoToPostgres",
                         "Created target table: " + lowerSchemaName + "." +
                             tableName);
          }

        } catch (const std::exception &e) {
          Logger::error("setupTableTargetMongoToPostgres",
                        "Error setting up table " + schemaName + "." +
                            tableName + ": " + e.what());
        }
      }

      txn.commit();
      Logger::info("setupTableTargetMongoToPostgres",
                   "Target table setup completed");
    } catch (const std::exception &e) {
      Logger::error("setupTableTargetMongoToPostgres",
                    "Error in setupTableTargetMongoToPostgres: " +
                        std::string(e.what()));
    }
  }

  void transferDataMongoToPostgres() {
    try {
      // Logger::debug("transferDataMongoToPostgres",
      //               "Starting MongoDB to PostgreSQL transfer");
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      {
        pqxx::work txn(pgConn);
        auto results =
            txn.exec("SELECT schema_name, table_name, connection_string, "
                     "last_offset, status FROM metadata.catalog "
                     "WHERE db_engine='MongoDB' AND active=true;");

        for (const auto &row : results) {
          if (row.size() < 5)
            continue;

          std::string schemaName = row[0].as<std::string>();
          std::string tableName = row[1].as<std::string>();
          std::string mongoConnStr = row[2].as<std::string>();
          std::string lastOffset = row[3].as<std::string>();
          std::string status = row[4].as<std::string>();

          Logger::debug("transferDataMongoToPostgres",
                        "Processing table: " + schemaName + "." + tableName +
                            " (status: " + status + ")");

          try {
            processTable(pgConn, schemaName, tableName, mongoConnStr,
                         lastOffset, status);
          } catch (const std::exception &e) {
            Logger::error("transferDataMongoToPostgres",
                          "Error processing table " + schemaName + "." +
                              tableName + ": " + e.what());
            updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
          }
        }

        txn.commit();
      }
    } catch (const std::exception &e) {
      Logger::error("transferDataMongoToPostgres",
                    "Error in transferDataMongoToPostgres: " +
                        std::string(e.what()));
    }
  }

private:
  std::unique_ptr<mongoc_client_t, void (*)(mongoc_client_t *)>
  connectMongoDB(const std::string &connStr) {
    mongoc_client_t *client = mongoc_client_new(connStr.c_str());
    if (!client) {
      Logger::error("connectMongoDB", "Failed to create MongoDB client");
      return std::unique_ptr<mongoc_client_t, void (*)(mongoc_client_t *)>(
          nullptr, mongoc_client_destroy);
    }

    // Test connection
    bson_t *ping = BCON_NEW("ping", BCON_INT32(1));
    bson_t reply;
    bson_error_t error;

    bool ret = mongoc_client_command_simple(client, "admin", ping, NULL, &reply,
                                            &error);
    bson_destroy(ping);
    bson_destroy(&reply);

    if (!ret) {
      Logger::error("connectMongoDB",
                    "Failed to ping MongoDB: " + std::string(error.message));
      mongoc_client_destroy(client);
      return std::unique_ptr<mongoc_client_t, void (*)(mongoc_client_t *)>(
          nullptr, mongoc_client_destroy);
    }

    Logger::debug("connectMongoDB",
                  "MongoDB connection established successfully");
    return std::unique_ptr<mongoc_client_t, void (*)(mongoc_client_t *)>(
        client, mongoc_client_destroy);
  }

  std::string toLowerCase(const std::string &str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
  }

  void createSchemaIfNotExists(pqxx::work &txn, const std::string &schemaName) {
    txn.exec("CREATE SCHEMA IF NOT EXISTS \"" + schemaName + "\";");
  }

  std::string buildCreateTableQuery(mongoc_client_t &mongoClient,
                                    const std::string &dbName,
                                    const std::string &collectionName,
                                    const std::string &targetSchema) {
    try {
      mongoc_collection_t *collection = mongoc_client_get_collection(
          &mongoClient, dbName.c_str(), collectionName.c_str());

      // Get sample document to determine schema
      bson_t *query = bson_new();
      bson_t *opts = BCON_NEW("limit", BCON_INT32(1));
      mongoc_cursor_t *cursor =
          mongoc_collection_find_with_opts(collection, query, opts, NULL);

      const bson_t *doc;
      if (mongoc_cursor_next(cursor, &doc)) {
        std::string createQuery =
            buildCreateTableFromDocument(doc, targetSchema, collectionName);
        mongoc_cursor_destroy(cursor);
        bson_destroy(query);
        bson_destroy(opts);
        mongoc_collection_destroy(collection);
        return createQuery;
      }

      mongoc_cursor_destroy(cursor);
      bson_destroy(query);
      bson_destroy(opts);
      mongoc_collection_destroy(collection);

      // If no documents, create basic table with _id
      return "CREATE TABLE IF NOT EXISTS \"" + targetSchema + "\".\"" +
             collectionName + "\" (_id VARCHAR(24) PRIMARY KEY, data JSONB);";

    } catch (const std::exception &e) {
      Logger::error("buildCreateTableQuery",
                    "Error building create table query: " +
                        std::string(e.what()));
      return "";
    }
  }

  std::string buildCreateTableFromDocument(const bson_t *doc,
                                           const std::string &targetSchema,
                                           const std::string &tableName) {
    std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" + targetSchema +
                              "\".\"" + tableName + "\" (";
    std::vector<std::string> columns;

    // Always include _id as primary key
    columns.push_back("_id VARCHAR(24) PRIMARY KEY");

    bson_iter_t iter;
    if (bson_iter_init(&iter, doc)) {
      while (bson_iter_next(&iter)) {
        const char *key = bson_iter_key(&iter);
        if (strcmp(key, "_id") == 0)
          continue; // Already added

        std::string columnDef = "\"" + std::string(key) + "\" ";

        switch (bson_iter_type(&iter)) {
        case BSON_TYPE_UTF8:
          columnDef += "TEXT";
          break;
        case BSON_TYPE_INT32:
          columnDef += "INTEGER";
          break;
        case BSON_TYPE_INT64:
          columnDef += "BIGINT";
          break;
        case BSON_TYPE_DOUBLE:
          columnDef += "DOUBLE PRECISION";
          break;
        case BSON_TYPE_BOOL:
          columnDef += "BOOLEAN";
          break;
        case BSON_TYPE_DATE_TIME:
          columnDef += "TIMESTAMP";
          break;
        case BSON_TYPE_OID:
          columnDef += "VARCHAR(24)";
          break;
        case BSON_TYPE_DOCUMENT:
        case BSON_TYPE_ARRAY:
        default:
          columnDef += "JSONB";
          break;
        }

        columns.push_back(columnDef);
      }
    }

    createQuery += columns[0];
    for (size_t i = 1; i < columns.size(); ++i) {
      createQuery += ", " + columns[i];
    }
    createQuery += ");";

    return createQuery;
  }

  void processTable(pqxx::connection &pgConn, const std::string &schemaName,
                    const std::string &tableName,
                    const std::string &mongoConnStr,
                    const std::string &lastOffset, const std::string &status) {

    if (status == "RESET") {
      Logger::info("processTable",
                   "Processing RESET table: " + schemaName + "." + tableName);
      {
        pqxx::work txn(pgConn);
        std::string lowerSchemaName = toLowerCase(schemaName);
        txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" + tableName +
                 "\" CASCADE;");
        txn.commit();
      }
      updateStatus(pgConn, schemaName, tableName, "FULL_LOAD", 0);
      Logger::info("processTable",
                   "Table " + schemaName + "." + tableName +
                       " reset completed, status changed to FULL_LOAD");
      return;
    }

    auto mongoClient = connectMongoDB(mongoConnStr);
    if (!mongoClient) {
      updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
      return;
    }

    int sourceCount = getSourceCount(*mongoClient, schemaName, tableName);
    int targetCount = getTargetCount(pgConn, schemaName, tableName);

    Logger::debug("processTable",
                  "Table " + schemaName + "." + tableName +
                      " - Source: " + std::to_string(sourceCount) +
                      ", Target: " + std::to_string(targetCount));

    if (sourceCount == targetCount) {
      updateStatus(pgConn, schemaName, tableName, "PERFECT_MATCH", sourceCount);
    } else if (sourceCount == 0) {
      updateStatus(pgConn, schemaName, tableName, "NO_DATA", 0);
    } else if (sourceCount < targetCount) {
      updateStatus(pgConn, schemaName, tableName, "ERROR", sourceCount);
    } else {
      performDataTransfer(pgConn, *mongoClient, schemaName, tableName,
                          sourceCount);
    }
  }

  int getSourceCount(mongoc_client_t &mongoClient, const std::string &dbName,
                     const std::string &collectionName) {
    try {
      mongoc_collection_t *collection = mongoc_client_get_collection(
          &mongoClient, dbName.c_str(), collectionName.c_str());

      bson_t *query = bson_new();
      int64_t count = mongoc_collection_count_documents(collection, query, NULL,
                                                        NULL, NULL, NULL);

      bson_destroy(query);
      mongoc_collection_destroy(collection);

      return static_cast<int>(count);
    } catch (const std::exception &e) {
      Logger::error("getSourceCount",
                    "Error getting source count: " + std::string(e.what()));
      return 0;
    }
  }

  int getTargetCount(pqxx::connection &pgConn, const std::string &schemaName,
                     const std::string &tableName) {
    try {
      std::string lowerSchemaName = toLowerCase(schemaName);
      pqxx::connection countConn(DatabaseConfig::getPostgresConnectionString());
      pqxx::work txn(countConn);
      auto result = txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                             "\".\"" + tableName + "\"");
      txn.commit();
      if (!result.empty()) {
        return result[0][0].as<int>();
      }
      return 0;
    } catch (const std::exception &e) {
      Logger::error("getTargetCount",
                    "Error getting target count: " + std::string(e.what()));
      return 0;
    }
  }

  void performDataTransfer(pqxx::connection &pgConn,
                           mongoc_client_t &mongoClient,
                           const std::string &dbName,
                           const std::string &collectionName, int sourceCount) {
    try {
      Logger::info("performDataTransfer",
                   "Transferring data for " + dbName + "." + collectionName);

      std::string lowerSchemaName = toLowerCase(dbName);

      mongoc_collection_t *collection = mongoc_client_get_collection(
          &mongoClient, dbName.c_str(), collectionName.c_str());

      bson_t *query = bson_new();
      mongoc_cursor_t *cursor =
          mongoc_collection_find_with_opts(collection, query, NULL, NULL);

      {
        pqxx::connection targetConn(
            DatabaseConfig::getPostgresConnectionString());
        pqxx::work targetTxn(targetConn);

        std::string truncateQuery = "TRUNCATE TABLE \"" + lowerSchemaName +
                                    "\".\"" + collectionName + "\" CASCADE;";
        targetTxn.exec(truncateQuery);

        const bson_t *doc;
        int transferred = 0;
        while (mongoc_cursor_next(cursor, &doc)) {
          std::string insertQuery =
              buildInsertQuery(doc, lowerSchemaName, collectionName);
          if (!insertQuery.empty()) {
            targetTxn.exec(insertQuery);
            transferred++;
          }
        }

        targetTxn.exec("UPDATE metadata.catalog SET last_offset='" +
                       std::to_string(sourceCount) + "' WHERE schema_name='" +
                       escapeSQL(dbName) + "' AND table_name='" +
                       escapeSQL(collectionName) + "';");

        targetTxn.commit();
        Logger::info("performDataTransfer",
                     "Successfully transferred " + std::to_string(transferred) +
                         " records for " + dbName + "." + collectionName);
      }

      mongoc_cursor_destroy(cursor);
      bson_destroy(query);
      mongoc_collection_destroy(collection);

      updateStatus(pgConn, dbName, collectionName, "PERFECT_MATCH",
                   sourceCount);

    } catch (const std::exception &e) {
      Logger::error("performDataTransfer",
                    "Error transferring data: " + std::string(e.what()));
      updateStatus(pgConn, dbName, collectionName, "ERROR", 0);
    }
  }

  std::string buildInsertQuery(const bson_t *doc, const std::string &schemaName,
                               const std::string &tableName) {
    try {
      std::string insertQuery =
          "INSERT INTO \"" + schemaName + "\".\"" + tableName + "\" VALUES (";
      std::vector<std::string> values;

      bson_iter_t iter;
      if (bson_iter_init(&iter, doc)) {
        while (bson_iter_next(&iter)) {
          const char *key = bson_iter_key(&iter);
          std::string value = getBsonValueAsString(&iter);
          values.push_back("'" + escapeSQL(value) + "'");
        }
      }

      if (values.empty())
        return "";

      insertQuery += values[0];
      for (size_t i = 1; i < values.size(); ++i) {
        insertQuery += ", " + values[i];
      }
      insertQuery += ");";

      return insertQuery;
    } catch (const std::exception &e) {
      Logger::error("buildInsertQuery",
                    "Error building insert query: " + std::string(e.what()));
      return "";
    }
  }

  std::string getBsonValueAsString(bson_iter_t *iter) {
    switch (bson_iter_type(iter)) {
    case BSON_TYPE_UTF8:
      return bson_iter_utf8(iter, NULL);
    case BSON_TYPE_INT32:
      return std::to_string(bson_iter_int32(iter));
    case BSON_TYPE_INT64:
      return std::to_string(bson_iter_int64(iter));
    case BSON_TYPE_DOUBLE:
      return std::to_string(bson_iter_double(iter));
    case BSON_TYPE_BOOL:
      return bson_iter_bool(iter) ? "true" : "false";
    case BSON_TYPE_OID: {
      char oid_str[25];
      bson_oid_to_string(bson_iter_oid(iter), oid_str);
      return std::string(oid_str);
    }
    case BSON_TYPE_DATE_TIME: {
      int64_t datetime = bson_iter_date_time(iter);
      // Convert to PostgreSQL timestamp format
      time_t time = datetime / 1000;
      struct tm *tm_info = localtime(&time);
      char buffer[30];
      strftime(buffer, 30, "%Y-%m-%d %H:%M:%S", tm_info);
      return std::string(buffer);
    }
    case BSON_TYPE_DOCUMENT:
    case BSON_TYPE_ARRAY: {
      // Convert to JSON string
      uint32_t doc_len;
      const uint8_t *doc_data;
      bson_iter_document(iter, &doc_len, &doc_data);
      bson_t bson;
      bson_init_static(&bson, doc_data, doc_len);
      char *json = bson_as_canonical_extended_json(&bson, NULL);
      std::string result(json);
      bson_free(json);
      return result;
    }
    default:
      return "NULL";
    }
  }

  void updateStatus(pqxx::connection &pgConn, const std::string &schemaName,
                    const std::string &tableName, const std::string &status,
                    int count) {
    try {
      pqxx::connection updateConn(
          DatabaseConfig::getPostgresConnectionString());
      pqxx::work txn(updateConn);
      txn.exec("UPDATE metadata.catalog SET status='" + status +
               "', last_sync_time=NOW(), last_offset='" +
               std::to_string(count) +
               "' "
               "WHERE schema_name='" +
               escapeSQL(schemaName) + "' AND table_name='" +
               escapeSQL(tableName) + "';");
      txn.commit();
    } catch (const std::exception &e) {
      Logger::error("updateStatus",
                    "Error updating status: " + std::string(e.what()));
    }
  }

  std::string escapeSQL(const std::string &value) {
    std::string escaped = value;
    size_t pos = 0;
    while ((pos = escaped.find("'", pos)) != std::string::npos) {
      escaped.replace(pos, 1, "''");
      pos += 2;
    }
    return escaped;
  }
};

#endif // MONGOTOPOSTGRES_H
