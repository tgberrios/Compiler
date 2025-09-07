#ifndef DDLEXPORTER_H
#define DDLEXPORTER_H

#include "Config.h"
#include "logger.h"
#include <filesystem>
#include <map>
#include <pqxx/pqxx>
#include <string>
#include <vector>

struct SchemaInfo {
  std::string schema_name;
  std::string db_engine;
  std::string database_name;
  std::string connection_string;
};

struct TableInfo {
  std::string table_name;
  std::string ddl;
  std::vector<std::string> indexes;
  std::vector<std::string> constraints;
  std::vector<std::string> functions;
};

class DDLExporter {
public:
  DDLExporter() = default;
  ~DDLExporter() = default;

  void exportAllDDL();
  void createFolderStructure();

private:
  void getSchemasFromCatalog();
  void exportSchemaDDL(const SchemaInfo &schema);
  void createEngineFolder(const std::string &engine);
  void createDatabaseFolder(const std::string &engine,
                            const std::string &database);
  void createSchemaFolder(const std::string &engine,
                          const std::string &database,
                          const std::string &schema);

  void exportMariaDBDDL(const SchemaInfo &schema);
  void exportPostgreSQLDDL(const SchemaInfo &schema);
  void exportMongoDBDDL(const SchemaInfo &schema);
  void exportMSSQLDDL(const SchemaInfo &schema);

  void saveTableDDL(const std::string &engine, const std::string &database,
                    const std::string &schema, const std::string &table_name,
                    const std::string &ddl);
  void saveIndexDDL(const std::string &engine, const std::string &database,
                    const std::string &schema, const std::string &table_name,
                    const std::string &index_ddl);
  void saveConstraintDDL(const std::string &engine, const std::string &database,
                         const std::string &schema,
                         const std::string &table_name,
                         const std::string &constraint_ddl);
  void saveFunctionDDL(const std::string &engine, const std::string &database,
                       const std::string &schema,
                       const std::string &function_name,
                       const std::string &function_ddl);

  std::string getConnectionString(const SchemaInfo &schema);
  std::string escapeSQL(const std::string &value);
  std::string sanitizeFileName(const std::string &name);

  std::vector<SchemaInfo> schemas;
  std::string exportPath = "DDL_EXPORT";
};

#endif // DDLEXPORTER_H
