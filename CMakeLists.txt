cmake_minimum_required(VERSION 3.16)
project(DataSync)

include(CheckIncludeFile)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR NOT CMAKE_BUILD_TYPE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -Wall -Wextra -fsanitize=address -fsanitize=undefined")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")
  message(STATUS "Building in DEBUG mode with AddressSanitizer")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")
  message(STATUS "Building in RELEASE mode")
endif()

set(ORACLE_FILES "")

set(ORACLE_INCLUDE_PATHS
    /usr/include/oracle/21/client64
    /usr/include/oracle/19/client64
    /usr/include/oracle/18/client64
    /opt/oracle/instantclient_21_1/sdk/include
    /opt/oracle/instantclient_19_8/sdk/include
    /opt/oracle/instantclient_18_5/sdk/include
)

set(HAVE_ORACLE_HEADER FALSE)
foreach(path ${ORACLE_INCLUDE_PATHS})
  if(EXISTS "${path}/oci.h")
    set(HAVE_ORACLE_HEADER TRUE)
    break()
  endif()
endforeach()

if(HAVE_ORACLE_HEADER)
  add_definitions(-DHAVE_ORACLE)
  message(STATUS "Oracle support: ENABLED")
  set(ORACLE_FILES
    src/governance/DataGovernanceOracle.cpp
    src/governance/LineageExtractorOracle.cpp
    src/engines/oracle_engine.cpp
    src/sync/OracleToPostgres.cpp
    src/sync/RedoLogReader.cpp
  )
else()
  message(STATUS "Oracle support: DISABLED (oci.h not found)")
endif()

# Spark detection
set(SPARK_FILES "")
set(SPARK_INCLUDE_PATHS
    /usr/local/spark/include
    /opt/spark/include
    ${CMAKE_SOURCE_DIR}/third_party/spark
)

set(SPARK_LIB_PATHS
    /usr/local/spark/lib
    /opt/spark/lib
)

# Check for Spark installation via spark-submit
find_program(SPARK_SUBMIT spark-submit
    PATHS
    /usr/local/spark/bin
    /opt/spark/bin
    /usr/bin
    /usr/local/bin
    ENV PATH
)

if(SPARK_SUBMIT)
  message(STATUS "Spark submit found: ${SPARK_SUBMIT}")
  # Try to get Spark home from spark-submit
  execute_process(
    COMMAND ${SPARK_SUBMIT} --version
    OUTPUT_VARIABLE SPARK_VERSION_OUTPUT
    ERROR_VARIABLE SPARK_VERSION_ERROR
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  
  # Check if Spark Connect is available (optional)
  find_program(SPARK_CONNECT spark-connect
      PATHS
      /usr/local/spark/bin
      /opt/spark/bin
  )
  
  set(HAVE_SPARK TRUE)
  add_definitions(-DHAVE_SPARK)
  message(STATUS "Spark support: ENABLED")
  list(APPEND SPARK_FILES
    src/engines/spark_engine.cpp
  )
else()
  message(STATUS "Spark support: DISABLED (spark-submit not found)")
  message(STATUS "  Install Spark and ensure spark-submit is in PATH")
endif()

# Hadoop/MapReduce detection (optional, legacy)
set(HADOOP_FILES "")
find_program(HADOOP_CMD hadoop
    PATHS
    /usr/local/hadoop/bin
    /opt/hadoop/bin
    /usr/bin
    ENV PATH
)

if(HADOOP_CMD)
  message(STATUS "Hadoop found: ${HADOOP_CMD}")
  set(HAVE_HADOOP TRUE)
  add_definitions(-DHAVE_HADOOP)
  message(STATUS "Hadoop/MapReduce support: ENABLED")
  list(APPEND HADOOP_FILES
    src/engines/mapreduce_engine.cpp
  )
else()
  message(STATUS "Hadoop/MapReduce support: DISABLED (hadoop not found)")
endif()

# Delta Lake detection (requires Spark)
if(HAVE_SPARK)
  # Delta Lake is typically used via Spark, check for delta-spark package
  set(HAVE_DELTA_LAKE TRUE)
  add_definitions(-DHAVE_DELTA_LAKE)
  message(STATUS "Delta Lake support: ENABLED (via Spark)")
  list(APPEND SPARK_FILES
    src/engines/delta_lake_engine.cpp
  )
else()
  message(STATUS "Delta Lake support: DISABLED (requires Spark)")
endif()

# Kafka detection (librdkafka)
set(KAFKA_FILES "")
find_library(RDKAFKA_LIB rdkafka
    PATHS
    /usr/local/lib
    /usr/lib
    /opt/kafka/lib
    ENV LD_LIBRARY_PATH
)

find_path(RDKAFKA_INCLUDE_DIR librdkafka/rdkafka.h
    PATHS
    /usr/local/include
    /usr/include
    /opt/kafka/include
)

if(RDKAFKA_LIB AND RDKAFKA_INCLUDE_DIR)
  set(HAVE_KAFKA TRUE)
  add_definitions(-DHAVE_KAFKA)
  message(STATUS "Kafka support: ENABLED")
  list(APPEND KAFKA_FILES
    src/engines/kafka_engine.cpp
  )
else()
  message(STATUS "Kafka support: DISABLED (librdkafka not found)")
  message(STATUS "  Install librdkafka and rebuild with HAVE_KAFKA")
endif()

# RabbitMQ detection (rabbitmq-c)
set(RABBITMQ_FILES "")
find_library(RABBITMQ_LIB rabbitmq
    PATHS
    /usr/local/lib
    /usr/lib
    /opt/rabbitmq/lib
    ENV LD_LIBRARY_PATH
)

find_path(RABBITMQ_INCLUDE_DIR amqp.h
    PATHS
    /usr/local/include
    /usr/include
    /opt/rabbitmq/include
)

if(RABBITMQ_LIB AND RABBITMQ_INCLUDE_DIR)
  set(HAVE_RABBITMQ TRUE)
  add_definitions(-DHAVE_RABBITMQ)
  message(STATUS "RabbitMQ support: ENABLED")
  list(APPEND RABBITMQ_FILES
    src/engines/rabbitmq_engine.cpp
  )
else()
  message(STATUS "RabbitMQ support: DISABLED (rabbitmq-c not found)")
  message(STATUS "  Install rabbitmq-c and rebuild with HAVE_RABBITMQ")
endif()

# Redis detection (hiredis)
set(REDIS_FILES "")
find_library(HIREDIS_LIB hiredis
    PATHS
    /usr/local/lib
    /usr/lib
    /opt/redis/lib
    ENV LD_LIBRARY_PATH
)

find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h
    PATHS
    /usr/local/include
    /usr/include
    /opt/redis/include
)

if(HIREDIS_LIB AND HIREDIS_INCLUDE_DIR)
  set(HAVE_REDIS TRUE)
  add_definitions(-DHAVE_REDIS)
  message(STATUS "Redis Streams support: ENABLED")
  list(APPEND REDIS_FILES
    src/engines/redis_streams_engine.cpp
  )
else()
  message(STATUS "Redis Streams support: DISABLED (hiredis not found)")
  message(STATUS "  Install hiredis and rebuild with HAVE_REDIS")
endif()

# Protobuf detection
find_package(Protobuf QUIET)
if(Protobuf_FOUND)
  set(HAVE_PROTOBUF TRUE)
  add_definitions(-DHAVE_PROTOBUF)
  message(STATUS "Protobuf support: ENABLED")
else()
  message(STATUS "Protobuf support: DISABLED (protobuf not found)")
  message(STATUS "  Install protobuf and rebuild with HAVE_PROTOBUF")
endif()

# Apache Iceberg detection
find_library(ICEBERG_LIB iceberg
    PATHS
    /usr/local/lib
    /opt/iceberg/lib
)

if(ICEBERG_LIB)
  set(HAVE_ICEBERG TRUE)
  add_definitions(-DHAVE_ICEBERG)
  message(STATUS "Apache Iceberg support: ENABLED")
  list(APPEND SPARK_FILES
    src/engines/iceberg_engine.cpp
  )
else()
  message(STATUS "Apache Iceberg support: DISABLED (library not found)")
endif()

include_directories(
    ./include
    ./include/catalog
    ./include/engines
    ./include/sync
    ./include/utils
    ./include/core
    ./include/governance
    ./include/metrics
    ./include/transformations
    ./include/third_party
    /usr/local/include
    /usr/include/mariadb
    /usr/include/mariadb/mysql
    /opt/microsoft/msodbcsql/include
    /usr/include/libmongoc-1.0
    /usr/include/libbson-1.0
    /usr/include/oracle/21/client64
    /usr/include
)

link_directories(
    /usr/local/lib
    /usr/lib64
    /usr/pgsql-17/lib
    /usr/pgsql-13/lib
    /usr/lib64/mysql
    /opt/microsoft/msodbcsql/lib64
    /usr/lib/oracle/21/client64/lib
    /usr/lib
)

add_executable(DataSync
    src/main.cpp
    src/core/database_config.cpp
    src/core/sync_config.cpp
    src/core/logger.cpp
    src/core/database_log_writer.cpp
    src/governance/DataGovernance.cpp
    src/governance/DataGovernanceMSSQL.cpp
    src/governance/DataGovernanceMariaDB.cpp
    src/governance/DataGovernanceMongoDB.cpp
    ${ORACLE_FILES}
    src/governance/DataQuality.cpp
    src/governance/data_classifier.cpp
    src/governance/QueryStoreCollector.cpp
    src/governance/QueryActivityLogger.cpp
    src/governance/MaintenanceManager.cpp
    src/governance/LineageExtractorMSSQL.cpp
    src/governance/LineageExtractorMariaDB.cpp
    src/governance/LineageExtractorMongoDB.cpp
    src/governance/ColumnCatalogCollector.cpp
    src/governance/ComplianceManager.cpp
    src/governance/AccessControlManager.cpp
    src/governance/DataRetentionManager.cpp
    src/governance/BusinessGlossaryManager.cpp
    src/governance/AlertingManager.cpp
    src/governance/WebhookManager.cpp
    src/governance/ImpactAnalyzer.cpp
    src/governance/ImpactAnalysisRepository.cpp
    src/governance/ColumnLineageExtractor.cpp
    src/governance/TransformationLineageTracker.cpp
    src/governance/DataDictionaryGenerator.cpp
    src/governance/LineageGraphBuilder.cpp
    src/governance/LineageVisualizer.cpp
    src/governance/PipelineMetadataCollector.cpp
    src/governance/PipelineDocumentationGenerator.cpp
    src/governance/DynamicMaskingEngine.cpp
    src/governance/TokenizationManager.cpp
    src/governance/AnonymizationEngine.cpp
    src/governance/FineGrainedPermissions.cpp
    src/monitoring/DistributedTracingManager.cpp
    src/monitoring/APMManager.cpp
    src/monitoring/BottleneckDetector.cpp
    src/monitoring/ResourceTracker.cpp
    src/monitoring/CostTracker.cpp
    src/monitoring/LogAggregator.cpp
    src/monitoring/AdvancedAlertingManager.cpp
    src/monitoring/QueryPerformanceAnalyzer.cpp
    src/catalog/DataLakeMappingManager.cpp
    src/catalog/UnusedObjectsDetector.cpp
    src/metrics/MetricsCollector.cpp
    src/utils/connection_utils.cpp
    src/utils/cluster_name_resolver.cpp
    src/utils/table_utils.cpp
    src/utils/HostnamePatternMatcher.cpp
    src/utils/MariaDBClusterNameProvider.cpp
    src/utils/MSSQLClusterNameProvider.cpp
    src/utils/PostgreSQLClusterNameProvider.cpp
    src/utils/CacheManager.cpp
    src/utils/ResultCache.cpp
    src/utils/MetadataCache.cpp
    src/utils/MemoryManager.cpp
    src/utils/DataCompressor.cpp
    src/storage/ColumnarStorage.cpp
    src/storage/ColumnarReader.cpp
    src/storage/ColumnarWriter.cpp
    src/transformations/transformation_engine.cpp
    src/transformations/lookup_transformation.cpp
    src/transformations/aggregate_transformation.cpp
    src/transformations/join_transformation.cpp
    src/transformations/router_transformation.cpp
    src/transformations/union_transformation.cpp
    src/transformations/sorter_transformation.cpp
    src/transformations/expression_transformation.cpp
    src/transformations/data_cleansing_transformation.cpp
    src/transformations/rank_transformation.cpp
    src/transformations/sequence_generator_transformation.cpp
    src/transformations/window_functions_transformation.cpp
    src/transformations/normalizer_transformation.cpp
    src/transformations/json_parser_transformation.cpp
    src/transformations/geolocation_transformation.cpp
    src/transformations/data_validation_transformation.cpp
    src/transformations/deduplication_transformation.cpp
    src/engines/mariadb_engine.cpp
    src/engines/mssql_engine.cpp
    src/engines/postgres_engine.cpp
    src/engines/mongodb_engine.cpp
    src/engines/db2_engine.cpp
    src/catalog/metadata_repository.cpp
    src/catalog/catalog_cleaner.cpp
    src/catalog/catalog_manager.cpp
    src/catalog/metadata_cache_repository.cpp
    src/catalog/catalog_lock.cpp
    src/sync/DatabaseToPostgresSync.cpp
    src/sync/MariaDBToPostgres.cpp
    src/sync/MSSQLToPostgres.cpp
    src/sync/MongoDBToPostgres.cpp
    src/sync/DB2ToPostgres.cpp
    src/sync/SchemaSync.cpp
    src/sync/StreamingData.cpp
    src/sync/TableProcessorThreadPool.cpp
    src/sync/APIToDatabaseSync.cpp
    src/catalog/api_catalog_repository.cpp
    src/catalog/csv_catalog_repository.cpp
    src/catalog/google_sheets_catalog_repository.cpp
    src/engines/api_engine.cpp
    src/catalog/custom_jobs_repository.cpp
    src/catalog/data_warehouse_repository.cpp
    src/catalog/data_vault_repository.cpp
    src/catalog/workflow_repository.cpp
    src/catalog/dbt_repository.cpp
    src/sync/DBTExecutor.cpp
    src/sync/CustomJobExecutor.cpp
    src/sync/DataWarehouseBuilder.cpp
    src/sync/DataVaultBuilder.cpp
    src/sync/WorkflowExecutor.cpp
    src/sync/EventTriggerManager.cpp
    src/sync/DataDrivenScheduler.cpp
    src/sync/BackfillManager.cpp
    src/sync/TaskQueue.cpp
    src/catalog/WorkflowVersionManager.cpp
    src/engines/csv_engine.cpp
    src/sync/CSVToDatabaseSync.cpp
    src/engines/google_sheets_engine.cpp
    src/sync/GoogleSheetsToDatabaseSync.cpp
    src/sync/PostgreSQLToPostgres.cpp
    src/engines/redshift_engine.cpp
    src/engines/snowflake_engine.cpp
    src/engines/bigquery_engine.cpp
    src/engines/postgres_warehouse_engine.cpp
    src/engines/salesforce_engine.cpp
    src/engines/sap_engine.cpp
    src/engines/teradata_engine.cpp
    src/engines/netezza_engine.cpp
    src/engines/hive_engine.cpp
    src/engines/cassandra_engine.cpp
    src/engines/dynamodb_engine.cpp
    src/engines/as400_engine.cpp
    src/engines/s3_engine.cpp
    src/engines/azure_blob_engine.cpp
    src/engines/gcs_engine.cpp
    src/engines/ftp_engine.cpp
    src/engines/email_engine.cpp
    src/engines/soap_engine.cpp
    src/engines/graphql_engine.cpp
    src/engines/excel_engine.cpp
    src/engines/fixed_width_engine.cpp
    src/engines/ebcdic_engine.cpp
    src/engines/xml_engine.cpp
    src/engines/avro_engine.cpp
    src/engines/parquet_engine.cpp
    src/engines/orc_engine.cpp
    src/engines/compressed_file_engine.cpp
    ${SPARK_FILES}
    ${HADOOP_FILES}
    ${KAFKA_FILES}
    ${RABBITMQ_FILES}
    ${REDIS_FILES}
    src/sync/DistributedProcessingManager.cpp
    src/sync/StreamProcessingManager.cpp
    src/sync/WindowingProcessor.cpp
    src/sync/StatefulProcessor.cpp
    src/sync/EventTimeProcessor.cpp
    src/sync/CEPProcessor.cpp
    src/sync/BinlogReader.cpp
    src/sync/WALReader.cpp
    src/sync/TransactionLogReader.cpp
    src/sync/ChangeStreamsReader.cpp
    src/sync/MessageSerializer.cpp
    src/sync/DistributedJoinExecutor.cpp
    src/sync/JoinOptimizer.cpp
    src/sync/PartitioningManager.cpp
    src/sync/PartitionPruner.cpp
    src/sync/PushdownOptimizer.cpp
    src/sync/IncrementalProcessor.cpp
    src/sync/CDCStrategyManager.cpp
    src/sync/MergeStrategyExecutor.cpp
    src/governance/SchemaEvolutionManager.cpp
    src/transformations/spark_translator.cpp
    src/transformations/spark_transformation.cpp
)

# Configure executable output directory to project root
set_target_properties(DataSync PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Add JSON library
add_definitions(-DJSON_USE_IMPLICIT_CONVERSIONS=1)

set(ORACLE_LIBS "")
if(HAVE_ORACLE_HEADER)
    set(ORACLE_LIBS clntsh)
endif()

# Optional libraries for new connectors
# These will be enabled when the libraries are installed
set(OPTIONAL_LIBS "")

# Stream processing libraries
if(HAVE_KAFKA)
    list(APPEND OPTIONAL_LIBS ${RDKAFKA_LIB})
    include_directories(${RDKAFKA_INCLUDE_DIR})
endif()

if(HAVE_RABBITMQ)
    list(APPEND OPTIONAL_LIBS ${RABBITMQ_LIB})
    include_directories(${RABBITMQ_INCLUDE_DIR})
endif()

if(HAVE_REDIS)
    list(APPEND OPTIONAL_LIBS ${HIREDIS_LIB})
    include_directories(${HIREDIS_INCLUDE_DIR})
endif()

if(HAVE_PROTOBUF)
    list(APPEND OPTIONAL_LIBS protobuf::libprotobuf)
endif()

# Compression libraries
# zlib (GZIP compression)
find_library(ZLIB_LIB z)
if(ZLIB_LIB)
    list(APPEND OPTIONAL_LIBS ${ZLIB_LIB})
    message(STATUS "Found zlib: ${ZLIB_LIB}")
endif()

# LZ4 compression
find_library(LZ4_LIB lz4
    PATHS
    /usr/local/lib
    /usr/lib
    ENV LD_LIBRARY_PATH
)
find_path(LZ4_INCLUDE_DIR lz4.h
    PATHS
    /usr/local/include
    /usr/include
)
if(LZ4_LIB AND LZ4_INCLUDE_DIR)
    set(HAVE_LZ4 TRUE)
    add_definitions(-DHAVE_LZ4)
    list(APPEND OPTIONAL_LIBS ${LZ4_LIB})
    include_directories(${LZ4_INCLUDE_DIR})
    message(STATUS "LZ4 support: ENABLED")
else()
    message(STATUS "LZ4 support: DISABLED (liblz4 not found)")
endif()

# Snappy compression
find_library(SNAPPY_LIB snappy
    PATHS
    /usr/local/lib
    /usr/lib
    ENV LD_LIBRARY_PATH
)
find_path(SNAPPY_INCLUDE_DIR snappy.h
    PATHS
    /usr/local/include
    /usr/include
)
if(SNAPPY_LIB AND SNAPPY_INCLUDE_DIR)
    set(HAVE_SNAPPY TRUE)
    add_definitions(-DHAVE_SNAPPY)
    list(APPEND OPTIONAL_LIBS ${SNAPPY_LIB})
    include_directories(${SNAPPY_INCLUDE_DIR})
    message(STATUS "Snappy support: ENABLED")
else()
    message(STATUS "Snappy support: DISABLED (libsnappy not found)")
endif()

find_library(BZIP2_LIB bz2)
if(BZIP2_LIB)
    list(APPEND OPTIONAL_LIBS ${BZIP2_LIB})
    message(STATUS "Found bzip2: ${BZIP2_LIB}")
endif()

find_library(LZ4_LIB lz4)
if(LZ4_LIB)
    list(APPEND OPTIONAL_LIBS ${LZ4_LIB})
    message(STATUS "Found lz4: ${LZ4_LIB}")
endif()

# Network and protocol libraries
find_library(SSH2_LIB ssh2)
if(SSH2_LIB)
    list(APPEND OPTIONAL_LIBS ${SSH2_LIB})
    message(STATUS "Found libssh2: ${SSH2_LIB}")
endif()

# File format libraries
find_library(XLSXWRITER_LIB xlsxwriter)
if(XLSXWRITER_LIB)
    list(APPEND OPTIONAL_LIBS ${XLSXWRITER_LIB})
    message(STATUS "Found libxlsxwriter: ${XLSXWRITER_LIB}")
endif()

find_library(PUGIXML_LIB pugixml)
if(PUGIXML_LIB)
    list(APPEND OPTIONAL_LIBS ${PUGIXML_LIB})
    message(STATUS "Found pugixml: ${PUGIXML_LIB}")
endif()

# Data format libraries
find_library(AVRO_LIB avrocpp)
if(AVRO_LIB)
    list(APPEND OPTIONAL_LIBS ${AVRO_LIB})
    add_definitions(-DHAVE_AVRO_CPP)
    message(STATUS "Found avro-cpp: ${AVRO_LIB}")
endif()

find_library(PARQUET_LIB parquet)
if(PARQUET_LIB)
    list(APPEND OPTIONAL_LIBS ${PARQUET_LIB})
    message(STATUS "Found parquet-cpp: ${PARQUET_LIB}")
endif()

# Cloud SDKs
# AWS SDK
find_package(aws-sdk-cpp QUIET)
if(aws-sdk-cpp_FOUND)
    list(APPEND OPTIONAL_LIBS aws-cpp-sdk-core aws-cpp-sdk-s3)
    add_definitions(-DHAVE_AWS_SDK)
    message(STATUS "Found AWS SDK")
    # Check for DynamoDB SDK
    find_library(AWS_SDK_DYNAMODB_LIB aws-cpp-sdk-dynamodb)
    if(AWS_SDK_DYNAMODB_LIB)
        list(APPEND OPTIONAL_LIBS ${AWS_SDK_DYNAMODB_LIB})
        add_definitions(-DHAVE_DYNAMODB_SDK)
        message(STATUS "Found AWS DynamoDB SDK")
    else()
        message(STATUS "AWS DynamoDB SDK not found - DynamoDBEngine will have limited functionality")
    endif()
else()
    # Try finding libraries directly
    find_library(AWS_SDK_CORE_LIB aws-cpp-sdk-core)
    find_library(AWS_SDK_S3_LIB aws-cpp-sdk-s3)
    if(AWS_SDK_CORE_LIB AND AWS_SDK_S3_LIB)
        list(APPEND OPTIONAL_LIBS ${AWS_SDK_CORE_LIB} ${AWS_SDK_S3_LIB})
        add_definitions(-DHAVE_AWS_SDK)
        message(STATUS "Found AWS SDK libraries")
    endif()
    # Also check for DynamoDB SDK
    find_library(AWS_SDK_DYNAMODB_LIB aws-cpp-sdk-dynamodb)
    if(AWS_SDK_DYNAMODB_LIB)
        list(APPEND OPTIONAL_LIBS ${AWS_SDK_DYNAMODB_LIB})
        add_definitions(-DHAVE_DYNAMODB_SDK)
        message(STATUS "Found AWS DynamoDB SDK")
    else()
        message(STATUS "AWS DynamoDB SDK not found - DynamoDBEngine will have limited functionality")
    endif()
endif()

# Azure SDK - try to find azure-storage-cpp
find_library(AZURE_STORAGE_LIB azure-storage)
if(AZURE_STORAGE_LIB)
    list(APPEND OPTIONAL_LIBS ${AZURE_STORAGE_LIB})
    message(STATUS "Found Azure Storage SDK: ${AZURE_STORAGE_LIB}")
endif()

# Google Cloud SDK
find_package(google_cloud_cpp_storage QUIET)
if(google_cloud_cpp_storage_FOUND)
    list(APPEND OPTIONAL_LIBS google-cloud-cpp::storage)
    message(STATUS "Found Google Cloud Storage SDK")
else()
    find_library(GCS_LIB google-cloud-cpp)
    if(GCS_LIB)
        list(APPEND OPTIONAL_LIBS ${GCS_LIB})
        message(STATUS "Found Google Cloud SDK: ${GCS_LIB}")
    endif()
endif()

target_link_libraries(DataSync
    mariadb
    mysqlclient
    pqxx
    pq
    pthread
    odbc
    mongoc-1.0
    bson-1.0
    ${ORACLE_LIBS}
    stdc++fs
    curl
    crypto
    ${OPTIONAL_LIBS}
)
