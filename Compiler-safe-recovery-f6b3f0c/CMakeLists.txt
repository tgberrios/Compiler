cmake_minimum_required(VERSION 3.16)
project(DataSync)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(
    ./include
    /usr/include/libbson-1.0
    /usr/include/libmongoc-1.0   # <-- corregido aquí
    /usr/local/include/jsoncpp
    /usr/include/mariadb
    /usr/include/mariadb/mysql
    /opt/microsoft/msodbcsql/include
)

link_directories(
    /usr/local/lib
    /usr/lib64
    /usr/pgsql-13/lib
    /opt/microsoft/msodbcsql/lib64
)

add_executable(DataSync 
    src/main.cpp 
    src/Config.cpp 
    src/logger.cpp 
    src/DataGovernance.cpp 
    src/DDLExporter.cpp 
    src/MetricsCollector.cpp
    src/ConnectionPool.cpp
    src/ConnectionGuard.cpp
    src/SyncReporter.cpp
)

set_target_properties(DataSync PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)

target_link_libraries(DataSync
    mariadb
    pqxx
    pq
    pthread
    odbc
    mongoc-1.0
    bson-1.0
    jsoncpp  # <-- enlazar jsoncpp también
    stdc++fs
)
