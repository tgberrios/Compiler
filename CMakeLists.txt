cmake_minimum_required(VERSION 3.16)
project(DataSync)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(
    ./include
    /usr/local/include
    /usr/include/mariadb
    /usr/include/mariadb/mysql
    /opt/microsoft/msodbcsql/include
    /usr/include/libmongoc-1.0
    /usr/include/libbson-1.0
)

link_directories(
    /usr/local/lib
    /usr/lib64
    /opt/microsoft/msodbcsql/lib64
)

add_executable(DataSync 
    src/main.cpp 
    src/Config.cpp 
    src/logger.cpp 
    src/DataGovernance.cpp 
    src/DDLExporter.cpp 
    src/MetricsCollector.cpp 
    src/SyncReporter.cpp 
    src/ConnectionPool.cpp
    src/DataQuality.cpp
)

# Configurar para que el ejecutable se genere en el directorio raíz
set_target_properties(DataSync PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Add JSON library
add_definitions(-DJSON_USE_IMPLICIT_CONVERSIONS=1)

target_link_libraries(DataSync
    mariadb
    pqxx
    pq
    pthread
    odbc
    mongoc-1.0
    bson-1.0
)
