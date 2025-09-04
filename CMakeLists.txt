cmake_minimum_required(VERSION 3.16)
project(DataSync)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(
    ./include
    /usr/local/include
    /usr/include/mariadb
    /usr/include/mariadb/mysql
)

link_directories(
    /usr/local/lib
    /usr/lib64
)

add_executable(DataSync src/main.cpp)

# Configurar para que el ejecutable se genere en el directorio raíz
set_target_properties(DataSync PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}
)

target_link_libraries(DataSync
    mariadb
    pqxx
    pq
    pthread
)
