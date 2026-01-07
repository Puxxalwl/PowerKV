cmake_minimum_required(VERSION 3.10)
project(PowerKV)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -march=native -pthread")

add_library(powerkv SHARED src/powerkv.cpp)

target_link_libraries(demo powerkv)