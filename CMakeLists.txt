cmake_minimum_required(VERSION 3.16)
project(sds CXX ASM)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-O3")

include_directories(include)

option(QP_MULTIPLEXING "Enable QP Multiplexing" OFF)
option(MEMORY_MULTIPLEXING "Enable Memory Multiplexing" OFF)

if(QP_MULTIPLEXING OR MEMORY_MULTIPLEXING)
    add_compile_definitions(
        $<$<BOOL:${QP_MULTIPLEXING}>:QP_MULTIPLEXING>
        $<$<BOOL:${MEMORY_MULTIPLEXING}>:MEMORY_MULTIPLEXING>
    )
endif()

add_subdirectory(util)
add_subdirectory(smart)
add_subdirectory(rdmax)

add_subdirectory(test)