cmake_minimum_required(VERSION 2.8)
project(RDDWithMapReduce)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# requires MPI
find_package(MPI REQUIRED)
include_directories(${MPI_INCLUDE_PATH})

# requires msgpack
include_directories(${RDDWithMapReduce_SOURCE_DIR}/include)

set(SOURCE_FILES main.cc mapper.h reducer.h rdd.cc rdd.h text_rdd.cc text_rdd.h word_count_mapper.cc word_count_mapper.h key_value_rdd.h word_count_reducer.cc word_count_reducer.h)
add_executable(RDDWithMapReduce ${SOURCE_FILES})

target_link_libraries(RDDWithMapReduce ${MPI_LIBRARIES})