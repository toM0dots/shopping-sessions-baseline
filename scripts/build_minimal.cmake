# cmake -DROOT=/path/to/repo [-DJOBS=4] -P scripts/build_minimal.cmake
cmake_minimum_required(VERSION 3.18)
if(NOT DEFINED ROOT)
  get_filename_component(ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
endif()
if(NOT DEFINED JOBS)
  set(JOBS 4)
endif()
set(DEPS "${ROOT}/dev-tools")
set(ARCHIVE "${DEPS}/arrow-19.0.1.tar.gz")
set(SOURCE "${DEPS}/arrow-apache-arrow-19.0.1")
set(PREFIX "${DEPS}/arrow-minimal")
set(DIGEST "4c898504958841cc86b6f8710ecb2919f96b5e10fa8989ac10ac4fca8362d86a")
file(MAKE_DIRECTORY "${DEPS}")
if(NOT EXISTS "${ARCHIVE}")
  file(DOWNLOAD "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-19.0.1.tar.gz"
       "${ARCHIVE}" EXPECTED_HASH "SHA256=${DIGEST}" TLS_VERIFY ON SHOW_PROGRESS)
endif()
file(SHA256 "${ARCHIVE}" ACTUAL_DIGEST)
if(NOT ACTUAL_DIGEST STREQUAL DIGEST)
  message(FATAL_ERROR "Arrow source archive checksum mismatch")
endif()
if(NOT EXISTS "${SOURCE}/cpp/CMakeLists.txt")
  file(ARCHIVE_EXTRACT INPUT "${ARCHIVE}" DESTINATION "${DEPS}")
endif()
function(run)
  execute_process(COMMAND ${ARGV} RESULT_VARIABLE status)
  if(NOT status STREQUAL "0")
    message(FATAL_ERROR "Build command failed (${status}): ${ARGV}")
  endif()
endfunction()
run("${CMAKE_COMMAND}" -S "${SOURCE}/cpp" -B "${DEPS}/arrow-minimal-build"
    -DCMAKE_BUILD_TYPE=Release "-DCMAKE_INSTALL_PREFIX=${PREFIX}" -DCMAKE_CXX_STANDARD=17
    -DARROW_BUILD_SHARED=OFF -DARROW_BUILD_STATIC=ON -DARROW_PARQUET=ON
    -DARROW_WITH_SNAPPY=ON -DARROW_DEPENDENCY_SOURCE=BUNDLED
    -DARROW_MIMALLOC=OFF -DARROW_JEMALLOC=OFF -DARROW_IPC=OFF -DARROW_COMPUTE=OFF
    -DARROW_CSV=OFF -DARROW_JSON=OFF -DARROW_FILESYSTEM=OFF -DARROW_S3=OFF
    -DARROW_SIMD_LEVEL=NONE -DPARQUET_REQUIRE_ENCRYPTION=OFF)
run("${CMAKE_COMMAND}" --build "${DEPS}/arrow-minimal-build" --parallel "${JOBS}")
run("${CMAKE_COMMAND}" --install "${DEPS}/arrow-minimal-build")
if(DEPS_ONLY)
  return()
endif()
run("${CMAKE_COMMAND}" -S "${ROOT}" -B "${ROOT}/build/minimal"
    -DCMAKE_BUILD_TYPE=Release "-DCMAKE_PREFIX_PATH=${PREFIX}")
run("${CMAKE_COMMAND}" --build "${ROOT}/build/minimal" --parallel "${JOBS}")
file(COPY "${ROOT}/build/minimal/shopping-sessions" DESTINATION "${ROOT}/build")
