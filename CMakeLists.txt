cmake_minimum_required(VERSION 3.18)
project(shopping_sessions LANGUAGES CXX)
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)
find_package(Threads REQUIRED)
add_executable(shopping-sessions pipeline/native.cpp)
target_compile_features(shopping-sessions PRIVATE cxx_std_17)
target_compile_options(shopping-sessions PRIVATE -O3)
target_link_libraries(shopping-sessions PRIVATE Parquet::parquet_static Arrow::arrow_static Threads::Threads)
# Link only the Arrow/Parquet code actually used. Standard OS/C++ libraries
# remain dynamic; this is not a fully static libc executable.
if(APPLE)
  target_link_options(shopping-sessions PRIVATE -Wl,-dead_strip)
elseif(UNIX)
  target_compile_options(shopping-sessions PRIVATE -ffunction-sections -fdata-sections)
  target_link_options(shopping-sessions PRIVATE -Wl,--gc-sections)
endif()
# Profile-guided optimization: configure with PGO=generate, run the binary on
# training data (it writes .gcda profiles next to its object files), then
# reconfigure the same build directory with PGO=use and rebuild.
set(PGO "OFF" CACHE STRING "Profile-guided optimization: OFF, generate or use")
if(PGO STREQUAL "generate")
  target_compile_options(shopping-sessions PRIVATE -fprofile-generate -fprofile-update=atomic)
  target_link_options(shopping-sessions PRIVATE -fprofile-generate)
elseif(PGO STREQUAL "use")
  target_compile_options(shopping-sessions PRIVATE -fprofile-use -fprofile-correction -Wno-missing-profile)
endif()
