file(GLOB SOURCE_RGZIP "*.cpp")

add_library(rgzip_lib STATIC ${SOURCE_RGZIP})
target_include_directories(rgzip_lib PUBLIC
    ${PHOTON_INCLUDE_DIR}
)
target_link_libraries(rgzip_lib photon_static)
# if(BUILD_TESTING)
#   add_subdirectory(test)
# endif()
