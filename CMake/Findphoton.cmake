include(FetchContent)
set(FETCHCONTENT_QUIET false)

FetchContent_Declare(
  photon
  GIT_REPOSITORY https://github.com/alibaba/PhotonLibOS.git
  GIT_TAG v0.6.10
)

if(BUILD_TESTING)
  set(BUILD_TESTING 0)
  FetchContent_MakeAvailable(photon)
  set(BUILD_TESTING 1)
else()
  FetchContent_MakeAvailable(photon)
endif()

if (BUILD_CURL_FROM_SOURCE)
  find_package(OpenSSL REQUIRED)
  find_package(CURL REQUIRED)
  add_dependencies(photon_obj CURL::libcurl OpenSSL::SSL OpenSSL::Crypto)
endif()

set(PHOTON_INCLUDE_DIR ${photon_SOURCE_DIR}/include/)
