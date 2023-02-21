FetchContent_Declare(
  rapidjson
  GIT_REPOSITORY https://gitee.com/Tencent/RapidJSON.git
  GIT_TAG 80b6d1c83402a5785c486603c5611923159d0894
)
FetchContent_GetProperties(rapidjson)
if (NOT rapidjson_POPULATED)
  FetchContent_Populate(rapidjson)
endif()

add_definitions("-DRAPIDJSON_HAS_STDSTRING=1")