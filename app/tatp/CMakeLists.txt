add_executable(tatp tatp.cpp tatp_utils.cpp)

target_link_libraries(tatp PUBLIC rdmax boost_system boost_coroutine ${FASST_PATH}/mica/util/config.o ${FASST_PATH}/mica/util/cityhash/city_mod.o)