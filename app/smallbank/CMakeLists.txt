add_executable(smallbank smallbank.cpp sb_utils.cpp)

target_link_libraries(smallbank PUBLIC rdmax boost_system boost_coroutine ${FASST_PATH}/mica/util/config.o ${FASST_PATH}/mica/util/cityhash/city_mod.o)