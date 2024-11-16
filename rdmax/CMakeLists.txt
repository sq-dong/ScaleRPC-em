add_library(rdmax rpc.cpp resource_manager.cpp task.cpp thread.cpp)
target_link_libraries(rdmax PUBLIC numa pthread fcontext ibverbs mlx5)
