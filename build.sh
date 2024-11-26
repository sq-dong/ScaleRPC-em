#!/bin/bash
# rm -rf build
mkdir build
cd build

# show_help() {
#     echo "Usage: $0 [options]"
#     echo "Options:"
#     echo "  --qp            Enable QP Multiplexing"
#     echo "  --memory        Enable Memory Multiplexing"
#     echo "  -h, --help      Show this help message and exit"
# }

CMAKE_COMMAND="cmake .."
# while [[ "$#" -gt 0 ]]; do
#     case $1 in
#         --qp) CMAKE_COMMAND+=" -DQP_MULTIPLEXING=ON" ;;
#         --memory) CMAKE_COMMAND+=" -DMEMORY_MULTIPLEXING=ON" ;;
#         -h|--help) show_help; exit 0 ;;
#         *) echo "Unknown parameter passed: $1"; show_help; exit 1 ;;
#     esac
#     shift
# done

echo "Running: $CMAKE_COMMAND"
$CMAKE_COMMAND
make -j
cp ../patch/libmlx5.so libmlx5.so
echo "Build completed with \`$CMAKE_COMMAND\`"