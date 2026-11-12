#!/bin/bash
# rm -rf build
mkdir build
cd build

CMAKE_COMMAND="cmake .."

echo "Running: $CMAKE_COMMAND"
$CMAKE_COMMAND
make -j
# Same rdma-core build: both libs required at runtime (see patch/guide.md).
cp ../patch/libibverbs.so libibverbs.so
cp ../patch/libmlx5.so libmlx5.so
ln -sfn libmlx5.so libmlx5-rdmav57.so
echo "Build completed with \`$CMAKE_COMMAND\`"
