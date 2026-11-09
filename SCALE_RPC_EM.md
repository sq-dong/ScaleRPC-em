# ScaleRPC-em (host-only)

This branch (`ConnectionGrouping`) is the RDMAX ScaleRPC-em host path:
timer-based connection grouping for SmallBank / TATP.

## Defaults in this tree

- `MEMORY_MULTIPLEXING=0` in `include/rdmax/rpc.h`  
  Shared request region needs in-network dispatch (e.g. Tofino). Without it,
  keep per-client slots.
- `QP_MULTIPLEXING=0` (ScaleRPC-em does not use RDMAX QP multiplexing).
- `NUM_MACHINES=1`, `NUM_CLIENTS=960` when memory multiplexing is off.
- Configs point at a single client IB address (`10.10.1.72`) and `gid_idx: 0`.
  Edit `config/{test_rpc,smallbank,tatp}.json` and `config/smart_config.json`
  for your cluster.
- Runtime client count may be `<= NUM_CLIENTS` (asserts relaxed for sweeps).

## Build / run (short)

```bash
# 1GiB hugepages on server and client hosts
echo 8 | sudo tee /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages

bash ./build.sh
# Optional: place a matching UUAR-patched libibverbs.so next to patch/libmlx5.so
# then LD_PRELOAD both from build/.

cd build
# Client first, then server. Example: 8 threads, 80 clients, depth 1.
# LD_PRELOAD=./libibverbs.so:./libmlx5.so ./test/test_rpc ...
```

TATP under tight 1GiB hugepage budgets:

```bash
export SMART_CONFIG_PATH=../config/smart_config_tatp.json
```

Start **client before server**. Run binaries from `build/`.
