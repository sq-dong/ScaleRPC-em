By default, the RDMA driver allows **at most** 12 median-latency
doorbell registers per RDMA context, which make our
optimiazation impractial. To remove such restriction, we have hacked the rdma-core library used for our test platform.

You may have to recompile the rdma-core library and preload it when executing the application.

1. Fetch the source code of rdma-core matching your OFED / system libibverbs
   (this host uses IBVERBS_PRIVATE_57; set `IBVERBS_PABI_VERSION` accordingly in
   rdma-core `CMakeLists.txt` if needed).

2. Edit `rdma-core/providers/mlx5/mlx5.c`, in `get_num_low_lat_uuars`:

```c
static int get_num_low_lat_uuars(int tot_uuars)
{
	char *env;
	int num = 4;

	env = getenv("MLX5_NUM_LOW_LAT_UUARS");
	if (env)
		num = atoi(env);

	if (num < 0)
		return -EINVAL;

	// PLEASE REMOVE THE NEXT LINE
	// num = max(num, tot_uuars - MLX5_MED_BFREGS_TSHOLD);
	return num;
}
```

3. Recompile rdma-core (e.g. `ninja mlx5`).

4. Copy **both** libraries from that same build into `patch/` (and `build.sh`
   copies them into `build/`):

	- `libmlx5.so`
	- `libibverbs.so`

5. Run with matching preload + library path:

	```bash
	cd build
	export LD_LIBRARY_PATH=$PWD
	export LD_PRELOAD=$PWD/libibverbs.so:$PWD/libmlx5.so
	./your_app <params>
	```

	Preloading only `libmlx5.so` against the system `libibverbs` will fail to
	open the device (private cmd ABI mismatch), even if the symbol version
	number matches. If `LD_PRELOAD` is unset, SMART prints a warning and
	disables thread-aware UUAR allocation.
