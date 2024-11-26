// Initialization functions for a FixedTable datastore

#ifndef DS_FIXEDTABLE_H
#define DS_FIXEDTABLE_H

#include "../ds.h"
#include "mica/table/fixedtable.h"
#include "rdmax/rpc.h"

typedef ::mica::table::BasicFixedTableConfig FixedTableConfig;
typedef ::mica::table::FixedTable<FixedTableConfig> FixedTable;
typedef ::mica::table::Result MicaResult; /* An enum */

#define DS_FIXEDTABLE_DPRINTF 0

// Debug macros
#define ds_fixedtable_printf(fmt, ...)   \
  do {                                   \
    if (DS_FIXEDTABLE_DPRINTF) {         \
      fprintf(stderr, fmt, __VA_ARGS__); \
      fflush(stderr);                    \
    }                                    \
  } while (0)

/*
 * Initialize the table with pre-defined SHM keys.
 * @config_filepath contains config parameters for the allocator and pool.
 * @val_size is the application-level opaque buffer size.
 */
static FixedTable* ds_fixedtable_init(const char* config_filepath,
                                      size_t val_size, int bkt_shm_key,
                                      bool is_primary) {
  ds_do_checks();

  auto config = ::mica::util::Config::load_file(config_filepath);

  /* Check that CRCW mode is set */
  auto table_config = config.get("table");
  if (!table_config.get("concurrent_read").get_bool() ||
      !table_config.get("concurrent_write").get_bool()) {
    fprintf(stderr, "HoTS Error: FixedTable only supports CRCW. Exiting.\n");
    exit(-1);
  }

  FixedTableConfig::Alloc* alloc =
      new FixedTableConfig::Alloc(config.get("alloc"));

  FixedTable* table = new (std::align_val_t(128))
      FixedTable(config.get("table"), val_size, bkt_shm_key, alloc, is_primary);

  return table;
}

#include "datastore/fixedtable/ds_fixedtable_handler.h"
#endif  // DS_FIXEDTABLE_H