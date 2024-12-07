/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2022-2023 Feng Ren, Tsinghua University
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "tatp.h"

#include <immintrin.h>
#include <inttypes.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <boost/bind.hpp>
#include <boost/coroutine/all.hpp>
#include <cassert>
#include <chrono>
#include <iostream>
#include <random>
#include <vector>

#include "logger/logger.h"
#include "rdmax/mapping.h"
#include "rdmax/rpc.h"
#include "rdmax/thread.h"
#include "tx/tx.h"

using namespace rdmax;
using namespace boost::coroutines;

// Define the coroutine typedefs
typedef boost::coroutines::symmetric_coroutine<void> symmetric_coroutine_t;
typedef symmetric_coroutine_t::call_type coro_call_t;
typedef symmetric_coroutine_t::yield_type coro_yield_t;
typedef int coro_id_t;

int numa_node;
static const size_t MEM_POOL_SIZE = (1ull << 30);
static int running_time = 10;
int num_threads = 1;
int num_clients = 1;
int stop_signal = 0;
double connect_time = 0.0;
std::string dump_file_path;
std::string dump_prefix;
std::atomic<uint64_t> total_cmsg_attempts(0);
std::atomic<uint64_t> total_tx_attempted(0);
std::atomic<uint64_t> total_tx_committed(0);
pthread_barrier_t barrier;
RpcNode *rpc_server;
RpcNode *rpc_client;
TATP *tatp;
__thread Mappings *mappings;
__thread Logger *logger;
__thread tatp_txn_type_t *workgen_arr;
__thread uint64_t tg_seed = 0xdeadbeef;       /* Thread-global random seed */
__thread long long stat_tx_attempted_tot = 0; /* Collected in non-debug */
__thread long long stat_tx_committed_tot = 0; /* Collected in non-debug */
__thread long long stat_tx_attempted[TATP_TXN_TYPES];
__thread long long stat_tx_committed[TATP_TXN_TYPES];

static const int worker_depth = 128;
static int clt_cmsg_depth = 1;

void client_report(uint64_t elapsed_time) {
  auto attempted_tput = total_tx_attempted.load() / elapsed_time / 1000000.0;
  auto committed_tput = total_tx_committed.load() / elapsed_time / 1000000.0;
  SDS_INFO(
      "%s: #threads = %d, #clients = %d, #machines = %d, #replicas = %d, "
      "elapsed_time = %ld, total_tx_attempted = %ld, total_tx_committed = %ld, "
      "attempted_tput = %.2f M/s, committed_tput = %.2f M/s",
      dump_prefix.c_str(), num_threads, num_clients, tatp->num_machines,
      tatp->num_replicas, elapsed_time, total_tx_attempted.load(),
      total_tx_committed.load(), attempted_tput, committed_tput);
}

void server_report(uint64_t elapsed_time) {
  auto cmsg_tput = total_cmsg_attempts.load() / elapsed_time / 1000000.0;
  SDS_INFO(
      "%s: #threads = %d, #clients = %d, #machines = %d, #replicas = %d, "
      "elapsed_time = %ld, total_cmsg_attempted = %ld, cmsg_tput = %.2f M/s",
      dump_prefix.c_str(), num_threads, num_clients, tatp->num_machines,
      tatp->num_replicas, elapsed_time, total_cmsg_attempts.load(), cmsg_tput);
}

#if MEASURE_LATENCY == 1
// Function to get current time in nanoseconds
double get_current_time_in_nanoseconds() {
  using namespace std::chrono;
  auto now = high_resolution_clock::now();
  return duration_cast<nanoseconds>(now.time_since_epoch()).count();
}

// Structure to pass arguments to thread functions
struct ThreadArgs {
  std::vector<double> *latency_samples;
};
#else
struct ThreadArgs {
  int thread_id;
};
#endif

// Profile: Read 1 SUBSCRIBER row
// Maximum messages in a batch = 1
bool txn_get_subscriber_data(coro_yield_t &yield, int coro_id, Tx *tx) {
  int txn_type = static_cast<int>(tatp_txn_type_t::get_subsciber_data);
  tx->start();

  tatp_sub_key_t key;
  key.s_id = tatp->get_nurand_subscriber(&tg_seed);

  hots_obj_t obj;

  tx->add_to_read_set(RPC_SUBSCRIBER_REQ, key.hots_key, &obj);
  tx_status_t ex_result = tx->do_read(yield);
  _unused(ex_result);
  if (ex_result != tx_status_t::in_progress) {
    /* Abort if we read a locked Subscriber record */
    tx->abort_rdonly();
    return false;
  }

  tatp_sub_val_t *val = (tatp_sub_val_t *)&obj.val;
  _unused(val);
  tatp_dassert(obj.val_size > 0);
  tatp_dassert(val->msc_location == tatp_sub_msc_location_magic);

  // tatp_stat_inc(stat_tx_commit_attempted[txn_type], 1);
  tx->commit_single_read();

  return true;
}

// Profile:
// 1. Read 1 SPECIAL_FACILITY row
// 2. Read up to 3 CALL_FORWARDING rows
// 3. Validate up to 4 rows
// Maximum messages in a batch = 4
bool txn_get_new_destination(coro_yield_t &yield, int coro_id, Tx *tx) {
  int txn_type = static_cast<int>(tatp_txn_type_t::get_new_destination);
  tx->start();

  /* Transaction parameters */
  uint32_t s_id = tatp->get_nurand_subscriber(&tg_seed);
  uint8_t sf_type = (hrd_fastrand(&tg_seed) % 4) + 1;
  uint8_t start_time = (hrd_fastrand(&tg_seed) % 3) * 8;
  uint8_t end_time = (hrd_fastrand(&tg_seed) % 24) * 1;

  unsigned cf_to_fetch = (start_time / 8) + 1;
  tatp_dassert(cf_to_fetch >= 1 && cf_to_fetch <= 3);

  /* Fetch a single special facility record */
  hots_obj_t specfac_obj;
  tatp_specfac_key_t specfac_key;

  specfac_key.s_id = s_id;
  specfac_key.sf_type = sf_type;
  tx->add_to_read_set(RPC_SPECIAL_FACILITY_REQ, specfac_key.hots_key,
                      &specfac_obj);

  /*
   * The Special Facility record exists only 62.5% of the time, and is_active
   * 85% of the time. So avoid issuing the Call Forwarding fetches if we
   * have a non-existent or inactive Special Facility record.
   */
  tx_status_t ex_result = tx->do_read(yield);
  _unused(ex_result);
  if (ex_result != tx_status_t::in_progress || specfac_obj.val_size == 0) {
    tx->abort_rdonly();
    return false;
  }

  /* If we are here, the Special Facility record exists. */
  tatp_specfac_val_t *specfac_val = (tatp_specfac_val_t *)&specfac_obj.val;
  tatp_dassert(specfac_val->data_b[0] == tatp_specfac_data_b0_magic);
  if (specfac_val->is_active == 0) {
    tx->abort_rdonly();
    return false;
  }

  /* Fetch possibly multiple call forwarding records. */
  hots_obj_t callfwd_obj[3];
  tatp_callfwd_key_t callfwd_key[3];

  for (unsigned i = 0; i < cf_to_fetch; i++) {
    callfwd_key[i].s_id = s_id;
    callfwd_key[i].sf_type = sf_type;
    callfwd_key[i].start_time = (i * 8);

    tx->add_to_read_set(RPC_CALL_FORWARDING_REQ, callfwd_key[i].hots_key,
                        &callfwd_obj[i]);
  }

  ex_result = tx->do_read(yield);
  _unused(ex_result);
  if (ex_result != tx_status_t::in_progress) {
    tx->abort_rdonly();
    return false;
  }

  bool callfwd_success = false;
  for (unsigned i = 0; i < cf_to_fetch; i++) {
    if (callfwd_obj[i].val_size == 0) {
      continue;
    }

    tatp_callfwd_val_t *callfwd_val = (tatp_callfwd_val_t *)&callfwd_obj[i].val;
    tatp_dassert(callfwd_val->numberx[0] == tatp_callfwd_numberx0_magic);

    if (callfwd_key[i].start_time <= start_time &&
        end_time < callfwd_val->end_time) {
      /* All conditions satisfied */
      callfwd_success = true;
    }
  }

  if (callfwd_success) {
    /* Try to commit, which may fail during validation */
    // tatp_stat_inc(stat_tx_commit_attempted[txn_type], 1);
    tx_status_t commit_result = tx->commit(yield);
    return (commit_result == tx_status_t::committed);
  } else {
    tx->abort_rdonly();
    return false;
  }
}

// Profile: Read 1 ACCESS_INFO row
// Maximum messages in a batch = 1
bool txn_get_access_data(coro_yield_t &yield, int coro_id, Tx *tx) {
  int txn_type = static_cast<int>(tatp_txn_type_t::get_access_data);
  tx->start();

  hots_obj_t obj;
  tatp_accinf_key_t key;
  key.s_id = tatp->get_nurand_subscriber(&tg_seed);
  key.ai_type = (hrd_fastrand(&tg_seed) & 3) + 1;

  tx->add_to_read_set(RPC_ACCESS_INFO_REQ, key.hots_key, &obj);
  tx_status_t ex_result = tx->do_read(yield);
  _unused(ex_result);

  /*
   * No transaction locks Access Info records. Txn status remains @in_progress
   * regardless of whether <s_id, ai_type> is found.
   */
  tatp_dassert(ex_result == tx_status_t::in_progress);

  if (obj.val_size > 0) {
    /* The key was found */
    tatp_accinf_val_t *val = (tatp_accinf_val_t *)&obj.val;
    _unused(val);
    tatp_dassert(val->data1 == tatp_accinf_data1_magic);

    // tatp_stat_inc(stat_tx_commit_attempted[txn_type], 1);
    tx->commit_single_read();
    return true;
  } else {
    /* Key not found */
    tx->abort_rdonly();
    return false;
  }
}

// Profile: Update 1 SUBSCRIBER row and 1 SPECIAL_FACILTY row
// Maximum messages in a batch = 4 (during commit-backup)
bool txn_update_subscriber_data(coro_yield_t &yield, int coro_id, Tx *tx) {
  int txn_type = static_cast<int>(tatp_txn_type_t::update_subscriber_data);
  tx->start();

  /* Transaction parameters */
  uint32_t s_id = tatp->get_nurand_subscriber(&tg_seed);
  uint8_t sf_type = (hrd_fastrand(&tg_seed) % 4) + 1;

  /* Read + lock the subscriber record */
  hots_obj_t sub_obj;
  tatp_sub_key_t sub_key;
  sub_key.s_id = s_id;

  tx->add_to_write_set(RPC_SUBSCRIBER_REQ, sub_key.hots_key, &sub_obj,
                       tx_write_mode_t::update);

  /* Read + lock the special facilty record */
  hots_obj_t specfac_obj;
  tatp_specfac_key_t specfac_key;
  specfac_key.s_id = s_id;
  specfac_key.sf_type = sf_type;

  tx->add_to_write_set(RPC_SPECIAL_FACILITY_REQ, specfac_key.hots_key,
                       &specfac_obj, tx_write_mode_t::update);

  tx_status_t ex_result = tx->do_read(yield);
  tatp_dassert(ex_result == tx_status_t::in_progress ||
               ex_result == tx_status_t::must_abort); /* Locked or not found */

  if (ex_result == tx_status_t::must_abort) {
    tx->abort(yield);
    return false;
  }

  /* If we are here, execution succeeded and we have locks */
  tatp_sub_val_t *sub_val = (tatp_sub_val_t *)sub_obj.val;
  tatp_dassert(sub_val->msc_location == tatp_sub_msc_location_magic);
  sub_val->bits = hrd_fastrand(&tg_seed); /* Update */

  tatp_specfac_val_t *specfac_val = (tatp_specfac_val_t *)specfac_obj.val;
  tatp_dassert(specfac_val->data_b[0] == tatp_specfac_data_b0_magic);
  specfac_val->data_a = hrd_fastrand(&tg_seed); /* Update */

  // tatp_stat_inc(stat_tx_commit_attempted[txn_type], 1);
  tx_status_t commit_status = tx->commit(yield);
  _unused(commit_status);

  /*
   * If we managed to lock the SUBSCRIBER and SPECIAL_FACILTY record, the
   * txn must commit.
   */
  tatp_dassert(commit_status == tx_status_t::committed);

  return true;
}

// Profile:
// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Update a SUBSCRIBER row
// Maximum messages in a batch = 2 (during execute, logging, commit-backup)
bool txn_update_location(coro_yield_t &yield, int coro_id, Tx *tx) {
  int txn_type = static_cast<int>(tatp_txn_type_t::update_location);
  tx->start();

  /* Transaction parameters */
  uint32_t s_id = tatp->get_nurand_subscriber(&tg_seed);
  uint32_t vlr_location = hrd_fastrand(&tg_seed);

  /* Read the secondary subscriber record */
  hots_obj_t sec_sub_obj;
  tatp_sec_sub_key_t sec_sub_key;
  sec_sub_key.sub_nbr = tatp->tatp_sub_nbr_from_sid_fast(s_id);

  tx->add_to_read_set(RPC_SEC_SUBSCRIBER_REQ, sec_sub_key.hots_key,
                      &sec_sub_obj);
  tx_status_t ex_result = tx->do_read(yield);

  tatp_dassert(ex_result == tx_status_t::in_progress); /* Never locked */
  tatp_dassert(sec_sub_obj.val_size > 0);              /* Must exist */

  tatp_sec_sub_val_t *sec_sub_val = (tatp_sec_sub_val_t *)sec_sub_obj.val;
  tatp_dassert(sec_sub_val->magic == tatp_sec_sub_magic);
  tatp_dassert(sec_sub_val->s_id == s_id);

  /* Read + lock the subscriber record */
  hots_obj_t sub_obj;
  tatp_sub_key_t sub_key;
  sub_key.s_id = sec_sub_val->s_id;

  tx->add_to_write_set(RPC_SUBSCRIBER_REQ, sub_key.hots_key, &sub_obj,
                       tx_write_mode_t::update);
  ex_result = tx->do_read(yield);

  tatp_dassert(ex_result == tx_status_t::in_progress ||
               ex_result == tx_status_t::must_abort); /* If locked */

  if (ex_result == tx_status_t::in_progress) {
    tatp_sub_val_t *sub_val = (tatp_sub_val_t *)sub_obj.val;
    tatp_dassert(sub_val->msc_location == tatp_sub_msc_location_magic);
    sub_val->vlr_location = vlr_location; /* Update */

    // tatp_stat_inc(stat_tx_commit_attempted[txn_type], 1);
    tx_status_t commit_status = tx->commit(yield);
    _unused(commit_status);

    /*
     * If we managed to lock the SUBSCRIBER record, the txn must commit.
     * (Validation must succeed because the secondary table is read-only.)
     */
    tatp_dassert(commit_status == tx_status_t::committed);
    return true;
  } else {
    tx->abort(yield);
    return false;
  }
}

// Profile:
// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Read a SPECIAL_FACILTY row
// 3. Insert a CALL_FORWARDING row
// Maximum messages in a batch = 2 (during validate and commit-backup)
bool txn_insert_call_forwarding(coro_yield_t &yield, int coro_id, Tx *tx) {
  int txn_type = static_cast<int>(tatp_txn_type_t::insert_call_forwarding);
  tx->start();

  /* Transaction parameters */
  uint32_t s_id = tatp->get_nurand_subscriber(&tg_seed);
  uint8_t sf_type = (hrd_fastrand(&tg_seed) % 4) + 1;
  uint8_t start_time = (hrd_fastrand(&tg_seed) % 3) * 8;
  uint8_t end_time = (hrd_fastrand(&tg_seed) % 24) * 1;

  // Read the secondary subscriber record
  hots_obj_t sec_sub_obj;
  tatp_sec_sub_key_t sec_sub_key;
  sec_sub_key.sub_nbr = tatp->tatp_sub_nbr_from_sid_fast(s_id);

  tx->add_to_read_set(RPC_SEC_SUBSCRIBER_REQ, sec_sub_key.hots_key,
                      &sec_sub_obj);
  tx_status_t ex_result = tx->do_read(yield);
  tatp_dassert(ex_result == tx_status_t::in_progress); /* Never locked */

  tatp_dassert(sec_sub_obj.val_size > 0); /* Must exist */

  auto *sec_sub_val = (tatp_sec_sub_val_t *)sec_sub_obj.val;
  _unused(sec_sub_val);
  tatp_dassert(sec_sub_val->magic == tatp_sec_sub_magic);
  tatp_dassert(sec_sub_val->s_id == s_id);

  // Read the Special Facility record
  hots_obj_t specfac_obj;
  tatp_specfac_key_t specfac_key;
  specfac_key.s_id = s_id;
  specfac_key.sf_type = sf_type;
  tx->add_to_read_set(RPC_SPECIAL_FACILITY_REQ, specfac_key.hots_key,
                      &specfac_obj);

  /* The Special Facility record exists only 62.5% of the time */
  ex_result = tx->do_read(yield);
  _unused(ex_result);
  if (ex_result != tx_status_t::in_progress || specfac_obj.val_size == 0) {
    tx->abort_rdonly();
    return false;
  }

  /* If we are here, the Special Facility record exists. */
  auto *specfac_val = (tatp_specfac_val_t *)&specfac_obj.val;
  _unused(specfac_val);
  tatp_dassert(specfac_val->data_b[0] == tatp_specfac_data_b0_magic);

  // Lock the Call Forwarding record
  hots_obj_t callfwd_obj;
  tatp_callfwd_key_t callfwd_key;
  callfwd_key.s_id = s_id;
  callfwd_key.sf_type = sf_type;
  callfwd_key.start_time = start_time;
  tx->add_to_write_set(RPC_CALL_FORWARDING_REQ, callfwd_key.hots_key,
                       &callfwd_obj, tx_write_mode_t::insert);

  ex_result = tx->do_read(yield);
  tatp_dassert(ex_result == tx_status_t::in_progress ||
               ex_result ==
                   tx_status_t::must_abort); /* If callfwd_key existed */

  if (ex_result == tx_status_t::in_progress) {
    /*
     * callfwd_obj is not a valid Call Forwarding HoTS object yet. We must
     * construct the real object to insert here.
     */
    hots_format_real_obj(callfwd_obj, sizeof(tatp_callfwd_val_t));

    tatp_callfwd_val_t *callfwd_val = (tatp_callfwd_val_t *)&callfwd_obj.val;
    callfwd_val->numberx[0] = tatp_callfwd_numberx0_magic;
    callfwd_val->end_time = end_time;

    // tatp_stat_inc(stat_tx_commit_attempted[txn_type], 1);
    tx_status_t commit_status = tx->commit(yield);
    return (commit_status == tx_status_t::committed);
  } else {
    /*
     * This happens when the lock_for_ins on the Call Forwarding record
     * fails. We cannot use abort_rdonly() here, but no RPC requests
     * will be sent anyway.
     */
    tx->abort(yield);
    return false;
  }
}

// Profile:
// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Delete a CALL_FORWARDING row
// Maximum messages in a batch = 2 (during commit-backup)
bool txn_delete_call_forwarding(coro_yield_t &yield, int coro_id, Tx *tx) {
  int txn_type = static_cast<int>(tatp_txn_type_t::delete_call_forwarding);
  tx->start();

  /* Transaction parameters */
  uint32_t s_id = tatp->get_nurand_subscriber(&tg_seed);
  uint8_t sf_type = (hrd_fastrand(&tg_seed) % 4) + 1;
  uint8_t start_time = (hrd_fastrand(&tg_seed) % 3) * 8;

  // Read the secondary subscriber record
  hots_obj_t sec_sub_obj;
  tatp_sec_sub_key_t sec_sub_key;
  sec_sub_key.sub_nbr = tatp->tatp_sub_nbr_from_sid_fast(s_id);

  tx->add_to_read_set(RPC_SEC_SUBSCRIBER_REQ, sec_sub_key.hots_key,
                      &sec_sub_obj);
  tx_status_t ex_result = tx->do_read(yield);
  if (ex_result != tx_status_t::in_progress) {
    tx->abort_rdonly();
    return false;
  }

  tatp_dassert(sec_sub_obj.val_size > 0); /* Must exist */

  tatp_sec_sub_val_t *sec_sub_val = (tatp_sec_sub_val_t *)sec_sub_obj.val;
  _unused(sec_sub_val);
  tatp_dassert(sec_sub_val->magic == tatp_sec_sub_magic);
  tatp_dassert(sec_sub_val->s_id == s_id);

  // Delete the Call Forwarding record if it exists
  hots_obj_t callfwd_obj;
  tatp_callfwd_key_t callfwd_key;
  callfwd_key.s_id = s_id;
  callfwd_key.sf_type = sf_type;
  callfwd_key.start_time = start_time;
  tx->add_to_write_set(RPC_CALL_FORWARDING_REQ, callfwd_key.hots_key,
                       &callfwd_obj, tx_write_mode_t::del);

  ex_result = tx->do_read(yield);
  tatp_dassert(ex_result == tx_status_t::in_progress ||
               ex_result ==
                   tx_status_t::must_abort); /* If callfwd_key didn't exist */

  if (ex_result == tx_status_t::in_progress) {
    /*
     * Delete-mode write set records are handled using get_for_upd, so
     * we have a valid Call Forwarding record here.
     */
    auto *callfwd_val = (tatp_callfwd_val_t *)&callfwd_obj.val;
    _unused(callfwd_val);
    tatp_dassert(callfwd_val->numberx[0] == tatp_callfwd_numberx0_magic);

    // tatp_stat_inc(stat_tx_commit_attempted[txn_type], 1);
    tx_status_t commit_status = tx->commit(yield);
    return (commit_status == tx_status_t::committed);
  } else {
    /*
     * This happens when the get_for_upd on the Call Forwarding record
     * fails. We cannot use abort_rdonly() here, but no RPC requests
     * will be sent anyway.
     */
    tx->abort(yield);
    return false;
  }
}

void client_coroutine_function(coro_id_t coro_id,
                               std::vector<ResponseRegion> &resp_regions,
#if MEASURE_LATENCY == 1
                               ThreadArgs *args,
#endif
                               coro_yield_t &yield) {
  ResponseRegion &resp_region = resp_regions[coro_id];
  Tx *tx = new Tx(coro_id, &resp_region, mappings, logger, GetThreadID());

#if MEASURE_LATENCY == 1
  double start_time = 0.0;
  const int sample_interval = 5000000 / num_clients;
  size_t cnt = 0;
  std::vector<double> &latency_samples = *(args->latency_samples);
#endif

  while (!stop_signal) {
#if MEASURE_LATENCY == 1
    ++cnt;
    if (cnt == sample_interval) {
      start_time = get_current_time_in_nanoseconds();
    }
#endif

    tatp_txn_type_t txn_type = workgen_arr[hrd_fastrand(&tg_seed) % 100];
    stat_tx_attempted_tot++;
    tatp_stat_inc(stat_tx_attempted[static_cast<int>(txn_type)], 1);

    bool tx_committed = false; /* Did the txn commit? */
    switch (txn_type) {
      case tatp_txn_type_t::get_subsciber_data:
        tx_committed = txn_get_subscriber_data(yield, coro_id, tx);
        break;
      case tatp_txn_type_t::get_new_destination:
        tx_committed = txn_get_new_destination(yield, coro_id, tx);
        break;
      case tatp_txn_type_t::get_access_data:
        tx_committed = txn_get_access_data(yield, coro_id, tx);
        break;
      case tatp_txn_type_t::update_subscriber_data:
        tx_committed = txn_update_subscriber_data(yield, coro_id, tx);
        break;
      case tatp_txn_type_t::update_location:
        tx_committed = txn_update_location(yield, coro_id, tx);
        break;
      case tatp_txn_type_t::insert_call_forwarding:
        tx_committed = txn_insert_call_forwarding(yield, coro_id, tx);
        break;
      case tatp_txn_type_t::delete_call_forwarding:
        tx_committed = txn_delete_call_forwarding(yield, coro_id, tx);
        break;
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(txn_type));
        exit(-1);
    }

    if (tx_committed) {
      stat_tx_committed_tot++;
      tatp_stat_inc(stat_tx_committed[static_cast<int>(txn_type)], 1);
    }

#if MEASURE_LATENCY == 1
    if (cnt == sample_interval) {
      double end_time = get_current_time_in_nanoseconds();
      double latency = end_time - start_time;
      latency_samples.push_back(latency);
      cnt = 0;
    }
#endif
  }
}

void *client_thread_func(void *arg) {
  int thread_id = GetThreadID();
  ThreadArgs *args = (ThreadArgs *)arg;
#if MEASURE_LATENCY == 1
  std::vector<double> &latency_samples = *(args->latency_samples);
#endif
  auto ctx = rpc_client;
  BindCoreNuma(thread_id, numa_node);

  mappings = new Mappings(-1, 1, num_threads, 0, false);
  workgen_arr = tatp->create_workgen_array();
  tg_seed = 0xdeadbeef + thread_id;

  std::vector<ResponseRegion> resp_regions;
  int num_clients_this_thread = 0;
  for (int i = 0; i < num_clients; ++i) {
    if (thread_id == ctx->get_peer_node_id(i) % num_threads) {
      resp_regions.emplace_back(*ctx, i, thread_id, clt_cmsg_depth);
      ++num_clients_this_thread;
    }
  }

  // Create a vector to store coroutines
  std::vector<coro_call_t *> coroutines;

  // Initialize coroutines for each client
  for (int i = 0; i < num_clients_this_thread; ++i) {
    coro_call_t *coroutine = new coro_call_t(
        boost::bind(&client_coroutine_function,
                    i,  // coro_id_t id
                    boost::ref(resp_regions),
#if MEASURE_LATENCY == 1
                    args,
#endif
                    _1));  // _1 is the placeholder for coro_yield_t&
    coroutines.push_back(coroutine);
  }

  pthread_barrier_wait(&barrier);
  // Main loop to resume coroutines
  while (!stop_signal) {
    for (auto &coroutine : coroutines) {
      if (*coroutine) {
        (*coroutine)();
      }
    }
  }
  // Clean up coroutines
  for (auto &coroutine : coroutines) {
    delete coroutine;
  }
  pthread_barrier_wait(&barrier);
  total_tx_attempted.fetch_add(stat_tx_attempted_tot);
  total_tx_committed.fetch_add(stat_tx_committed_tot);
  return NULL;
}

#if MEASURE_LATENCY == 1
// Function to compute and report latency statistics
void compute_and_report_latency_statistics(const std::vector<double> &samples) {
  if (samples.empty()) {
    std::cout << "No latency samples collected." << std::endl;
    return;
  }

  std::vector<double> sorted_samples = samples;
  std::sort(sorted_samples.begin(), sorted_samples.end());

  double sum = 0.0;
  for (double s : samples) {
    sum += s;
  }
  double average = sum / samples.size();

  double median = 0.0;
  if (sorted_samples.size() % 2 == 0) {
    median = (sorted_samples[sorted_samples.size() / 2 - 1] +
              sorted_samples[sorted_samples.size() / 2]) /
             2;
  } else {
    median = sorted_samples[sorted_samples.size() / 2];
  }

  size_t idx_99 = static_cast<size_t>(0.99 * sorted_samples.size());
  if (idx_99 >= sorted_samples.size()) {
    idx_99 = sorted_samples.size() - 1;
  }
  double p99_latency = sorted_samples[idx_99];

  std::cout << "Samples collected: " << samples.size() << std::endl;
  std::cout << "Latency statistics (nanoseconds):" << std::endl;
  std::cout << "Average latency: " << average << " ns" << std::endl;
  std::cout << "Median latency: " << median << " ns" << std::endl;
  std::cout << "99th percentile latency: " << p99_latency << " ns" << std::endl;
}
#endif

void run_client(uint16_t port) {
  pthread_t tid[kMaxThreads];
  double elapsed_time;
  rpc_client = new RpcNode();
  int rc = rpc_client->start(port);
  assert(!rc);
  while (rpc_client->get_connection_count() != num_clients) {
    usleep(100000);  // sleep for 0.1 seconds
  }
  rpc_client->stop();

  ThreadArgs thread_args[kMaxThreads];
#if MEASURE_LATENCY == 1
  std::vector<double> latency_samples_array[kMaxThreads];
#endif

  tatp = new TATP(1, num_threads, 1);

  pthread_barrier_init(&barrier, NULL, num_threads + 1);
  for (long i = 0; i < num_threads; ++i) {
#if MEASURE_LATENCY == 1
    thread_args[i].latency_samples = &latency_samples_array[i];
#endif
    pthread_create(&tid[i], NULL, client_thread_func, (void *)&thread_args[i]);
  }
  pthread_barrier_wait(&barrier);
  SDS_INFO("client machine starts running");
  sleep(running_time * 2);
  stop_signal = 1;
  pthread_barrier_wait(&barrier);
  for (int i = 0; i < num_threads; ++i) {
    pthread_join(tid[i], NULL);
  }
  pthread_barrier_destroy(&barrier);
  client_report(running_time);

#if MEASURE_LATENCY == 1
  // Collect latency samples
  std::vector<double> all_latency_samples;
  for (int i = 0; i < num_threads; ++i) {
    all_latency_samples.insert(all_latency_samples.end(),
                               latency_samples_array[i].begin(),
                               latency_samples_array[i].end());
  }

  // Compute and report latency statistics
  compute_and_report_latency_statistics(all_latency_samples);
#endif
}

void *worker_thread_func(void *arg) {
  int wrk_id = GetThreadID();
  auto ctx = rpc_server;
  uint64_t attempts = 0;
  BindCoreNuma(wrk_id, numa_node);
  std::vector<RequestRegion> req_regions;

  mappings = new Mappings(wrk_id, 1, num_threads, 0, false);
  tatp->populate_all_tables_barrier(mappings);

#if MEMORY_MULTIPLEXING == 1
  req_regions.emplace_back(*ctx, wrk_id, 0);
  tatp->register_rpc_handlers(&req_regions[0]);
#else
  for (int i = 0; i < num_clients; ++i) {
    req_regions.emplace_back(*ctx, wrk_id, i);
    tatp->register_rpc_handlers(&req_regions[i]);
  }
#endif

  int clt_i = 0;
  int token = worker_depth;
  size_t cnt = 0;
  pthread_barrier_wait(&barrier);

#if CONNECTION_GROUPING == 1
  while (!stop_signal) {
    for (int group_start = 0; group_start < num_clients;
         group_start += CLIENTS_PER_GROUP) {
      int group_end = std::min(group_start + CLIENTS_PER_GROUP, num_clients);
      auto group_start_time = std::chrono::high_resolution_clock::now();
      uint64_t loop = 0;

      while (true) {
        for (int clt_i = group_start; clt_i < group_end; ++clt_i) {
          int num_cmsgs = req_regions[clt_i].poll_req_cmsgs();
          if (num_cmsgs == 0) {
            continue;
          }
          int num_inner_reqs = req_regions[clt_i].handle_req_cmsgs(num_cmsgs);
          if (token - num_cmsgs < 0) {
            assert(ctx->sync() == 0);
            token = worker_depth;
          }
          req_regions[clt_i].post_resp_cmsgs(num_cmsgs);
          attempts += num_cmsgs;
          token -= num_cmsgs;
        }
        ++loop;
        if (loop >= TIME_CHECK_INTERVAL) {
          loop = 0;
          auto now_time = std::chrono::high_resolution_clock::now();
          int64_t period =
              std::chrono::duration_cast<std::chrono::microseconds>(
                  now_time - group_start_time)
                  .count();
          if (period >= TIME_PER_GROUP) {
            break;
          }
        }
      }
    }
  }
#else
  while (!stop_signal) {
#if MEMORY_MULTIPLEXING == 1
    {
#else
    for (clt_i = 0; clt_i < num_clients; ++clt_i) {
#endif
      int num_cmsgs = req_regions[clt_i].poll_req_cmsgs();
      if (num_cmsgs == 0) {
        continue;
      }
      int num_inner_reqs = req_regions[clt_i].handle_req_cmsgs(num_cmsgs);
      if (token - num_cmsgs < 0) {
        assert(ctx->sync() == 0);
        token = worker_depth;
      }
      req_regions[clt_i].post_resp_cmsgs(num_cmsgs);
#if MEMORY_MULTIPLEXING == 1
      req_regions[clt_i].checkout_new_window();
#endif
      attempts += num_cmsgs;
      token -= num_cmsgs;
    }
  }
#endif

  pthread_barrier_wait(&barrier);
  total_cmsg_attempts.fetch_add(attempts);
  return NULL;
}

void run_server(const std::vector<std::string> &client_machine_list,
                uint16_t port) {
  struct timeval start_tv, end_tv;
  pthread_t tid[kMaxThreads];
  double elapsed_time;
  int qp_count = num_threads;

  gettimeofday(&start_tv, NULL);
  rpc_server = new RpcNode();
  for (int client_id = 0; client_id < num_clients; ++client_id) {
    int machine_id = client_id / (num_clients / client_machine_list.size());
    if (machine_id == client_machine_list.size()) {
      machine_id = client_machine_list.size() - 1;
    }
    int rc = rpc_server->connect(
        client_id, client_machine_list[machine_id].c_str(), port, qp_count);
    assert(!rc);
  }
  gettimeofday(&end_tv, NULL);
  connect_time = (end_tv.tv_sec - start_tv.tv_sec) * 1000.0 +
                 (end_tv.tv_usec - start_tv.tv_usec) / 1000.0;

  tatp = new TATP(1, num_threads, 1);
  tatp->init_all_tables();

  pthread_barrier_init(&barrier, NULL, num_threads + 1);
  for (long i = 0; i < num_threads; ++i) {
    pthread_create(&tid[i], NULL, worker_thread_func, (void *)nullptr);
  }
  pthread_barrier_wait(&barrier);
  SDS_INFO("server machine starts running");
  gettimeofday(&start_tv, NULL);
  sleep(running_time);
  stop_signal = 1;
  pthread_barrier_wait(&barrier);
  gettimeofday(&end_tv, NULL);
  for (int i = 0; i < num_threads; ++i) {
    pthread_join(tid[i], NULL);
  }
  pthread_barrier_destroy(&barrier);
  elapsed_time = (end_tv.tv_sec - start_tv.tv_sec) * 1.0 +
                 (end_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
  server_report(elapsed_time);

  for (int client_id = 0; client_id < num_clients; ++client_id) {
    rpc_server->disconnect(client_id);
  }
  delete rpc_server;
}

int main(int argc, char **argv) {
  const char *env_path = getenv("TATP_CONF");
  JsonConfig config = JsonConfig::load_file(
      env_path ? env_path : ROOT_DIR "/config/tatp.json");
  int port = (int)config.get("port").get_int64();
  dump_file_path = config.get("dump_file_path").get_str();
  if (getenv("DUMP_FILE_PATH")) {
    dump_file_path = getenv("DUMP_FILE_PATH");
  }
  if (getenv("DUMP_PREFIX")) {
    dump_prefix = std::string(getenv("DUMP_PREFIX"));
  } else {
    dump_prefix = "tatp";
  }
  numa_node = (int)config.get("numa_node").get_int64();
  if (argc == 3) {
    std::vector<std::string> client_machine_list;
    JsonConfig clients = config.get("clients");
    for (int i = 0; i < clients.size(); ++i) {
      client_machine_list.push_back(clients.get(i).get_str());
    }
    assert(!client_machine_list.empty());
    // @num_threads workers
    num_threads = atoi(argv[1]);
    // @num_clients clients to connect
    num_clients = atoi(argv[2]);
#if MEMORY_MULTIPLEXING == 0
    assert(num_clients == NUM_CLIENTS);
#endif
    run_server(client_machine_list, port);
  } else if (argc == 4) {
    // @num_threads threads to emulate clients
    num_threads = atoi(argv[1]);
    // @num_clients clients to be emulated
    num_clients = atoi(argv[2]);
#if MEMORY_MULTIPLEXING == 0
    assert(num_clients == NUM_CLIENTS / NUM_MACHINES);
#endif
    // @clt_cmsg_depth RPC requests to be posted by each client
    clt_cmsg_depth = atoi(argv[3]);
    run_client(port);
  } else {
    SDS_WARN("Usage: %s <num_threads> <num_clients> [clt_cmsg_depth]", argv[0]);
  }
  return 0;
}