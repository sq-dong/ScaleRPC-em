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
#include "sb.h"
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
SB *sb;
__thread Mappings *mappings;
__thread Logger *logger;
__thread sb_txn_type_t *workgen_arr;
__thread uint64_t tg_seed = 0xdeadbeef;       /* Thread-global random seed */
__thread long long stat_tx_attempted_tot = 0; /* Collected in non-debug */
__thread long long stat_tx_committed_tot = 0; /* Collected in non-debug */
__thread long long stat_tx_attempted[SB_TXN_TYPES];
__thread long long stat_tx_committed[SB_TXN_TYPES];

static const int worker_depth = 32;
static int clt_cmsg_depth = 1;

void client_report(uint64_t elapsed_time) {
  auto attempted_tput = total_tx_attempted.load() / elapsed_time / 1000000.0;
  auto committed_tput = total_tx_committed.load() / elapsed_time / 1000000.0;
  SDS_INFO(
      "%s: #threads = %d, #clients = %d, #machines = %d, #replicas = %d, "
      "elapsed_time = %ld, total_tx_attempted = %ld, total_tx_committed = %ld, "
      "attempted_tput = %.2f M/s, committed_tput = %.2f M/s",
      dump_prefix.c_str(), num_threads, num_clients, sb->num_machines,
      sb->num_replicas, elapsed_time, total_tx_attempted.load(),
      total_tx_committed.load(), attempted_tput, committed_tput);
}

void server_report(uint64_t elapsed_time) {
  auto cmsg_tput = total_cmsg_attempts.load() / elapsed_time / 1000000.0;
  SDS_INFO(
      "%s: #threads = %d, #clients = %d, #machines = %d, #replicas = %d, "
      "elapsed_time = %ld, total_cmsg_attempted = %ld, cmsg_tput = %.2f M/s",
      dump_prefix.c_str(), num_threads, num_clients, sb->num_machines,
      sb->num_replicas, elapsed_time, total_cmsg_attempts.load(), cmsg_tput);
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

void txn_amalgamate(coro_yield_t &yield, int coro_id, Tx *tx) {
  sb_txn_type_t txn_type = sb_txn_type_t::amalgamate;
  tx->start();
  sb_stat_inc(stat_tx_attempted[static_cast<int>(txn_type)], 1);
  stat_tx_attempted_tot++;

  /* Transaction parameters */
  uint64_t acct_id_0, acct_id_1;
  sb->get_two_accounts(&tg_seed, &acct_id_0, &acct_id_1);

  /* Read from savings and checking tables for acct_id_0 */
  hots_obj_t sav_obj_0;
  sb_sav_key_t sav_key_0;
  sav_key_0.acct_id = acct_id_0;
  tx->add_to_write_set(RPC_SAVING_REQ, sav_key_0.hots_key, &sav_obj_0,
                       tx_write_mode_t::update);

  hots_obj_t chk_obj_0;
  sb_chk_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  tx->add_to_write_set(RPC_CHECKING_REQ, chk_key_0.hots_key, &chk_obj_0,
                       tx_write_mode_t::update);

  /* Read from checking account for acct_id_1 */
  hots_obj_t chk_obj_1;
  sb_chk_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  tx->add_to_write_set(RPC_CHECKING_REQ, chk_key_1.hots_key, &chk_obj_1,
                       tx_write_mode_t::update);

  tx_status_t ex_result = tx->do_read(yield);
  _unused(ex_result);

  sb_dassert(ex_result == tx_status_t::in_progress ||
             ex_result == tx_status_t::must_abort); /* Found or locked */

  if (ex_result == tx_status_t::must_abort) {
    tx->abort(yield);
    return;
  }

  /* If we are here, execution succeeded and we have locks */
  sb_sav_val_t *sav_val_0 = (sb_sav_val_t *)sav_obj_0.val;
  sb_chk_val_t *chk_val_0 = (sb_chk_val_t *)chk_obj_0.val;
  sb_chk_val_t *chk_val_1 = (sb_chk_val_t *)chk_obj_1.val;
  sb_dassert(sav_val_0->magic == sb_sav_magic);
  sb_dassert(chk_val_0->magic == sb_chk_magic);
  sb_dassert(chk_val_1->magic == sb_chk_magic);

  /* Increase acct_id_1's balance and set acct_id_0's balances to 0 */
  chk_val_1->bal += (sav_val_0->bal + chk_val_0->bal);

  sav_val_0->bal = 0;
  chk_val_0->bal = 0;

  tx_status_t commit_status = tx->commit(yield);
  _unused(commit_status);

  /* if we managed to lock the records, the txn must commit */
  sb_dassert(commit_status == tx_status_t::committed);

  stat_tx_committed_tot++;
  sb_stat_inc(stat_tx_committed[static_cast<int>(txn_type)], 1);
}

/* Calculate the sum of saving and checking balance */
void txn_balance(coro_yield_t &yield, int coro_id, Tx *tx) {
  sb_txn_type_t txn_type = sb_txn_type_t::balance;
  tx->start();
  sb_stat_inc(stat_tx_attempted[static_cast<int>(txn_type)], 1);
  stat_tx_attempted_tot++;

  /* Transaction parameters */
  uint64_t acct_id;
  sb->get_account(&tg_seed, &acct_id);

  /* Read from savings and checking tables */
  hots_obj_t sav_obj;
  sb_sav_key_t sav_key;
  sav_key.acct_id = acct_id;
  tx->add_to_read_set(RPC_SAVING_REQ, sav_key.hots_key, &sav_obj);

  hots_obj_t chk_obj;
  sb_chk_key_t chk_key;
  chk_key.acct_id = acct_id;
  tx->add_to_read_set(RPC_CHECKING_REQ, chk_key.hots_key, &chk_obj);

  tx_status_t ex_result = tx->do_read(yield);
  _unused(ex_result);
  if (ex_result != tx_status_t::in_progress) {
    /* Abort if we read a locked  record */
    tx->abort_rdonly();
    return;
  }

  sb_sav_val_t *sav_val = (sb_sav_val_t *)sav_obj.val;
  _unused(sav_val);
  sb_chk_val_t *chk_val = (sb_chk_val_t *)chk_obj.val;
  _unused(chk_val);
  sb_dassert(sav_val->magic == sb_sav_magic);
  sb_dassert(chk_val->magic == sb_chk_magic);

  tx_status_t commit_result = tx->commit(yield);
  if (commit_result == tx_status_t::committed) {
    stat_tx_committed_tot++;
    sb_stat_inc(stat_tx_committed[static_cast<int>(txn_type)], 1);
  } else {
    tx->abort_rdonly();
  }
}

/* Add $1.3 to acct_id's checking account */
void txn_deposit_checking(coro_yield_t &yield, int coro_id, Tx *tx) {
  sb_txn_type_t txn_type = sb_txn_type_t::deposit_checking;
  tx->start();
  sb_stat_inc(stat_tx_attempted[static_cast<int>(txn_type)], 1);
  stat_tx_attempted_tot++;

  /* Transaction parameters */
  uint64_t acct_id;
  sb->get_account(&tg_seed, &acct_id);
  float amount = 1.3;

  /* Read from checking table */
  hots_obj_t chk_obj;
  sb_chk_key_t chk_key;
  chk_key.acct_id = acct_id;
  tx->add_to_write_set(RPC_CHECKING_REQ, chk_key.hots_key, &chk_obj,
                       tx_write_mode_t::update);

  tx_status_t ex_result = tx->do_read(yield);
  _unused(ex_result);
  sb_dassert(ex_result == tx_status_t::in_progress ||
             ex_result == tx_status_t::must_abort); /* Found or locked */

  if (ex_result == tx_status_t::must_abort) {
    tx->abort(yield);
    return;
  }

  /* If we are here, execution succeeded and we have a lock*/
  sb_chk_val_t *chk_val = (sb_chk_val_t *)chk_obj.val;
  sb_dassert(chk_val->magic == sb_chk_magic);

  chk_val->bal += amount; /* Update checking balance */

  tx_status_t commit_status = tx->commit(yield);
  _unused(commit_status);

  /* If we managed to lock the record, the txn must commit */
  sb_dassert(commit_status == tx_status_t::committed);

  stat_tx_committed_tot++;
  sb_stat_inc(stat_tx_committed[static_cast<int>(txn_type)], 1);
}

/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
void txn_send_payment(coro_yield_t &yield, int coro_id, Tx *tx) {
  sb_txn_type_t txn_type = sb_txn_type_t::send_payment;
  tx->start();
  sb_stat_inc(stat_tx_attempted[static_cast<int>(txn_type)], 1);
  stat_tx_attempted_tot++;

  /* Transaction parameters: send money from acct_id_0 to acct_id_1 */
  uint64_t acct_id_0, acct_id_1;
  sb->get_two_accounts(&tg_seed, &acct_id_0, &acct_id_1);
  float amount = 5.0;

  /* Read from checking table */
  hots_obj_t chk_obj_0, chk_obj_1;

  sb_chk_key_t chk_key_0, chk_key_1;
  chk_key_0.acct_id = acct_id_0;
  chk_key_1.acct_id = acct_id_1;

  tx->add_to_write_set(RPC_CHECKING_REQ, chk_key_0.hots_key, &chk_obj_0,
                       tx_write_mode_t::update);
  tx->add_to_write_set(RPC_CHECKING_REQ, chk_key_1.hots_key, &chk_obj_1,
                       tx_write_mode_t::update);

  tx_status_t ex_result = tx->do_read(yield);
  _unused(ex_result);
  sb_dassert(ex_result == tx_status_t::in_progress ||
             ex_result == tx_status_t::must_abort); /* Found or locked */

  if (ex_result == tx_status_t::must_abort) {
    tx->abort(yield);
    return;
  }

  /* if we are here, execution succeeded and we have locks */
  sb_chk_val_t *chk_val_0 = (sb_chk_val_t *)chk_obj_0.val;
  sb_chk_val_t *chk_val_1 = (sb_chk_val_t *)chk_obj_1.val;
  sb_dassert(chk_val_0->magic == sb_chk_magic);
  sb_dassert(chk_val_1->magic == sb_chk_magic);

  if (chk_val_0->bal < amount) {
    tx->abort(yield);
    return;
  }

  chk_val_0->bal -= amount; /* Debit */
  chk_val_1->bal += amount; /* Credit */

  tx_status_t commit_status = tx->commit(yield);
  _unused(commit_status);

  /* if we managed to lock the records, the txn must commit */
  sb_dassert(commit_status == tx_status_t::committed);

  stat_tx_committed_tot++;
  sb_stat_inc(stat_tx_committed[static_cast<int>(txn_type)], 1);
}

/* Add $20 to acct_id's saving's account */
void txn_transact_saving(coro_yield_t &yield, int coro_id, Tx *tx) {
  sb_txn_type_t txn_type = sb_txn_type_t::transact_saving;
  tx->start();
  sb_stat_inc(stat_tx_attempted[static_cast<int>(txn_type)], 1);
  stat_tx_attempted_tot++;

  /* Transaction parameters */
  uint64_t acct_id;
  sb->get_account(&tg_seed, &acct_id);
  float amount = 20.20;

  /* Read from saving table */
  hots_obj_t sav_obj;
  sb_sav_key_t sav_key;
  sav_key.acct_id = acct_id;
  tx->add_to_write_set(RPC_SAVING_REQ, sav_key.hots_key, &sav_obj,
                       tx_write_mode_t::update);

  tx_status_t ex_result = tx->do_read(yield);
  _unused(ex_result);
  sb_dassert(ex_result == tx_status_t::in_progress ||
             ex_result == tx_status_t::must_abort); /* Found or locked */

  if (ex_result == tx_status_t::must_abort) {
    tx->abort(yield);
    return;
  }

  /* If we are here, execution succeeded and we have a lock */
  sb_sav_val_t *sav_val = (sb_sav_val_t *)sav_obj.val;
  sb_dassert(sav_val->magic == sb_sav_magic);

  sav_val->bal += amount; /* Update saving balance */

  tx_status_t commit_status = tx->commit(yield);
  _unused(commit_status);

  /* If we managed to lock the record, the txn must commit */
  sb_dassert(commit_status == tx_status_t::committed);

  stat_tx_committed_tot++;
  sb_stat_inc(stat_tx_committed[static_cast<int>(txn_type)], 1);
}

/* Read saving and checking balance + update checking balance unconditionally */
void txn_write_check(coro_yield_t &yield, int coro_id, Tx *tx) {
  sb_txn_type_t txn_type = sb_txn_type_t::write_check;
  tx->start();
  sb_stat_inc(stat_tx_attempted[static_cast<int>(txn_type)], 1);
  stat_tx_attempted_tot++;

  /* Transaction parameters */
  uint64_t acct_id;
  sb->get_account(&tg_seed, &acct_id);
  float amount = 5.0;

  /* Read from savings. Read checking record for update. */
  hots_obj_t sav_obj;
  sb_sav_key_t sav_key;
  sav_key.acct_id = acct_id;
  tx->add_to_read_set(RPC_SAVING_REQ, sav_key.hots_key, &sav_obj);

  hots_obj_t chk_obj;
  sb_chk_key_t chk_key;
  chk_key.acct_id = acct_id;
  tx->add_to_write_set(RPC_CHECKING_REQ, chk_key.hots_key, &chk_obj,
                       tx_write_mode_t::update);

  tx_status_t ex_result = tx->do_read(yield);
  _unused(ex_result);
  sb_dassert(ex_result == tx_status_t::in_progress ||
             ex_result == tx_status_t::must_abort); /* Found or locked */

  if (ex_result == tx_status_t::must_abort) {
    tx->abort(yield);
    return;
  }

  sb_sav_val_t *sav_val = (sb_sav_val_t *)sav_obj.val;
  sb_chk_val_t *chk_val = (sb_chk_val_t *)chk_obj.val;
  sb_dassert(sav_val->magic == sb_sav_magic);
  sb_dassert(chk_val->magic == sb_chk_magic);

  if (sav_val->bal + chk_val->bal < amount) {
    chk_val->bal -= (amount + 1);
  } else {
    chk_val->bal -= amount;
  }

  tx_status_t commit_result = tx->commit(yield);

  /* This transaction can fail during validation */
  if (commit_result == tx_status_t::committed) {
    stat_tx_committed_tot++;
    sb_stat_inc(stat_tx_committed[static_cast<int>(txn_type)], 1);
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

    sb_txn_type_t txn_type = workgen_arr[hrd_fastrand(&tg_seed) % 100];
    switch (txn_type) {
      case sb_txn_type_t::amalgamate:
        txn_amalgamate(yield, coro_id, tx);
        break;
      case sb_txn_type_t::balance:
        txn_balance(yield, coro_id, tx);
        break;
      case sb_txn_type_t::deposit_checking:
        txn_deposit_checking(yield, coro_id, tx);
        break;
      case sb_txn_type_t::send_payment:
        txn_send_payment(yield, coro_id, tx);
        break;
      case sb_txn_type_t::transact_saving:
        txn_transact_saving(yield, coro_id, tx);
        break;
      case sb_txn_type_t::write_check:
        txn_write_check(yield, coro_id, tx);
        break;
      default:
        printf("Invalid txn type\n");
        assert(false);
        exit(-1);
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
  workgen_arr = sb->create_workgen_array();
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

  sb = new SB(1, num_threads, 1);

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
  sb->populate_all_tables_barrier(mappings);

#if MEMORY_MULTIPLEXING == 1
  req_regions.emplace_back(*ctx, wrk_id, 0);
  sb->register_rpc_handlers(&req_regions[0]);
#else
  for (int i = 0; i < num_clients; ++i) {
    req_regions.emplace_back(*ctx, wrk_id, i);
    sb->register_rpc_handlers(&req_regions[i]);
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

  sb = new SB(1, num_threads, 1);
  sb->init_all_tables();

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
  const char *env_path = getenv("SMALLBANK_CONF");
  JsonConfig config = JsonConfig::load_file(
      env_path ? env_path : ROOT_DIR "/config/smallbank.json");
  int port = (int)config.get("port").get_int64();
  dump_file_path = config.get("dump_file_path").get_str();
  if (getenv("DUMP_FILE_PATH")) {
    dump_file_path = getenv("DUMP_FILE_PATH");
  }
  if (getenv("DUMP_PREFIX")) {
    dump_prefix = std::string(getenv("DUMP_PREFIX"));
  } else {
    dump_prefix = "smallbank";
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