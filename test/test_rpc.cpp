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
#include <cassert>
#include <chrono>
#include <iostream>
#include <random>
#include <vector>

#include "rdmax/mapping.h"
#include "rdmax/rpc.h"
#include "rdmax/thread.h"

using namespace rdmax;

#define MEASURE_LATENCY 1

int numa_node;
static const size_t MEM_POOL_SIZE = (1ull << 30);
static int running_time = 15;
int num_threads = 1;
int num_clients = 1;
int stop_signal = 0;
double connect_time = 0.0;
std::string dump_file_path;
std::string dump_prefix;
std::atomic<uint64_t> total_attempts(0);
pthread_barrier_t barrier;
RpcNode *rpc_server;
RpcNode *rpc_client;
static const int dummy_rpc_req_type = 0;
static const int dummy_rpc_req_size = 32;
static const int dummy_rpc_resp_size = 32;

static const int worker_depth = 32;
static int clt_cmsg_depth = 1;
static int clt_inner_req_depth = 1;

size_t dummy_rpc_handler(uint8_t *resp_buf, rpc_resptype_t *resp_type,
                         const uint8_t *req_buf, size_t req_len, void *arg) {
  assert(resp_buf != NULL && req_buf != NULL && req_len == dummy_rpc_req_size);
  (void)(arg);

  return dummy_rpc_resp_size;
}

void add_dummy_rpc_req(rpc_cmsg_t *req_cmsg, int num_reqs) {
  for (int req_i = 0; req_i < num_reqs; ++req_i) {
    rpc_inner_msg_t *req =
        (rpc_inner_msg_t *)((uint8_t *)req_cmsg + req_cmsg->cmsg_len);
    req->req_type = dummy_rpc_req_type;
    req->data_len = dummy_rpc_req_size;
    req_cmsg->cmsg_len += sizeof(rpc_inner_msg_t) + req->data_len;
    ++req_cmsg->num_inner_msgs;
  }
}

void report(uint64_t elapsed_time) {
  auto throughput = total_attempts / elapsed_time / 1000000.0;
  SDS_INFO(
      "%s: #threads = %d, #clients = %d, IOPS = %.3lf M/s (coalesced message), "
      "IOPS = %.3lf M/s (inner request)",
      dump_prefix.c_str(), num_threads, num_clients, throughput,
      throughput * clt_inner_req_depth);
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

void *client_thread_func(void *arg) {
  int thread_id = GetThreadID();
  ThreadArgs *args = (ThreadArgs *)arg;  
#if MEASURE_LATENCY == 1
  std::vector<double> &latency_samples = *(args->latency_samples);
#endif
  auto ctx = rpc_client;
  BindCoreNuma(thread_id, numa_node);

  std::vector<ResponseRegion> resp_regions;
  int num_clients_this_thread = 0;
  for (int i = 0; i < num_clients; ++i) {
    if (thread_id == ctx->get_peer_node_id(i) % num_threads) {
      resp_regions.emplace_back(*ctx, i, thread_id, clt_cmsg_depth);
      ++num_clients_this_thread;
    }
  }

#if MEASURE_LATENCY == 1
  double start_time = 0.0;
  const int sample_interval = 500000 / num_clients;
  size_t cnt = 0;
#endif

  pthread_barrier_wait(&barrier);
  for (int i = 0; i < num_clients_this_thread; ++i) {
    for (int j = 0; j < clt_cmsg_depth; ++j) {
      rpc_cmsg_t *req_cmsg = resp_regions[i].start_req_cmsg(
          dummy_rpc_req_type,
          get_worker_id(resp_regions[i].global_client_id(), num_threads));
      add_dummy_rpc_req(req_cmsg, clt_inner_req_depth);
    }
    int num_reqs_posted = resp_regions[i].post_req_cmsgs();
  }

  while (!stop_signal) {
    for (int i = 0; i < num_clients_this_thread; ++i) {
      if (!resp_regions[i].check_resp_cmsg(clt_cmsg_depth - 1)) {
        continue;
      }
#if MEASURE_LATENCY == 1
      if (i == 0 && cnt == sample_interval) {
        double end_time = get_current_time_in_nanoseconds();
        double latency = end_time - start_time;
        latency_samples.push_back(latency);
        cnt = 0;
      }
#endif
      for (int j = 0; j < clt_cmsg_depth; ++j) {
        rpc_cmsg_t *req_cmsg = resp_regions[i].start_req_cmsg(
            dummy_rpc_req_type,
            get_worker_id(resp_regions[i].global_client_id(), num_threads));
        add_dummy_rpc_req(req_cmsg, clt_inner_req_depth);
      }
      int num_reqs_posted = resp_regions[i].post_req_cmsgs();

#if MEASURE_LATENCY == 1
      if (i == 0) {
        ++cnt;
        if (cnt == sample_interval) {
          start_time = get_current_time_in_nanoseconds();
        }
      }
#endif
    }
  }
  pthread_barrier_wait(&barrier);
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
  SDS_INFO("client machine get ready");

  ThreadArgs thread_args[kMaxThreads];
#if MEASURE_LATENCY == 1
  std::vector<double> latency_samples_array[kMaxThreads];
#endif

  pthread_barrier_init(&barrier, NULL, num_threads + 1);
  for (long i = 0; i < num_threads; ++i) {
#if MEASURE_LATENCY == 1
    thread_args[i].latency_samples = &latency_samples_array[i];
#endif
    pthread_create(&tid[i], NULL, client_thread_func, (void *)&thread_args[i]);
  }
  pthread_barrier_wait(&barrier);
  sleep(running_time * 2);
  stop_signal = 1;
  pthread_barrier_wait(&barrier);
  for (int i = 0; i < num_threads; ++i) {
    pthread_join(tid[i], NULL);
  }
  pthread_barrier_destroy(&barrier);

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

#ifdef MEMORY_MULTIPLEXING
  req_regions.emplace_back(*ctx, wrk_id, 0);
  req_regions[0].register_rpc_handler(dummy_rpc_req_type, dummy_rpc_handler,
                                      nullptr);
#else
  for (int i = 0; i < num_clients; ++i) {
    req_regions.emplace_back(*ctx, wrk_id, i);
    req_regions[i].register_rpc_handler(dummy_rpc_req_type, dummy_rpc_handler,
                                        nullptr);
  }
#endif

  int clt_i = 0;
  int token = worker_depth;
  size_t cnt = 0;
  pthread_barrier_wait(&barrier);
  while (!stop_signal) {
#ifdef MEMORY_MULTIPLEXING
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
#ifdef MEMORY_MULTIPLEXING
      req_regions[clt_i].checkout_new_window();
#endif
      attempts += num_cmsgs;
      token -= num_cmsgs;
    }
  }
  pthread_barrier_wait(&barrier);
  total_attempts.fetch_add(attempts);
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

  pthread_barrier_init(&barrier, NULL, num_threads + 1);
  for (long i = 0; i < num_threads; ++i) {
    pthread_create(&tid[i], NULL, worker_thread_func, (void *)nullptr);
  }
  pthread_barrier_wait(&barrier);
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
  report(elapsed_time);

  for (int client_id = 0; client_id < num_clients; ++client_id) {
    rpc_server->disconnect(client_id);
  }
  delete rpc_server;
}

int main(int argc, char **argv) {
  const char *env_path = getenv("TEST_RPC_CONF");
  JsonConfig config = JsonConfig::load_file(
      env_path ? env_path : ROOT_DIR "/config/test_rpc.json");
  int port = (int)config.get("port").get_int64();
  dump_file_path = config.get("dump_file_path").get_str();
  if (getenv("DUMP_FILE_PATH")) {
    dump_file_path = getenv("DUMP_FILE_PATH");
  }
  if (getenv("DUMP_PREFIX")) {
    dump_prefix = std::string(getenv("DUMP_PREFIX"));
  } else {
    dump_prefix = "test_rpc";
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
    run_server(client_machine_list, port);
  } else if (argc == 4) {
    // @num_threads threads to emulate clients
    num_threads = atoi(argv[1]);
    // @num_clients clients to be emulated
    num_clients = atoi(argv[2]);
    // @clt_cmsg_depth RPC requests to be posted by each client
    clt_cmsg_depth = atoi(argv[3]);
    run_client(port);
  } else {
    SDS_WARN("Usage: %s <num_threads> <num_clients> [clt_cmsg_depth]", argv[0]);
  }
  return 0;
}