// based on SMART and FaSST

#ifndef SDS_RPC_H
#define SDS_RPC_H

#include <infiniband/verbs.h>

#include <atomic>
#include <functional>
#include <thread>
#include <vector>

#include "common.h"
#include "global_address.h"
#include "resource_manager.h"
#include "task.h"
#include "thread.h"

namespace rdmax {

#define QP_MULTIPLEXING 0
// Host-only ScaleRPC-em: disable shared request region (needs in-network
// dispatch / Tofino). Use per-client slots instead.
#define MEMORY_MULTIPLEXING 0
#define MEASURE_LATENCY 1
#define CONNECTION_GROUPING 1

#define MAX_NUM_WORKERS 40
#define MSG_SLOT_SIZE 1024  // max size of a RPC request/response in bytes
#define WORKER_DEPTH 8
#define MAX_OUTSTANDING_CMSGS \
  8  // a client has @MAX_OUTSTANDING_CMSGS slots in a worker's request region
#define MAX_INNER_MSGS_COALESCED 8

#if MEMORY_MULTIPLEXING == 1
#define DEFUALT_WINDOW_SIZE 256  // slot
#define WORKER_MEMORY_SIZE (1 << 24)
#define OFFSET(worker_id) (worker_id * WORKER_MEMORY_SIZE)
#else
// Single client machine. Set to 2 for a two-machine client topology.
// NUM_CLIENTS is the address-space ceiling; runtime argv may be <= this value.
#define NUM_MACHINES 1
#define NUM_CLIENTS 960
#define OFFSET(wrk_id, clt_id, slot_idx)                          \
  ((((wrk_id) * NUM_CLIENTS + (clt_id)) * MAX_OUTSTANDING_CMSGS + \
    (slot_idx)) *                                                 \
   MSG_SLOT_SIZE)
#endif

// from rpc_types.h in FaSST
#define RPC_MIN_REQ_TYPE 0   /* Minimum type for requests */
#define RPC_MAX_REQ_TYPE 255 /* Maximum type for requests */
#define RPC_IS_VALID_TYPE(x) (x >= RPC_MIN_REQ_TYPE && x <= RPC_MAX_REQ_TYPE)
#define RPC_PRIMARY_DS_REQ_SPACING 10

// from rpc_defs.h in FaSST
typedef uint8_t rpc_reqtype_t;
typedef uint8_t rpc_resptype_t;

#define RPC_REQ_MAGIC 0x5A
#define RPC_RESP_MAGIC 0xA5
typedef uint8_t magic_tail_t;
static_assert(sizeof(magic_tail_t) == 1, "");

struct rpc_cmsg_t {
  uint16_t cmsg_len : 10;  // including all headers
  uint16_t num_inner_msgs : 6;
  uint16_t src_id;       // client id for requests, worker id for responses
  uint16_t window_size;  // used in Memory Multiplexing
  uint16_t resp_offset;  // where the response message should be put in a
                         // client's response region
};
static_assert(sizeof(rpc_cmsg_t) == 8, "");

struct rpc_inner_msg_t {
  union {
    rpc_reqtype_t req_type;
    rpc_resptype_t resp_type;
  };
  uint16_t data_len;  // excluding the header
};
static_assert(sizeof(rpc_inner_msg_t) == 4, "");

inline bool check_finished_req_cmsg(volatile rpc_cmsg_t *cmsg) {
  return *(magic_tail_t *)((uint8_t *)cmsg + cmsg->cmsg_len) == RPC_REQ_MAGIC;
}

inline void clear_magic_tail(volatile rpc_cmsg_t *cmsg) {
  *(magic_tail_t *)((uint8_t *)cmsg + cmsg->cmsg_len) = 0;
}

class RequestRegion {
 public:
  RequestRegion(class RpcNode &node, node_t wrk_id, node_t clt_id);
  void register_rpc_handler(int req_type,
                            size_t (*func)(uint8_t *resp_buf,
                                           rpc_resptype_t *resp_type,
                                           const uint8_t *req_buf,
                                           size_t req_len, void *arg),
                            void *arg);
  int poll_req_cmsgs();
  int handle_req_cmsgs(int num_cmsgs);
  int post_resp_cmsgs(int num_cmsgs);
#if MEMORY_MULTIPLEXING == 1
  void checkout_new_window();
#endif

 private:
  RpcNode &node_;
  uint8_t *addr_;
  uint32_t head_;
  uint32_t tail_;
  node_t wrk_id_;
  uint16_t window_size_;
  struct targets {
    node_t clt_id;
    uint64_t resp_offset;
  } targets_[WORKER_DEPTH];
#if MEMORY_MULTIPLEXING == 1
  uint32_t index_base_;
  uint16_t new_window_size_;
#endif

  // Handlers: An RPC handler writes its response data and the response type
  // to resp_buf and resp_type respectively, and returns the response length
  size_t (*rpc_handler[RPC_MAX_REQ_TYPE])(uint8_t *resp_buf,
                                          rpc_resptype_t *resp_type,
                                          const uint8_t *req_buf,
                                          size_t req_len, void *arg) = {NULL};
  void *rpc_handler_arg[RPC_MAX_REQ_TYPE] = {NULL};
};

class ResponseRegion {
 public:
  ResponseRegion(class RpcNode &node, node_t local_node_id, node_t qp_node_id,
                 int max_clt_cmsg_depth);
  rpc_cmsg_t *start_req_cmsg(node_t wrk_id);
  rpc_inner_msg_t *start_inner_msg(rpc_cmsg_t *cmsg, rpc_reqtype_t req_type,
                                   int size, uint8_t *resp_buf);
  int post_req_cmsgs();
  bool check_resp_cmsg(int resp_i);
  rpc_cmsg_t *get_resp_cmsg(int index);
  node_t global_client_id() { return global_clt_id_; };

 private:
  RpcNode &node_;
  uint8_t *addr_;
  uint64_t offset_;  // offset from the beginning of the response region
#if MEMORY_MULTIPLEXING == 0
  uint32_t head_[MAX_NUM_WORKERS];
#endif
  uint32_t num_cmsg_to_post_;
  node_t local_node_id_;
  node_t global_clt_id_;
  node_t target_wrks_[MAX_OUTSTANDING_CMSGS];
  uint8_t *user_resp_buf_[MAX_OUTSTANDING_CMSGS][MAX_INNER_MSGS_COALESCED];
  node_t qp_node_id_;
};

class RpcNode {
 public:
  RpcNode();

  RpcNode(void *cache_ptr);

  RpcNode(const RpcNode &) = delete;

  RpcNode &operator=(const RpcNode &) = delete;

  enum class Option { None, PostRequest, Sync };

  int connect(node_t mem_node_id, const char *domain_name, uint16_t tcp_port,
              int qp_size) {
    /**
     * Since the compute node in SMART and the RPC server in RDMAX
     *  are the ones using QP multiplexing, and the compute node initiates
     * the connection in SMART, the RPC server in RDMAX assumes the task of
     * initiating the connection in order to be compatible with it.
     */
    is_client_ = false;
    return manager_.connect(mem_node_id, domain_name, tcp_port, qp_size);
  }

  int disconnect(node_t mem_node_id) {
    return manager_.disconnect(mem_node_id);
  }

  int write(const void *local, const GlobalAddress &remote, size_t length,
            Option option = Option::None, int flags = 0) {
    int rc = add_request(IBV_WR_RDMA_WRITE, local, remote, length, 0, 0, flags);
    if (rc || option == Option::None) {
      return rc;
    } else if (option == Option::PostRequest) {
      return post_request();
    } else {
      return sync();
    }
  }

  int post_request();

  int sync();

  int poll_once(node_t mem_node_id, bool notify = false);

  const SmartConfig &config() const { return manager_.config(); }

  void disable_inline_write() { manager_.config().max_inline_data = 0; }

  int start(uint16_t tcp_port);

  int stop();

  int get_connection_count() { return manager_.max_node_id() + 1; }

  void *get_cache_addr() { return cache_; }

  int get_peer_node_id(node_t node_id) const {
    return manager_.get_peer_node_id(node_id);
  }

 private:
  int add_request(ibv_wr_opcode opcode, const void *local,
                  const GlobalAddress &remote, size_t length,
                  uint64_t compare_add, uint64_t swap, int flags);

  struct RequestBuffer {
    const static int kCapacity = 64;

    struct ibv_send_wr wr_list[kCapacity];
    struct ibv_sge sge_list[kCapacity];
    int size;
    int remote_node_id;

   public:
    RequestBuffer() : size(0), remote_node_id(-1) {}
  };

  struct QueuePairState {
    std::vector<uint64_t> post_req;
    std::vector<uint64_t> ack_req;
    uint64_t inflight_ack;

   public:
    QueuePairState() : inflight_ack(0) {}
  };

  struct ThreadLocal {
    QueuePairState qp_state;
    RequestBuffer req_buf[kMaxTasksPerThread];
    std::vector<uint64_t> post_req_snapshot[kMaxTasksPerThread];
  };

  ResourceManager manager_;
  void *cache_;
  size_t cache_size_;
  std::atomic<size_t> cache_offset_;
  alignas(128) ThreadLocal tl_data_[kMaxThreads];
  ResourceManager::Listener *listener_;
  bool is_client_;
};

}  // namespace rdmax

#endif  // SDS_RPC_H
