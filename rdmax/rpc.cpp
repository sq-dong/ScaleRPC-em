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

#include "rdmax/rpc.h"

#include <unistd.h>

namespace rdmax {

RpcNode::RpcNode() : cache_offset_(0) {
  auto &config = manager_.config();
  cache_size_ = config.initiator_cache_size;
  cache_ = mmap_huge_page(cache_size_);
  if (manager_.register_main_memory(cache_, cache_size_, MR_FULL_PERMISSION)) {
    exit(EXIT_FAILURE);
  }

  for (auto &tl : tl_data_) {
    tl.qp_state.post_req.resize(config.max_nodes, 0);
    tl.qp_state.ack_req.resize(config.max_nodes, 0);
  }
}

RpcNode::RpcNode(void *cache_pptr) : cache_offset_(0) {
  assert(cache_pptr);

  auto &config = manager_.config();
  cache_size_ = config.initiator_cache_size;
  cache_ = cache_pptr;
  if (manager_.register_main_memory(cache_, cache_size_, MR_FULL_PERMISSION)) {
    exit(EXIT_FAILURE);
  }

  for (auto &tl : tl_data_) {
    tl.qp_state.post_req.resize(config.max_nodes, 0);
    tl.qp_state.ack_req.resize(config.max_nodes, 0);
  }
}

int RpcNode::add_request(ibv_wr_opcode opcode, const void *local,
                         const GlobalAddress &remote, size_t length,
                         uint64_t compare_add, uint64_t swap, int flags) {
  auto thread_id = GetThreadID();
  auto &req_buf = tl_data_[thread_id].req_buf[GetTaskID()];
  assert(remote.node < manager_.config().max_nodes);
  assert((uint64_t)local >= (uint64_t)cache_ &&
         (uint64_t)local < (uint64_t)cache_ + cache_size_);
  uint32_t rkey;
  uintptr_t remote_va;
  if (manager_.get_remote_memory_key(remote.node, remote.mr_id, remote.offset,
                                     length, rkey, remote_va)) {
    return -1;
  }

  node_t _target_node = remote.node;
#if QP_MULTIPLEXING == 1
  if (!is_client_) {
    remote_va = (remote_va & 0xFFC0FFFFFFFFFFFF) |
                ((uintptr_t)((thread_id) & 0x3F) << 48);
    _target_node = 0;
  }
#endif

  if (req_buf.size && req_buf.remote_node_id != _target_node) {
#if MEMORY_MULTIPLEXING == 1
    if (!is_client_) {
      if (post_request()) {
        return -1;
      }
    } else {
      if (sync()) {
        return -1;
      }
    }
#else
    if (sync()) {
      return -1;
    }
#endif
    assert(req_buf.size == 0);
  }

  int idx = req_buf.size;
  if (req_buf.size + 1 == RequestBuffer::kCapacity) {
    if (sync()) {
      return -1;
    }
    assert(req_buf.size + 1 < RequestBuffer::kCapacity);
    idx = req_buf.size;
  }

  req_buf.size++;
  req_buf.remote_node_id = _target_node;

  auto &wr = req_buf.wr_list[idx];
  auto &sge = req_buf.sge_list[idx];
  sge.length = length;
  sge.lkey = manager_.get_local_memory_lkey(0);
  sge.addr = (uintptr_t)local;
  wr.wr_id = 0;
  wr.opcode = opcode;
  wr.num_sge = 1;
  wr.sg_list = &sge;
  wr.send_flags = flags;
  wr.next = nullptr;
  if (opcode == IBV_WR_ATOMIC_FETCH_AND_ADD ||
      opcode == IBV_WR_ATOMIC_CMP_AND_SWP) {
    wr.wr.atomic.rkey = rkey;
    wr.wr.atomic.remote_addr = remote_va + remote.offset;
    wr.wr.atomic.compare_add = compare_add;
    wr.wr.atomic.swap = swap;
  } else {
    wr.wr.rdma.rkey = rkey;
    wr.wr.rdma.remote_addr = remote_va + remote.offset;
  }
  if (opcode == IBV_WR_RDMA_WRITE &&
      length <= manager_.config().max_inline_data) {
    wr.send_flags |= IBV_SEND_INLINE;
  }
  if (idx > 0) {
    req_buf.wr_list[idx - 1].next = &wr;
  }
  return 0;
}

static inline uint64_t MAKE_WR_ID(node_t remote_node_id, int wr_size) {
  int tid = GetThreadID();
  int task_id = GetTaskID();
  return ((uint64_t)task_id << 56ull) + ((uint64_t)tid << 48ull) +
         ((uint64_t)remote_node_id << 32ull) + wr_size;
}

static inline node_t REMOTE_NODE_ID(uint64_t wr_id) {
  return (node_t)((wr_id >> 32ull) & UINT16_MAX);
}

static inline int THREAD_ID(uint64_t wr_id) {
  return (int)(wr_id >> 48ull) & UINT8_MAX;
}

static inline int TASK_ID(uint64_t wr_id) {
  return (int)(wr_id >> 56ull) & UINT8_MAX;
}

static inline int WR_SIZE(uint64_t wr_id) { return (int)(wr_id & UINT32_MAX); }

int RpcNode::post_request() {
  int thread_id = GetThreadID();
  auto &tl = tl_data_[thread_id];
  auto &req_buf = tl.req_buf[GetTaskID()];
  auto &state = tl.qp_state;
  if (!req_buf.size) {
    return 0;
  }
  int wr_size = req_buf.size;
  req_buf.wr_list[req_buf.size - 1].wr_id =
      MAKE_WR_ID(req_buf.remote_node_id, wr_size);
  req_buf.wr_list[req_buf.size - 1].send_flags |= IBV_SEND_SIGNALED;
  int qp_idx;
  int qp_size = manager_.get_qp_size(req_buf.remote_node_id);

#if QP_MULTIPLEXING == 1
  if (is_client_) {
    // index starts from 0; use the last which is a normal QP
    qp_idx = qp_size - 1;
  } else {
    qp_idx = thread_id % (qp_size - 1);
  }
#else
  qp_idx = thread_id % qp_size;
#endif

  if (manager_.post_send(req_buf.remote_node_id, qp_idx, req_buf.wr_list)) {
    return -1;
  }
  req_buf.size = 0;
  state.post_req[req_buf.remote_node_id] += wr_size;
  state.inflight_ack += wr_size;
  return 0;
}

int RpcNode::sync() {
  auto &tl = tl_data_[GetThreadID()];
  auto &state = tl.qp_state;
  if (post_request()) {
    return -1;
  }
  auto &post_req = tl.post_req_snapshot[GetTaskID()];
  post_req.resize(state.post_req.size());
  for (int id = 0; id < post_req.size(); ++id) {
    post_req[id] = state.post_req[id];
  }
  for (int id = 0; id < post_req.size(); ++id) {
    while (state.ack_req[id] < post_req[id]) {
      if (TaskPool::IsEnabled()) {
        WaitTask();
      } else {
        if (poll_once(id, false) < 0) {
          return -1;
        }
      }
    }
  }
  return 0;
}

int RpcNode::poll_once(node_t remote_node_id, bool notify) {
  int thread_id = GetThreadID();
  auto &tl = tl_data_[thread_id];
  auto &state = tl.qp_state;
  int total_ack = 0;
  if (!state.inflight_ack) {
    return 0;
  }
  std::vector<uint64_t> wr_id_list;
  int qp_idx;
  int qp_size = manager_.get_qp_size(remote_node_id);

#if QP_MULTIPLEXING == 1
  if (is_client_) {
    // index starts from 0; use the last which is a normal QP
    qp_idx = qp_size - 1;
  } else {
    qp_idx = thread_id % (qp_size - 1);
  }
#else
  qp_idx = thread_id % qp_size;
#endif

  int rc = manager_.do_poll(remote_node_id, qp_idx, wr_id_list);
  if (rc < 0) {
    return -1;
  }
  if (manager_.config().qp_sharing) {
    for (auto &wr_id : wr_id_list) {
      int size = WR_SIZE(wr_id);
      int tid = THREAD_ID(wr_id);
      int node_id = REMOTE_NODE_ID(wr_id);
      __sync_fetch_and_add(&tl_data_[tid].qp_state.ack_req[node_id], size);
      total_ack += size;
      __sync_fetch_and_sub(&tl_data_[tid].qp_state.inflight_ack, size);
      if (notify) {
        auto &post_req = tl.post_req_snapshot[TASK_ID(wr_id)];
        if (!post_req.empty() && state.ack_req[node_id] >= post_req[node_id]) {
          NotifyTask(TASK_ID(wr_id));
        }
      }
    }
  } else {
    for (auto &wr_id : wr_id_list) {
      int size = WR_SIZE(wr_id);
      int node_id = REMOTE_NODE_ID(wr_id);
      state.ack_req[REMOTE_NODE_ID(wr_id)] += size;
      total_ack += size;
      state.inflight_ack -= size;
      if (notify) {
        auto &post_req = tl.post_req_snapshot[TASK_ID(wr_id)];
        if (!post_req.empty() && state.ack_req[node_id] >= post_req[node_id]) {
          NotifyTask(TASK_ID(wr_id));
        }
      }
    }
  }
  return total_ack;
}

int RpcNode::start(uint16_t tcp_port) {
  /**
   * Since the compute node in SMART and the RPC server in RDMAX
   *  are the ones using QP multiplexing, and the compute node initiates
   * the connection in SMART, the RPC server in RDMAX assumes the task of
   * initiating the connection in order to be compatible with it.
   */
  is_client_ = true;
  using namespace std::placeholders;
  if (listener_) {
    return -1;
  }
  listener_ = ResourceManager::Listener::Start(
      &manager_, tcp_port,
      [](ResourceManager::RemoteNode *) -> int { return 0; });
  if (!listener_) {
    return -1;
  }
  return 0;
}

int RpcNode::stop() {
  if (listener_) {
    listener_->stop();
    delete listener_;
    listener_ = nullptr;
  }
  return 0;
}

}  // namespace rdmax

namespace rdmax {

RequestRegion::RequestRegion(class RpcNode &node, node_t wrk_id, node_t clt_id)
    : node_(node), wrk_id_(wrk_id) {
  head_ = 0;
  tail_ = 0;
#if MEMORY_MULTIPLEXING == 1
  addr_ = (uint8_t *)node.get_cache_addr() + OFFSET(wrk_id);
  assert(((uintptr_t)addr_ & (WORKER_MEMORY_SIZE - 1)) == 0);
  index_base_ = 0;
  window_size_ = DEFUALT_WINDOW_SIZE;
  new_window_size_ = DEFUALT_WINDOW_SIZE;
#else
  addr_ = (uint8_t *)node.get_cache_addr() + OFFSET(wrk_id, clt_id, 0);
  window_size_ = MAX_OUTSTANDING_CMSGS;
#endif
}

/* Register a handler and an optional argument for a request type. */
void RequestRegion::register_rpc_handler(
    int req_type,
    size_t (*func)(uint8_t *resp_buf, rpc_resptype_t *resp_type,
                   const uint8_t *req_buf, size_t req_len, void *),
    void *arg) {
  assert(func != NULL);
  if (!RPC_IS_VALID_TYPE(req_type)) {
    printf("Rpc: Error. Cannot register handler for non-request types.\n");
    exit(-1);
  }

  if (rpc_handler[req_type] != NULL || rpc_handler_arg[req_type] != NULL) {
    printf("Rpc: Error. Function for handler type %d already registered.\n",
           req_type);
    exit(-1);
  }

  // if (arg == NULL) {
  //   printf("Rpc: Warning. Handler for request type %d assigned NULL arg.\n",
  //          req_type);
  // }

  rpc_handler[req_type] = func; /* Actually register */
  rpc_handler_arg[req_type] = arg;
}

int RequestRegion::poll_req_cmsgs() {
  int cmsg_i = 0;
  for (; cmsg_i < WORKER_DEPTH; ++cmsg_i) {
#if MEMORY_MULTIPLEXING == 1
    uint8_t *slot = addr_ + (((head_ + cmsg_i) % window_size_) * MSG_SLOT_SIZE +
                             index_base_ * MSG_SLOT_SIZE) %
                                WORKER_MEMORY_SIZE;
#else
    uint8_t *slot = addr_ + ((head_ + cmsg_i) % window_size_) * MSG_SLOT_SIZE;
#endif
    volatile rpc_cmsg_t *cmsg = (rpc_cmsg_t *)slot;
    if (cmsg->magic != RPC_REQ_MAGIC) {
      break;
    }
  }
  if (cmsg_i > 0) {
    head_ = (head_ + cmsg_i) % window_size_;
#if MEMORY_MULTIPLEXING == 1
    uint8_t *slot =
        addr_ + (((head_ + window_size_ - 1) % window_size_) * MSG_SLOT_SIZE +
                 index_base_ * MSG_SLOT_SIZE) %
                    WORKER_MEMORY_SIZE;
    rpc_cmsg_t *last_req_cmsg = (rpc_cmsg_t *)slot;
    new_window_size_ = be16toh(last_req_cmsg->window_size);
#endif
  }
  return cmsg_i;
}

int RequestRegion::handle_req_cmsgs(int num_cmsgs) {
  int num_inner_reqs = 0;

  for (int cmsg_i = 0; cmsg_i < num_cmsgs; ++cmsg_i) {
#if MEMORY_MULTIPLEXING == 1
    uint8_t *slot = addr_ + (((tail_ + cmsg_i) % window_size_) * MSG_SLOT_SIZE +
                             index_base_ * MSG_SLOT_SIZE) %
                                WORKER_MEMORY_SIZE;
#else
    uint8_t *slot = addr_ + ((tail_ + cmsg_i) % window_size_) * MSG_SLOT_SIZE;
#endif
    rpc_cmsg_t *req_cmsg = (rpc_cmsg_t *)slot;
    rpc_inner_msg_t *req = (rpc_inner_msg_t *)(req_cmsg + 1);

    uint8_t buf[MSG_SLOT_SIZE];
    rpc_cmsg_t *resp_cmsg = (rpc_cmsg_t *)buf;
    rpc_inner_msg_t *resp = (rpc_inner_msg_t *)(resp_cmsg + 1);
    size_t resp_cmsg_size = sizeof(rpc_cmsg_t);

    targets_[cmsg_i].clt_id = req_cmsg->src_id;
    targets_[cmsg_i].resp_offset = req_cmsg->resp_offset;

    num_inner_reqs += req_cmsg->num_inner_msgs;

    for (int req_i = 0; req_i < req_cmsg->num_inner_msgs; ++req_i) {
      assert(rpc_handler[req->req_type] != NULL);
      size_t resp_len = rpc_handler[req->req_type](
          (uint8_t *)(resp + 1), &(resp->resp_type), (uint8_t *)(req + 1),
          req->data_len, rpc_handler_arg[req->req_type]);
      resp->data_len = resp_len;
      resp_cmsg_size += sizeof(rpc_inner_msg_t) + resp_len;

      req = (rpc_inner_msg_t *)((uint8_t *)(req + 1) + req->data_len);
      resp = (rpc_inner_msg_t *)((uint8_t *)(resp + 1) + resp_len);
    }

    resp_cmsg->magic = RPC_RESP_MAGIC;
    resp_cmsg->num_inner_msgs = req_cmsg->num_inner_msgs;
    resp_cmsg->src_id = wrk_id_;
    resp_cmsg->cmsg_len = resp_cmsg_size;

    memcpy(slot, resp_cmsg, resp_cmsg_size);
  }

  return num_inner_reqs;
}

int RequestRegion::post_resp_cmsgs(int num_cmsgs) {
  int rc = 0;
  for (int cmsg_i = 0; cmsg_i < num_cmsgs; ++cmsg_i) {
#if MEMORY_MULTIPLEXING == 1
    uint8_t *slot = addr_ + (((tail_ + cmsg_i) % window_size_) * MSG_SLOT_SIZE +
                             index_base_ * MSG_SLOT_SIZE) %
                                WORKER_MEMORY_SIZE;
#else
    uint8_t *slot = addr_ + ((tail_ + cmsg_i) % window_size_) * MSG_SLOT_SIZE;
#endif
    rpc_cmsg_t *resp_cmsg = (rpc_cmsg_t *)slot;
    auto option = (cmsg_i == num_cmsgs - 1) ? RpcNode::Option::PostRequest
                                            : RpcNode::Option::None;
    GlobalAddress remote_addr(targets_[cmsg_i].clt_id,
                              targets_[cmsg_i].resp_offset);
    rc |= node_.write((void *)resp_cmsg, remote_addr, resp_cmsg->cmsg_len,
                      option);
  }
  assert(rc == 0);
  tail_ = (tail_ + num_cmsgs) % window_size_;
  return num_cmsgs;
}

#if MEMORY_MULTIPLEXING == 1
void RequestRegion::checkout_new_window() {
  if (window_size_ != new_window_size_) {
    index_base_ =
        (index_base_ + window_size_) % (WORKER_MEMORY_SIZE / MSG_SLOT_SIZE);
    head_ = 0;
    tail_ = 0;
    window_size_ = new_window_size_;
  }
}
#endif
}  // namespace rdmax

namespace rdmax {

ResponseRegion::ResponseRegion(class RpcNode &node, node_t local_node_id,
                               node_t qp_node_id, int max_clt_cmsg_depth)
    : node_(node), local_node_id_(local_node_id), qp_node_id_(qp_node_id) {
  assert(max_clt_cmsg_depth <= MAX_OUTSTANDING_CMSGS);
  global_clt_id_ = node.get_peer_node_id(local_node_id);
  offset_ = local_node_id * MSG_SLOT_SIZE * max_clt_cmsg_depth;
  addr_ = (uint8_t *)node.get_cache_addr() + offset_;
  num_cmsg_to_post_ = 0;
#if MEMORY_MULTIPLEXING == 0
  memset(head_, 0, sizeof(head_[0]) * MAX_NUM_WORKERS);
#endif
  memset(target_wrks_, -1, sizeof(target_wrks_[0]) * MAX_OUTSTANDING_CMSGS);
}

rpc_cmsg_t *ResponseRegion::start_req_cmsg(node_t wrk_id) {
  rpc_cmsg_t *req_cmsg =
      (rpc_cmsg_t *)(addr_ + num_cmsg_to_post_ * MSG_SLOT_SIZE);
  req_cmsg->magic = RPC_REQ_MAGIC;
  req_cmsg->num_inner_msgs = 0;
  req_cmsg->src_id = global_clt_id_;
  req_cmsg->cmsg_len = sizeof(rpc_cmsg_t);

  target_wrks_[num_cmsg_to_post_] = wrk_id;
  num_cmsg_to_post_++;
  return req_cmsg;
}

rpc_inner_msg_t *ResponseRegion::start_inner_msg(rpc_cmsg_t *cmsg,
                                                 rpc_reqtype_t req_type,
                                                 int size, uint8_t *resp_buf) {
  assert(num_cmsg_to_post_ > 0);
  rpc_inner_msg_t *inner_msg =
      (rpc_inner_msg_t *)((uint8_t *)cmsg + cmsg->cmsg_len);
  inner_msg->req_type = req_type;
  inner_msg->data_len = size;
  user_resp_buf_[num_cmsg_to_post_ - 1][cmsg->num_inner_msgs] = resp_buf;
  cmsg->num_inner_msgs++;
  cmsg->cmsg_len += sizeof(rpc_inner_msg_t) + size;
  return inner_msg;
}

int ResponseRegion::post_req_cmsgs() {
  node_.poll_once(qp_node_id_);
  int num_cmsg_posted = 0;
  for (int req_cmsg_i = 0; req_cmsg_i < num_cmsg_to_post_; ++req_cmsg_i) {
#if MEMORY_MULTIPLEXING == 1
    uint64_t offset = OFFSET(target_wrks_[req_cmsg_i]);
#else
    uint64_t offset = OFFSET(target_wrks_[req_cmsg_i], global_clt_id_,
                             head_[target_wrks_[req_cmsg_i]]);
    head_[target_wrks_[req_cmsg_i]] =
        (head_[target_wrks_[req_cmsg_i]] + 1) % MAX_OUTSTANDING_CMSGS;
#endif
    GlobalAddress remote_addr(qp_node_id_, offset);
    auto option = (req_cmsg_i == num_cmsg_to_post_ - 1)
                      ? RpcNode::Option::PostRequest
                      : RpcNode::Option::None;
    rpc_cmsg_t *req_cmsg = (rpc_cmsg_t *)(addr_ + req_cmsg_i * MSG_SLOT_SIZE);
    size_t cmsg_len = req_cmsg->cmsg_len;
    req_cmsg->resp_offset = offset_ + req_cmsg_i * MSG_SLOT_SIZE;
    if (node_.write((void *)req_cmsg, remote_addr, cmsg_len, option)) {
      return -1;
    }
    ++num_cmsg_posted;
  }
  num_cmsg_to_post_ = 0;
  return num_cmsg_posted;
}

rpc_cmsg_t *ResponseRegion::get_resp_cmsg(int index) {
  rpc_cmsg_t *resp_cmsg = (rpc_cmsg_t *)(addr_ + index * MSG_SLOT_SIZE);
  rpc_inner_msg_t *inner_msg = (rpc_inner_msg_t *)(resp_cmsg + 1);
  for (int i = 0; i < resp_cmsg->num_inner_msgs; ++i) {
    memcpy(user_resp_buf_[index][i], inner_msg + 1, inner_msg->data_len);
    inner_msg =
        (rpc_inner_msg_t *)((uint8_t *)(inner_msg + 1) + inner_msg->data_len);
  }
  return resp_cmsg;
}

bool ResponseRegion::check_resp_cmsg(int resp_i) {
  volatile rpc_cmsg_t *resp_cmsg =
      (rpc_cmsg_t *)(addr_ + resp_i * MSG_SLOT_SIZE);
  return resp_cmsg->magic == RPC_RESP_MAGIC;
}

}  // namespace rdmax
