#include "rdma_conn.h"
#include <arpa/inet.h>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <infiniband/verbs.h>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <sstream>
#include <thread>
#include <unistd.h>

std::unordered_map<
    uint8_t,
    std::pair<std::function<void(RDMAConnection *conn, const void *msg_data,
                                 uint32_t size, void *resp_data)>,
              uint32_t>>
    RDMAConnection::m_rpc_exec_map_;

template <typename Dur> uint64_t get_wall_time() {
  return std::chrono::duration_cast<Dur>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

template <typename T>
T alloc_buf_ptr(std::atomic<T> &buf_head, T size, T *tail_ptr,
                std::atomic<T> &left_half_cnt, std::atomic<T> &right_half_cnt,
                T min_len, T max_len) {
  T ret;
  T head_next;
  T current_head = buf_head.load(std::memory_order_acquire);
  while (1) {
    if (current_head + size > max_len) {
      head_next = size;
      ret = 0;
      if (tail_ptr) {
        *tail_ptr = current_head;
      }
    } else {
      head_next = current_head + size;
      ret = current_head;
    }
    if (head_next + min_len > max_len) {
      head_next = 0;
    }

    bool left;

    if (ret < max_len / 2) {
      if (current_head >= max_len / 2 &&
          left_half_cnt.load(std::memory_order_acquire) != 0) {
        return (T)-1;
      }
      if (ret + size >= max_len / 2 &&
          right_half_cnt.load(std::memory_order_acquire) != 0) {
        return (T)-1;
      }
      left_half_cnt.fetch_add(1, std::memory_order_acquire);
      left = true;
    } else {
      if (right_half_cnt.load(std::memory_order_acquire) != 0 &&
          current_head < max_len / 2) {
        return (T)-1;
      }
      right_half_cnt.fetch_add(1, std::memory_order_acquire);
      left = false;
    }

    if (buf_head.compare_exchange_weak(current_head, head_next,
                                       std::memory_order_acquire)) {
      break;
    }
    if (left) {
      left_half_cnt.fetch_sub(1, std::memory_order_release);
    } else {
      right_half_cnt.fetch_sub(1, std::memory_order_release);
    }
    std::this_thread::yield();
  }
  return ret;
}

template <typename T>
T alloc_buf_ptr(T &buf_head, T size, T *tail_ptr, std::atomic<T> &left_half_cnt,
                std::atomic<T> &right_half_cnt, T min_len, T max_len) {
  T ret;
  T head_next;
  const T current_head = buf_head;

  if (current_head + size > max_len) {
    head_next = size;
    ret = 0;
    if (tail_ptr) {
      *tail_ptr = current_head;
    }
  } else {
    head_next = current_head + size;
    ret = current_head;
  }
  if (head_next + min_len > max_len) {
    head_next = 0;
  }

  if (ret < max_len / 2) {
    if (current_head >= max_len / 2 &&
        left_half_cnt.load(std::memory_order_acquire) != 0) {
      return (T)-1;
    }
    if (ret + size >= max_len / 2 &&
        right_half_cnt.load(std::memory_order_acquire) != 0) {
      return (T)-1;
    }
    left_half_cnt.fetch_add(1, std::memory_order_acquire);
  } else {
    if (right_half_cnt.load(std::memory_order_acquire) != 0 &&
        current_head < max_len / 2) {
      return (T)-1;
    }
    right_half_cnt.fetch_add(1, std::memory_order_acquire);
  }

  buf_head = head_next;
  return ret;
}

template <typename T>
void dealloc_buf_ptr(T ptr, std::atomic<T> &left_half_cnt,
                     std::atomic<T> &right_half_cnt, T max_len) {
  if (ptr < max_len / 2) {
    left_half_cnt.fetch_sub(1, std::memory_order_release);
  } else {
    right_half_cnt.fetch_sub(1, std::memory_order_release);
  }
}

// 重用sync_data_t
static thread_local std::vector<RDMAConnection::sync_data_t *> sd_pool;
RDMAConnection::sync_data_t *RDMAConnection::alloc_sync_data() {
  if (sd_pool.empty()) {
    return new RDMAConnection::sync_data_t();
  } else {
    RDMAConnection::sync_data_t *sd = sd_pool.back();
    sd_pool.pop_back();
    return sd;
  }
}
void RDMAConnection::dealloc_sync_data(RDMAConnection::sync_data_t *sd) {
  sd->msg_offsets.clear();
  sd->resp_mbs.clear();
  sd->resp_data_ptr.clear();
  sd_pool.push_back(sd);
}

void RDMAConnection::register_rpc_func(
    uint8_t rpc_op,
    std::function<void(RDMAConnection *conn, const void *msg_data,
                       uint32_t size, void *resp_data)> &&rpc_func,
    uint32_t resp_max_size) {
  if (rpc_op == 0) {
    perror("rpc op can't be 0");
    return;
  }

  RDMAConnection::m_rpc_exec_map_.emplace(
      rpc_op,
      std::make_pair(
          std::forward<
              std::function<void(RDMAConnection * conn, const void *msg_data,
                                 uint32_t size, void *resp_data)>>(rpc_func),
          resp_max_size));
}

void RDMAConnection::msg_recv_work() {
  std::deque<uint64_t> hdls;
  std::vector<const void *> resp_tmp;
  uint16_t msg_size;
  MsgQueueHandle recver_qh;

  // printf("try poll %p\n", (char *)recver.m_msg_buf_->addr);

  while (!m_stop_) {
    MsgBlock *msg_mb =
        (MsgBlock *)((char *)recver.m_msg_buf_->addr + recver.m_msg_head_);
    // 1. 轮询当前msg_buf的指针指向的mb是否完成
    if (msg_mb->valid()) {
      // printf("poll %p\n", msg_mb);
      msg_size = msg_mb->msg_size();

      // 如果是nop，将轮询指针回环到头
      if (msg_mb->rpc_op == 0) {
        recver.m_msg_head_ = 0;
        continue;
      }

      // 2. 为消息返回申请resp_buf
      MsgBlock *resp_mb =
          (MsgBlock *)((char *)recver.m_resp_buf_->addr + msg_mb->resp_offset);
      resp_mb->notify = 1;
      resp_mb->size = m_rpc_exec_map_[msg_mb->rpc_op].second;
      resp_mb->set_complete_byte();

      // 3. 执行处理函数
      m_rpc_exec_map_[msg_mb->rpc_op].first(this, msg_mb->data, msg_mb->size,
                                            resp_mb->data);

      // printf("send resp: %#lx %u\n",
      //        recver.m_peer_resp_buf_addr_ + msg_mb->resp_offset,
      //        resp_mb->msg_size());

      // 4. 发送返回
      prep_write(recver_qh, (uintptr_t)resp_mb, recver.m_resp_buf_->lkey,
                 resp_mb->msg_size(),
                 recver.m_peer_resp_buf_addr_ + msg_mb->resp_offset,
                 recver.m_peer_resp_buf_rkey_);

      // batch 返回
      if (msg_mb->last_end) {
        uint64_t hdl = submit(recver_qh);
        hdls.push_back(hdl);
      }

      // 5. 清空消息buf
      memset(msg_mb, 0, msg_size);

      // 6. 推进轮询msg_buf指针
      const uint32_t msg_head = recver.m_msg_head_;
      uint32_t msg_head_next;
      if (MAX_MESSAGE_BUFFER_SIZE - msg_head - msg_size <
          MsgBlock::msg_min_size()) {
        msg_head_next = 0;
      } else {
        msg_head_next = msg_head + msg_size;
      }
      recver.m_msg_head_ = msg_head_next;

      // printf("try poll %p\n",
      //        (char *)recver.m_msg_buf_->addr + recver.m_msg_head_.load());
    }
    // 7. 轮询返回值是否发送成功
    while (!hdls.empty()) {
      uint64_t hdl = hdls.front();
      if (remote_task_try_get(hdl, resp_tmp) == 0) {
        hdls.pop_front();
      }
    }
    std::this_thread::yield();
  }
}

void *RDMAConnection::prep_rpc_send_defer(MsgQueueHandle &qh, uint8_t rpc_op,
                                          uint32_t param_data_length,
                                          uint32_t resp_data_length) {
  if (__glibc_unlikely(!m_rpc_conn_)) {
    perror("non rpc_conn can't send rpc msg");
    return nullptr;
  }

  uint32_t msg_nop_offset = 0;
  MsgBlock *msg_mb, *nop_mb, tmp;

  pthread_spin_lock(&m_sending_lock_);

  m_send_defer_cnt_.fetch_add(1, std::memory_order_acquire);

  // 1. 申请msg_buf
  tmp.size = param_data_length;
  uint32_t msg_offset = alloc_buf_ptr<uint32_t>(
      sender.m_msg_head_, tmp.msg_size(), &msg_nop_offset,
      sender.m_msg_buf_left_half_cnt_, sender.m_msg_buf_right_half_cnt_,
      MsgBlock::msg_min_size(), MAX_MESSAGE_BUFFER_SIZE);

  if (__glibc_unlikely(msg_offset == -1U)) {
    perror("msg buf full");
  }

  // 2. 如果尾部不够，则在尾部插入一个nop操作，让recver跳过尾部再回环轮询
  if (msg_nop_offset != 0) {
    nop_mb = (MsgBlock *)((char *)sender.m_msg_buf_->addr + msg_nop_offset);
    nop_mb->rpc_op = 0;
    nop_mb->size = 0;
    nop_mb->notify = 1;
    nop_mb->set_complete_byte();

    prep_write(m_msg_qh_, (uintptr_t)nop_mb, sender.m_msg_buf_->lkey,
               MsgBlock::msg_min_size(),
               sender.m_peer_msg_buf_addr_ + msg_nop_offset,
               sender.m_peer_msg_buf_rkey_);
  }

  // 3. 申请resp_buf
  tmp.size = resp_data_length;
  uint32_t resp_offset = alloc_buf_ptr<uint32_t>(
      sender.m_resp_head_, tmp.msg_size(), nullptr,
      sender.m_resp_buf_left_half_cnt_, sender.m_resp_buf_right_half_cnt_,
      MsgBlock::msg_min_size(), MAX_MESSAGE_BUFFER_SIZE);

  if (__glibc_unlikely(resp_offset == -1U)) {
    perror("resp buf full");
  }

  // 4. 发送msg
  msg_mb = (MsgBlock *)((char *)sender.m_msg_buf_->addr + msg_offset);
  msg_mb->size = param_data_length;
  msg_mb->notify = 1;
  msg_mb->resp_offset = resp_offset;
  msg_mb->rpc_op = rpc_op;
  msg_mb->set_complete_byte();

  prep_write(m_msg_qh_, (uintptr_t)msg_mb, sender.m_msg_buf_->lkey,
             msg_mb->msg_size(), sender.m_peer_msg_buf_addr_ + msg_offset,
             sender.m_peer_msg_buf_rkey_);

  pthread_spin_unlock(&m_sending_lock_);

  // 5. 清空resp buf
  memset((char *)sender.m_resp_buf_->addr + resp_offset, 0, tmp.msg_size());

  // printf("send msg: %p %u, ready resp: %p %u\n", msg_mb, msg_mb->msg_size(),
  //        (char *)sender.m_resp_buf_->addr + resp_offset, tmp.msg_size());

  qh.msg_offsets.push_back(msg_offset);
  qh.resp_mbs.push_back(
      (MsgBlock *)((char *)sender.m_resp_buf_->addr + resp_offset));

  return msg_mb->data;
}

int RDMAConnection::prep_write(MsgQueueHandle &qh, uint64_t local_addr,
                               uint32_t lkey, uint32_t length,
                               uint64_t remote_addr, uint32_t rkey) {
  qh.sges.push_back(
      ibv_sge{.addr = local_addr, .length = length, .lkey = lkey});

  qh.send_wrs.push_back(ibv_send_wr{
      .num_sge = 1,
      .opcode = IBV_WR_RDMA_WRITE,
      .wr = {.rdma = {.remote_addr = remote_addr, .rkey = rkey}},
  });

  return 0;
}

int RDMAConnection::prep_read(MsgQueueHandle &qh, uint64_t local_addr,
                              uint32_t lkey, uint32_t length,
                              uint64_t remote_addr, uint32_t rkey) {
  qh.sges.push_back(
      ibv_sge{.addr = local_addr, .length = length, .lkey = lkey});

  qh.send_wrs.push_back(ibv_send_wr{
      .num_sge = 1,
      .opcode = IBV_WR_RDMA_READ,
      .wr = {.rdma = {.remote_addr = remote_addr, .rkey = rkey}},
  });

  return 0;
}

int RDMAConnection::prep_fetch_add(MsgQueueHandle &qh, uint64_t local_addr,
                                   uint32_t lkey, uint64_t remote_addr,
                                   uint32_t rkey, uint64_t n) {
  if (__glibc_unlikely(!m_atomic_support_)) {
    perror("rdma fetch add: this device don't support atomic operations");
    return -1;
  }
  if (__glibc_unlikely(remote_addr % 8)) {
    perror("rdma fetch add: remote addr must be 8-byte aligned");
    return -1;
  }

  qh.sges.push_back(ibv_sge{.addr = local_addr, .length = 8, .lkey = lkey});

  qh.send_wrs.push_back(ibv_send_wr{
      .num_sge = 1,
      .opcode = IBV_WR_ATOMIC_FETCH_AND_ADD,
      .wr = {.atomic =
                 {
                     .remote_addr = remote_addr,
                     .compare_add = n,
                     .rkey = rkey,
                 }},
  });

  return 0;
}

int RDMAConnection::prep_cas(MsgQueueHandle &qh, uint64_t local_addr,
                             uint32_t lkey, uint64_t remote_addr, uint32_t rkey,
                             uint64_t expected, uint64_t desired) {
  if (__glibc_unlikely(!m_atomic_support_)) {
    perror("rdma fetch add: this device don't support atomic operations");
    return -1;
  }
  if (__glibc_unlikely(remote_addr % 8)) {
    perror("rdma cas: remote addr must be 8-byte aligned");
    return -1;
  }

  qh.sges.push_back(ibv_sge{.addr = local_addr, .length = 8, .lkey = lkey});

  qh.send_wrs.push_back(ibv_send_wr{
      .num_sge = 1,
      .opcode = IBV_WR_ATOMIC_CMP_AND_SWP,
      .wr = {.atomic =
                 {
                     .remote_addr = remote_addr,
                     .compare_add = expected,
                     .swap = desired,
                     .rkey = rkey,
                 }},
  });
  return 0;
}

int RDMAConnection::prep_rpc_send(MsgQueueHandle &qh, uint8_t rpc_op,
                                  const void *param_data,
                                  uint32_t param_data_length,
                                  uint32_t resp_data_length) {
  void *msg_buf =
      prep_rpc_send_defer(qh, rpc_op, param_data_length, resp_data_length);

  if (__glibc_unlikely(!msg_buf)) {
    return -1;
  }

  memcpy(msg_buf, param_data, param_data_length);

  prep_rpc_send_confirm();
  return 0;
}

void RDMAConnection::prep_rpc_send_confirm() {
  m_send_defer_cnt_.fetch_sub(1, std::memory_order_release);
}

uint64_t RDMAConnection::submit(MsgQueueHandle &qh) {
  std::vector<ibv_send_wr> &send_wrs = qh.send_wrs;
  std::vector<ibv_sge> &sges = qh.sges;
  std::vector<ibv_send_wr> send_msg_wrs;
  std::vector<ibv_sge> msg_sges;

  sync_data_t *sd = alloc_sync_data();

  pthread_spin_lock(&m_sending_lock_);

  while (m_send_defer_cnt_.load(std::memory_order_relaxed)) {
    std::this_thread::yield();
  }
  sd->msg_offsets.swap(qh.msg_offsets);
  sd->resp_mbs.swap(qh.resp_mbs);
  msg_sges.swap(m_msg_qh_.sges);
  send_msg_wrs.swap(m_msg_qh_.send_wrs);

  pthread_spin_unlock(&m_sending_lock_);

  if (!sd->msg_offsets.empty()) {
    MsgBlock *first_msg_mb =
        (MsgBlock *)((char *)sender.m_msg_buf_->addr + sd->msg_offsets.back());
    first_msg_mb->last_end = true;
  }

  sd->resp_poll_idx = 0;
  sd->timeout = false;

  for (size_t i = 0; i < send_wrs.size(); ++i) {
    send_wrs[i].sg_list = &sges[i];
    send_wrs[i].next = &send_wrs[i + 1];

    // printf("send %#lx to %#lx\n", sges[i].addr,
    //        send_wrs[i].wr.rdma.remote_addr);
  }
  for (size_t i = 0; i < send_msg_wrs.size(); ++i) {
    send_msg_wrs[i].sg_list = &msg_sges[i];
    send_msg_wrs[i].next = &send_msg_wrs[i + 1];

    // printf("send msg %#lx to %#lx\n", msg_sges[i].addr,
    //        send_msg_wrs[i].wr.rdma.remote_addr);
  }

  ibv_send_wr *wr_head = nullptr;
  if (!send_wrs.empty()) {
    wr_head = &send_wrs.front();
    if (!send_msg_wrs.empty()) {
      send_wrs.back().next = &send_msg_wrs.front();
      send_msg_wrs.back().next = nullptr;
    } else {
      send_wrs.back().next = nullptr;
    }
  } else if (!send_msg_wrs.empty()) {
    wr_head = &send_msg_wrs.front();
    send_msg_wrs.back().next = nullptr;
  }
  // send_wrs.back().next = nullptr;

  // 相邻的且addr是连续的wr进行组合
  // 一种方式实现rpc msg batch发送
  size_t combined_wr_size = 0;
  ibv_send_wr *back_wr;
  if (wr_head) {
    combined_wr_size = 1;
    back_wr = wr_head;
    for (ibv_send_wr *wri = back_wr, *wrj = wri->next; wrj != nullptr;
         wrj = wrj->next) {
      if ((wri->opcode == wrj->opcode) &&
          (wri->sg_list->addr + wri->sg_list->length == wrj->sg_list->addr)) {
        wri->sg_list->length += wrj->sg_list->length;
        wri->next = wrj->next;
      } else {
        wri = back_wr = wrj;
        ++combined_wr_size;
      }
    }
  }

  sd->inflight = combined_wr_size;

  uint64_t wr_id = reinterpret_cast<uint64_t>(sd);

  if (combined_wr_size == 0) {
    sd->wc_finish = true;
    sd->now_ms = get_wall_time<std::chrono::milliseconds>();
    return wr_id;
  }

  sd->wc_finish = false;

  back_wr->wr_id = wr_id;
  back_wr->send_flags = IBV_SEND_SIGNALED;

  if (m_inline_support_) {
    // 较小的msg可以inline到发送包里减少时延
    for (ibv_send_wr *wp = wr_head; wp != nullptr; wp = wp->next) {
      if (wp->opcode == IBV_WR_RDMA_WRITE &&
          wp->sg_list->length <= MSG_INLINE_THRESHOLD) {
        wp->send_flags |= IBV_SEND_INLINE;
      }
    }
  }

  // 探察当前正在发送的wr个数
  uint32_t inflight = m_inflight_count_.load(std::memory_order_acquire);
  do {
    if (__glibc_unlikely(inflight + combined_wr_size > MAX_SEND_WR)) {
      perror("ibv_post_send too much inflight wr");
      goto need_retry;
    }
  } while (!m_inflight_count_.compare_exchange_weak(
      inflight, inflight + combined_wr_size, std::memory_order_acquire));

  sd->now_ms = get_wall_time<std::chrono::milliseconds>();

  struct ibv_send_wr *bad_send_wr;
  if (__glibc_unlikely(ibv_post_send(m_cm_id_->qp, wr_head, &bad_send_wr) !=
                       0)) {
    perror("ibv_post_send fail");
    goto need_retry;
  }

  qh.send_wrs.clear();
  qh.sges.clear();

  return wr_id;

need_retry:
  pthread_spin_lock(&m_sending_lock_);
  if (m_msg_qh_.send_wrs.empty()) {
    m_msg_qh_.send_wrs.swap(send_msg_wrs);
    m_msg_qh_.sges.swap(msg_sges);
  } else {
    m_msg_qh_.send_wrs.insert(m_msg_qh_.send_wrs.end(), send_msg_wrs.begin(),
                              send_msg_wrs.end());
    m_msg_qh_.sges.insert(m_msg_qh_.sges.end(), msg_sges.begin(),
                          msg_sges.end());
  }
  if (m_msg_qh_.msg_offsets.empty()) {
    m_msg_qh_.msg_offsets.swap(sd->msg_offsets);
    m_msg_qh_.resp_mbs.swap(sd->resp_mbs);
  } else {
    m_msg_qh_.msg_offsets.insert(m_msg_qh_.msg_offsets.end(),
                                 sd->msg_offsets.begin(),
                                 sd->msg_offsets.end());
    m_msg_qh_.resp_mbs.insert(m_msg_qh_.resp_mbs.end(), sd->resp_mbs.begin(),
                              sd->resp_mbs.end());
  }
  pthread_spin_unlock(&m_sending_lock_);

  dealloc_sync_data(sd);

  return 0;
}

int RDMAConnection::acknowledge_cqe(int rc, ibv_wc wcs[]) {
  for (int i = 0; i < rc; ++i) {
    auto &wc = wcs[i];

    sync_data_t *sd = reinterpret_cast<sync_data_t *>(wc.wr_id);
    m_inflight_count_.fetch_sub(sd->inflight, std::memory_order_release);

    if (__glibc_likely(!sd->timeout)) {
      sd->wc_finish = true;
    } else {
      // ! 如果超时了，resp_buf本应该释放，但是remote仍可能发送，并造成污染
      for (size_t j = 0; j < sd->resp_mbs.size(); ++j) {
        auto &msg_offset = sd->msg_offsets[j];
        MsgBlock *msg_mb =
            (MsgBlock *)((char *)sender.m_msg_buf_->addr + msg_offset);
        memset(msg_mb, 0, msg_mb->msg_size());
        dealloc_buf_ptr<uint32_t>(msg_offset, sender.m_msg_buf_left_half_cnt_,
                                  sender.m_msg_buf_right_half_cnt_,
                                  MAX_MESSAGE_BUFFER_SIZE);
      }
      // 其他线程在轮询超时后，已经不能继续轮询，此时需要将sd删除
      dealloc_sync_data(sd);
    }
    if (__glibc_likely(IBV_WC_SUCCESS == wc.status)) {
      // Break out as operation completed successfully
    } else if (IBV_WC_WR_FLUSH_ERR == wc.status) {
      perror("cmd_send IBV_WC_WR_FLUSH_ERR");
      return -1;
    } else if (IBV_WC_RNR_RETRY_EXC_ERR == wc.status) {
      perror("cmd_send IBV_WC_RNR_RETRY_EXC_ERR");
      return -1;
    } else {
      fprintf(stderr, "cmd_send status error: %s\n",
              ibv_wc_status_str(wc.status));
      return -1;
    }
  }
  return 0;
}

int RDMAConnection::remote_task_try_get(
    uint64_t task_id, std::vector<const void *> &resp_data_ptr) {
  struct ibv_wc wcs[POLL_ENTRY_COUNT];
  sync_data_t *l_sd = reinterpret_cast<sync_data_t *>(task_id);

  if (__glibc_unlikely(l_sd->timeout)) {
    perror("rdma task timeout");
    return -1;
  }

  int rc = ibv_poll_cq(m_cq_, POLL_ENTRY_COUNT, wcs);
  if (__glibc_unlikely(rc < 0)) {
    perror("ibv_poll_cq fail");
    return -1;
  }

  if (__glibc_unlikely(acknowledge_cqe(rc, wcs) == -1)) {
    perror("acknowledge_cqe fail");
    return -1;
  }

  // 轮询resp
  if (try_poll_resp(l_sd) == 0 && l_sd->wc_finish) {
    resp_data_ptr.swap(l_sd->resp_data_ptr);
    dealloc_sync_data(l_sd);
    return 0;
  }

  // 超时检测
  uint32_t now = get_wall_time<std::chrono::milliseconds>();

  if (now - l_sd->now_ms > RDMA_TIMEOUT_MS) {
    l_sd->timeout = true;
    perror("rdma task timeout");
    return -1;
  }

  return 1;
}

int RDMAConnection::try_poll_resp(sync_data_t *sd) {
  for (size_t &poll_idx = sd->resp_poll_idx; poll_idx < sd->resp_mbs.size();
       ++poll_idx) {
    auto &msg_offset = sd->msg_offsets[poll_idx];
    auto &resp_mb = sd->resp_mbs[poll_idx];
    if (!resp_mb->valid()) {
      return 1;
    }

    // printf("ack msg: %p %u\n", resp_mb, resp_mb->msg_size());

    if (resp_mb->size > 0) {
      // 0拷贝返回用户resp_data，由用户调用dealloc_resp_data()释放
      sd->resp_data_ptr.push_back(resp_mb->data);
    } else {
      sd->resp_data_ptr.push_back(nullptr);
      dealloc_resp_data(resp_mb->data);
    }
    // 释放msg_buf
    MsgBlock *msg_mb =
        (MsgBlock *)((char *)sender.m_msg_buf_->addr + msg_offset);
    memset(msg_mb, 0, msg_mb->msg_size());
    dealloc_buf_ptr<uint32_t>(msg_offset, sender.m_msg_buf_left_half_cnt_,
                              sender.m_msg_buf_right_half_cnt_,
                              MAX_MESSAGE_BUFFER_SIZE);
  }
  return (sd->resp_poll_idx == sd->resp_mbs.size()) ? 0 : 1;
}

int RDMAConnection::remote_task_wait(uint64_t task_id,
                                     std::vector<const void *> &resp_data_ptr) {
  while (1) {
    int ret = remote_task_try_get(task_id, resp_data_ptr);
    if (ret == 0) {
      return 0;
    } else if (__glibc_unlikely(ret == -1)) {
      return -1;
    }
    std::this_thread::yield();
  }
}

void RDMAConnection::dealloc_resp_data(const void *data_ptr) {
  // 根据偏移找到resp_buf，释放resp_buf
  uint32_t offset = (uint64_t)data_ptr - (uint64_t)sender.m_resp_buf_->addr -
                    offsetof(MsgBlock, data);
  dealloc_buf_ptr<uint32_t>(offset, sender.m_resp_buf_left_half_cnt_,
                            sender.m_resp_buf_right_half_cnt_,
                            MAX_MESSAGE_BUFFER_SIZE);
}