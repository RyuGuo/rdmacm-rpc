#include "rdma_conn.h"

struct SyncData {
  volatile bool wc_finish;
  bool timeout;
  uint32_t inflight;
  uint32_t now_ms;
  size_t resp_poll_idx;
  RDMAConnection *conn;
  std::vector<uint32_t> msg_offsets;
  std::vector<MsgBlock *> resp_mbs;
};

std::unordered_map<
  uint16_t, std::function<uint32_t(RDMAConnection *conn, void *msg_data, uint32_t length,
                                   void *resp_data, uint32_t max_resp_data_length, void **uctx)>>
  RDMAConnection::m_rpc_handle_map_;

std::function<uint32_t(RDMAConnection *conn, uint32_t write_imm_data, void **uctx)>
  RDMAConnection::m_write_with_imm_handle_;

template <typename Dur>
uint64_t get_wall_time() {
  return std::chrono::duration_cast<Dur>(std::chrono::system_clock::now().time_since_epoch())
    .count();
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
    if (current_head >= max_len / 2 && left_half_cnt.load(std::memory_order_acquire) != 0) {
      return (T)-1;
    }
    if (ret + size >= max_len / 2 && right_half_cnt.load(std::memory_order_acquire) != 0) {
      return (T)-1;
    }
    left_half_cnt.fetch_add(1, std::memory_order_acquire);
  } else {
    if (right_half_cnt.load(std::memory_order_acquire) != 0 && current_head < max_len / 2) {
      return (T)-1;
    }
    right_half_cnt.fetch_add(1, std::memory_order_acquire);
  }

  buf_head = head_next;
  return ret;
}

template <typename T>
void dealloc_buf_ptr(T ptr, std::atomic<T> &left_half_cnt, std::atomic<T> &right_half_cnt,
                     T max_len) {
  if (ptr < max_len / 2) {
    left_half_cnt.fetch_sub(1, std::memory_order_release);
  } else {
    right_half_cnt.fetch_sub(1, std::memory_order_release);
  }
}

// 重用sync_data_t的池
static thread_local std::vector<SyncData *> sd_pool;
SyncData *alloc_sync_data() {
  if (sd_pool.empty()) {
    return new SyncData();
  } else {
    SyncData *sd = sd_pool.back();
    sd_pool.pop_back();
    return sd;
  }
}
void dealloc_sync_data(SyncData *sd) {
  sd->msg_offsets.clear();
  sd->resp_mbs.clear();
  sd_pool.push_back(sd);
}

void RDMAConnection::register_rpc_func(
  uint16_t rpc_op,
  std::function<uint32_t(RDMAConnection *conn, void *msg_data, uint32_t length, void *resp_data,
                         uint32_t max_resp_data_length, void **uctx)> &&rpc_func) {
  RDMAConnection::m_rpc_handle_map_.emplace(rpc_op, rpc_func);
}

void RDMAConnection::register_rdma_write_with_imm_handle(
  std::function<uint32_t(RDMAConnection *conn, uint32_t write_imm_data, void **uctx)>
    &&write_with_imm_handle) {
  m_write_with_imm_handle_ = std::forward<
    std::function<uint32_t(RDMAConnection * conn, uint32_t write_imm_data, void **uctx)>>(
    write_with_imm_handle);
}

SpinLock RDMAConnection::m_core_bind_lock_;

RDMAMsgRTCThread::RDMAMsgRTCThread(rdma_thread_id_t tid)
  : m_stop_(false), m_th_id_(tid), m_core_id_(-1) {
  m_th_ = std::thread(&RDMAMsgRTCThread::thread_routine, this);
}
RDMAMsgRTCThread::~RDMAMsgRTCThread() {
  m_stop_ = true;
  m_th_.join();
  if (m_core_id_ != -1) {
    RDMAConnection::m_core_bind_lock_.lock();
    RDMAConnection::VEC_RECVER_THREAD_BIND_CORE.push_back(m_core_id_);
    RDMAConnection::m_core_bind_lock_.unlock();
  }
}
void RDMAMsgRTCThread::join_recver_conn(RDMAConnection *conn) {
  m_set_lck_.lock();
  m_conn_set_.push_back(conn);
  m_qp_conn_map_.emplace(conn->m_cm_id_->qp->qp_num, conn);
  m_set_lck_.unlock();
}
void RDMAMsgRTCThread::exit_recver_conn(RDMAConnection *conn) {
  m_set_lck_.lock();
  // 这里conn set采用数组，以减少轮询时的cache miss
  // conn退出时时间复杂度较高，但可忽略
  for (auto it = m_conn_set_.begin(); it != m_conn_set_.end(); ++it) {
    m_conn_set_.erase(it);
    break;
  }
  m_qp_conn_map_.erase(conn->m_cm_id_->qp->qp_num);
  m_set_lck_.unlock();
}
void RDMAMsgRTCThread::core_bind() {
  if (RDMAConnection::VEC_RECVER_THREAD_BIND_CORE.empty())
    return;
  RDMAConnection::m_core_bind_lock_.lock();
  if (RDMAConnection::VEC_RECVER_THREAD_BIND_CORE.empty())
    return;
  m_core_id_ = RDMAConnection::VEC_RECVER_THREAD_BIND_CORE.back();
  RDMAConnection::VEC_RECVER_THREAD_BIND_CORE.pop_back();
  RDMAConnection::m_core_bind_lock_.unlock();

  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(m_core_id_, &mask);
  if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) == -1) {
    perror("pthread_setaffinity_np fail");
    return;
  }
}
void RDMAMsgRTCThread::thread_routine() {
  RDMABatch void_batch;
  core_bind();

  while (__glibc_likely(!m_stop_)) {
    m_set_lck_.lock();
    for (auto &conn : m_conn_set_) {
      poll_msg(conn);
      poll_recv_task(conn);

      // 轮询返回值是否发送成功
      // 注：这里仅poll_cq而没有探测msg_mb，无法在用户端实现自动poll_msg
      if (conn->m_inflight_count_.load(std::memory_order_relaxed) > 0) {
        conn->m_poll_conn_sd_wr_();
      }
    }

    if (!m_tps_.empty()) {
      // 将poll msg分发到其他线程执行
      RDMAThreadScheduler::get_instance().task_dispatch(this, m_tps_);
      m_tps_.clear();
    }

    if (!m_uctx_tps_.empty()) {
      m_task_queue_.enqueue_bulk(m_uctx_tps_.begin(), m_uctx_tps_.size());
      m_uctx_tps_.clear();
    }

    ThreadTaskPack tp;
    while (m_task_queue_.try_dequeue(tp)) {
      RDMAConnection *conn = tp.conn;

      switch (tp.type) {
      case ThreadTaskPack::MSG: {
        uint32_t ret = conn->m_msg_handle_(conn, tp.msg_mb, &tp.uctx);
        if (ret == RDMAConnection::UCTX_YIELD) {
          m_uctx_tps_.push_back(tp);
          continue;
        }

        // 如果不是必要的立即返回（batch
        // rpc，且有些rpc不需要立即返回），可以暂时等待后面的rpc完成
        if (!tp.is_not_last) {
          // 这里的等待保证wr与resp msg addr的排序一致，最大程度起到batch作用
          while (__atomic_load_n(tp.to_seq_ptr, __ATOMIC_RELAXED) != tp.seq) {
            std::this_thread::yield();
          }
          // printf("[%u] send resp clear\n", m_th_id_);
          conn->submit(void_batch);
          // 那些需要返回消息的tp作为flag task进行回收资源
          RDMAThreadScheduler::get_instance().flag_task_done(tp);
        } else {
          __atomic_fetch_add(tp.to_seq_ptr, 1, __ATOMIC_RELEASE);
        }

        // 清空消息buf
        memset(tp.msg_mb, 0, ret);
      } break;
      case ThreadTaskPack::WRITE_IMM: {
        uint32_t ret = conn->m_write_with_imm_handle_(conn, tp.write_imm_data, &tp.uctx);
        if (ret == RDMAConnection::UCTX_YIELD) {
          m_uctx_tps_.push_back(tp);
          continue;
        }
      } break;
      }
    }

    m_set_lck_.unlock();
  }
}

uint32_t RDMAConnection::m_msg_handle_(RDMAConnection *conn, MsgBlock *msg_mb, void **uctx) {
  // 1. 为消息返回申请resp_buf
  MsgBlock *resp_mb = (MsgBlock *)((char *)m_recver_.m_resp_buf_->addr + msg_mb->resp_offset);
  if (*uctx == nullptr) {
    resp_mb->notify = 1;
    resp_mb->size = msg_mb->prep_resp_size;
    resp_mb->set_complete_byte();
  }

  // 2. 执行处理函数
  uint32_t resp_size = m_rpc_handle_map_[msg_mb->rpc_op](
    conn, msg_mb->data, msg_mb->size, resp_mb->data, msg_mb->prep_resp_size, uctx);

  if (resp_size == RDMAConnection::UCTX_YIELD) {
    return RDMAConnection::UCTX_YIELD;
  } else if (msg_mb->prep_resp_size < resp_size) {
    errno = EFBIG;
    perror("response size is too large, it'll be truncated when sending "
           "back");
  }

  // printf("[%lu] send resp: %#lx %u\n", pthread_self(),
  //        m_recver_.m_peer_resp_buf_addr_ + msg_mb->resp_offset,
  //        resp_mb->msg_size());

  // 3. 发送返回
  conn->m_sending_lock_.lock();
  conn->prep_write(conn->m_msg_batch_, (uintptr_t)resp_mb, m_recver_.m_resp_buf_->lkey,
                   resp_mb->msg_size(), m_recver_.m_peer_resp_buf_addr_ + msg_mb->resp_offset,
                   m_recver_.m_peer_resp_buf_rkey_);
  conn->m_sending_lock_.unlock();

  return msg_mb->msg_size();
}

void *RDMAConnection::prep_rpc_send_defer(RDMABatch &b, uint8_t rpc_op, uint32_t param_data_length,
                                          uint32_t resp_data_length) {
  if (__glibc_unlikely(!m_rpc_conn_)) {
    errno = EPERM;
    perror("non rpc_conn can't send rpc msg");
    return nullptr;
  }

  uint32_t msg_nop_offset = 0;
  MsgBlock *msg_mb, *nop_mb, tmp;

  m_sending_lock_.lock();

  uint32_t current_msg_head = m_sender_.m_msg_head_;

  // 1. 申请msg_buf
  tmp.size = param_data_length;
  uint32_t msg_offset = alloc_buf_ptr<uint32_t>(
    m_sender_.m_msg_head_, tmp.msg_size(), &msg_nop_offset, m_sender_.m_msg_buf_left_half_cnt_,
    m_sender_.m_msg_buf_right_half_cnt_, MsgBlock::msg_min_size(), m_sender_.m_matched_buf_size_);

  if (__glibc_unlikely(msg_offset == -1U)) {
    m_sending_lock_.unlock();
    errno = ENOMEM;
    perror("msg buf full");
    return nullptr;
  }

  // 2. 申请resp_buf
  tmp.size = resp_data_length;
  uint32_t resp_offset = alloc_buf_ptr<uint32_t>(
    m_sender_.m_resp_head_, tmp.msg_size(), nullptr, m_sender_.m_resp_buf_left_half_cnt_,
    m_sender_.m_resp_buf_right_half_cnt_, MsgBlock::msg_min_size(), m_sender_.m_matched_buf_size_);

  if (__glibc_unlikely(resp_offset == -1U)) {
    // 这里不能直接释放，否则远端会一直轮询此地址而卡住，要直接退回header指针
    m_sender_.m_msg_head_ = current_msg_head;
    dealloc_buf_ptr<uint32_t>(msg_offset, m_sender_.m_msg_buf_left_half_cnt_,
                              m_sender_.m_msg_buf_right_half_cnt_, MAX_MESSAGE_BUFFER_SIZE);
    m_sending_lock_.unlock();
    errno = ENOMEM;
    perror("resp buf full");
    return nullptr;
  }

  // 3. 如果尾部不够，则在尾部插入一个nop操作，让recver跳过尾部再回环轮询
  if (msg_nop_offset != 0) {
    nop_mb = (MsgBlock *)((char *)m_sender_.m_msg_buf_->addr + msg_nop_offset);
    nop_mb->is_buf_last = true;
    nop_mb->size = 0;
    nop_mb->notify = 1;
    nop_mb->set_complete_byte();

    prep_write(m_msg_batch_, (uintptr_t)nop_mb, m_sender_.m_msg_buf_->lkey,
               MsgBlock::msg_min_size(), m_sender_.m_peer_msg_buf_addr_ + msg_nop_offset,
               m_sender_.m_peer_msg_buf_rkey_);
  }

  // 4. 发送msg
  msg_mb = (MsgBlock *)((char *)m_sender_.m_msg_buf_->addr + msg_offset);
  msg_mb->size = param_data_length;

  prep_write(m_msg_batch_, (uintptr_t)msg_mb, m_sender_.m_msg_buf_->lkey, msg_mb->msg_size(),
             m_sender_.m_peer_msg_buf_addr_ + msg_offset, m_sender_.m_peer_msg_buf_rkey_);

  m_send_defer_cnt_.fetch_add(1, std::memory_order_acquire);

  m_sending_lock_.unlock();

  msg_mb->notify = 1;
  msg_mb->resp_offset = resp_offset;
  msg_mb->prep_resp_size = resp_data_length;
  msg_mb->rpc_op = rpc_op;
  msg_mb->is_buf_last = false;
  msg_mb->not_last_end = false;
  msg_mb->set_complete_byte();

  // 5. 清空resp buf
  memset((char *)m_sender_.m_resp_buf_->addr + resp_offset, 0, tmp.msg_size());

  // printf("[%lu] send msg: %p %u, ready resp: %p %u\n", pthread_self(),
  // msg_mb,
  //        msg_mb->msg_size(), (char *)m_sender_.m_resp_buf_->addr + resp_offset,
  //        tmp.msg_size());

  if (!b.m_msg_offsets_.empty()) {
    MsgBlock *prev_msg_mb =
      (MsgBlock *)((char *)m_sender_.m_msg_buf_->addr + b.m_msg_offsets_.back());
    prev_msg_mb->not_last_end = true;
  }

  b.m_msg_offsets_.push_back(msg_offset);
  b.m_resp_mbs_.push_back((MsgBlock *)((char *)m_sender_.m_resp_buf_->addr + resp_offset));

  return msg_mb->data;
}

int RDMAConnection::prep_write(RDMABatch &b, uint64_t local_addr, uint32_t lkey, uint32_t length,
                               uint64_t remote_addr, uint32_t rkey) {
  b.m_sge_wrs_.push_back({ibv_sge{.addr = local_addr, .length = length, .lkey = lkey},
                          ibv_send_wr{.num_sge = 1,
                                      .opcode = IBV_WR_RDMA_WRITE,
                                      .wr = {.rdma = {.remote_addr = remote_addr, .rkey = rkey}}}});

  return 0;
}

int RDMAConnection::prep_read(RDMABatch &b, uint64_t local_addr, uint32_t lkey, uint32_t length,
                              uint64_t remote_addr, uint32_t rkey) {
  b.m_sge_wrs_.push_back({ibv_sge{.addr = local_addr, .length = length, .lkey = lkey},
                          ibv_send_wr{.num_sge = 1,
                                      .opcode = IBV_WR_RDMA_READ,
                                      .wr = {.rdma = {.remote_addr = remote_addr, .rkey = rkey}}}});

  return 0;
}

int RDMAConnection::prep_fetch_add(RDMABatch &b, uint64_t local_addr, uint32_t lkey,
                                   uint64_t remote_addr, uint32_t rkey, uint64_t n) {
  if (__glibc_unlikely(!m_atomic_support_)) {
    errno = EPERM;
    perror("rdma fetch add: this device don't support atomic operations");
    return -1;
  }
  if (__glibc_unlikely(remote_addr % 8)) {
    errno = EINVAL;
    perror("rdma fetch add: remote addr must be 8-byte aligned");
    return -1;
  }

  b.m_sge_wrs_.push_back({ibv_sge{.addr = local_addr, .length = 8, .lkey = lkey},
                          ibv_send_wr{
                            .num_sge = 1,
                            .opcode = IBV_WR_ATOMIC_FETCH_AND_ADD,
                            .wr = {.atomic =
                                     {
                                       .remote_addr = remote_addr,
                                       .compare_add = n,
                                       .rkey = rkey,
                                     }},
                          }});

  return 0;
}

int RDMAConnection::prep_cas(RDMABatch &b, uint64_t local_addr, uint32_t lkey, uint64_t remote_addr,
                             uint32_t rkey, uint64_t expected, uint64_t desired) {
  if (__glibc_unlikely(!m_atomic_support_)) {
    errno = EPERM;
    perror("rdma fetch add: this device don't support atomic operations");
    return -1;
  }
  if (__glibc_unlikely(remote_addr % 8)) {
    errno = EINVAL;
    perror("rdma cas: remote addr must be 8-byte aligned");
    return -1;
  }

  b.m_sge_wrs_.push_back({ibv_sge{.addr = local_addr, .length = 8, .lkey = lkey},
                          ibv_send_wr{
                            .num_sge = 1,
                            .opcode = IBV_WR_ATOMIC_CMP_AND_SWP,
                            .wr = {.atomic =
                                     {
                                       .remote_addr = remote_addr,
                                       .compare_add = expected,
                                       .swap = desired,
                                       .rkey = rkey,
                                     }},
                          }});

  return 0;
}

int RDMAConnection::prep_write_imm(RDMABatch &b, uint64_t local_addr, uint32_t lkey,
                                   uint32_t length, uint32_t imm_data, uint64_t remote_addr,
                                   uint32_t rkey) {
  if (__glibc_unlikely(!m_rpc_conn_)) {
    errno = EPERM;
    perror("non rpc_conn can't write imm");
    return -1;
  }

  b.m_sge_wrs_.push_back({ibv_sge{.addr = local_addr, .length = length, .lkey = lkey},
                          ibv_send_wr{
                            .num_sge = 1,
                            .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
                            .imm_data = imm_data,
                            .wr = {.rdma =
                                     {
                                       .remote_addr = remote_addr,
                                       .rkey = rkey,
                                     }},
                          }});

  return 0;
}

int RDMAConnection::prep_rpc_send(RDMABatch &b, uint8_t rpc_op, const void *param_data,
                                  uint32_t param_data_length, uint32_t resp_data_length) {
  void *msg_buf = prep_rpc_send_defer(b, rpc_op, param_data_length, resp_data_length);

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

RDMAFuture RDMAConnection::submit(RDMABatch &b) {
  RDMAFuture fu;
  std::vector<SgeWr> &sge_wrs = b.m_sge_wrs_;
  static thread_local std::vector<SgeWr> msg_sge_wrs;
  msg_sge_wrs.clear();

  SyncData *sd = alloc_sync_data();
  sd->conn = this;
  sd->resp_poll_idx = 0;
  sd->timeout = false;

  sd->msg_offsets.swap(b.m_msg_offsets_);
  sd->resp_mbs.swap(b.m_resp_mbs_);

  for (size_t i = 0; i < sge_wrs.size(); ++i) {
    sge_wrs[i].wr.sg_list = &sge_wrs[i].sge;
    sge_wrs[i].wr.next = &sge_wrs[i + 1].wr;

    // printf("send %#lx to %#lx\n", sge_wrs[i].sge.addr,
    //        sge_wrs[i].wr.wr.rdma.remote_addr);
  }

  m_sending_lock_.lock();

  // 等待用户的send data复制完成
  // 本质上m_send_defer_cnt_是自旋读写锁
  while (m_send_defer_cnt_.load(std::memory_order_relaxed)) {
    m_sending_lock_.unlock();
    std::this_thread::yield();
    m_sending_lock_.lock();
  }

  msg_sge_wrs.swap(m_msg_batch_.m_sge_wrs_);

  m_sending_lock_.unlock();

  for (size_t i = 0; i < msg_sge_wrs.size(); ++i) {
    msg_sge_wrs[i].wr.sg_list = &msg_sge_wrs[i].sge;
    msg_sge_wrs[i].wr.next = &msg_sge_wrs[i + 1].wr;

    // printf("send msg %#lx to %#lx\n", msg_sge_wrs[i].sge.addr,
    //        msg_sge_wrs[i].wr.wr.rdma.remote_addr);
  }

  ibv_send_wr *wr_head = nullptr;
  if (!sge_wrs.empty()) {
    wr_head = &sge_wrs.front().wr;
    if (!msg_sge_wrs.empty()) {
      sge_wrs.back().wr.next = &msg_sge_wrs.front().wr;
      msg_sge_wrs.back().wr.next = nullptr;
    } else {
      sge_wrs.back().wr.next = nullptr;
    }
  } else if (!msg_sge_wrs.empty()) {
    wr_head = &msg_sge_wrs.front().wr;
    msg_sge_wrs.back().wr.next = nullptr;
  }

  // 相邻的且addr是连续的wr进行组合
  // 一种方式实现rpc msg batch发送
  uint32_t combined_wr_size = 0;
  ibv_send_wr *back_wr;
  if (wr_head) {
    combined_wr_size = 1;
    back_wr = wr_head;
    for (ibv_send_wr *wri = back_wr, *wrj = wri->next; wrj != nullptr; wrj = wrj->next) {
      if ((wrj->opcode == IBV_WR_RDMA_WRITE || wrj->opcode == IBV_WR_RDMA_READ)
          && (wri->opcode == wrj->opcode) && (wri->sg_list->lkey == wrj->sg_list->lkey)
          && (wri->sg_list->addr + wri->sg_list->length == wrj->sg_list->addr)
          && (wri->wr.rdma.rkey == wrj->wr.rdma.rkey)
          && (wri->wr.rdma.remote_addr + wri->sg_list->length == wrj->wr.rdma.remote_addr)) {
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
  fu.m_sd_ = sd;

  if (combined_wr_size == 0) {
    sd->wc_finish = true;
    if (RDMAConnection::RDMA_TIMEOUT_ENABLE)
      sd->now_ms = get_wall_time<std::chrono::milliseconds>();
    return fu;
  }

  sd->wc_finish = false;

  back_wr->wr_id = wr_id;
  back_wr->send_flags = IBV_SEND_SIGNALED;

  if (m_inline_support_) {
    // 较小的msg可以inline到发送包里减少时延
    for (ibv_send_wr *wp = wr_head; wp != nullptr; wp = wp->next) {
      if (wp->opcode == IBV_WR_RDMA_WRITE
          && (wp->sg_list->length <= MSG_INLINE_THRESHOLD || wp->wr.rdma.rkey == 0)) {
        wp->send_flags |= IBV_SEND_INLINE;
      }
    }
  }

  // 探察当前正在发送的wr个数
  uint32_t inflight = m_inflight_count_.load(std::memory_order_acquire);
  while (1) {
    if (__glibc_unlikely((int)(inflight + combined_wr_size) > MAX_SEND_WR)) {
      errno = ENOSPC;
      perror("ibv_post_send too much inflight wr");
      m_poll_conn_sd_wr_();
      inflight = m_inflight_count_.load(std::memory_order_acquire);
      continue;
    }
    //   // printf("inflight+: %u\n", inflight);
    if (m_inflight_count_.compare_exchange_weak(inflight, inflight + combined_wr_size,
                                                std::memory_order_acquire)) {
      break;
    }
  }

  // for (ibv_send_wr *wp = wr_head; wp != nullptr; wp = wp->next) {
  //   printf("send msg %#lx:%u to %#lx\n", wp->sg_list->addr, wp->sg_list->length,
  //          wp->wr.rdma.remote_addr);
  // }

  if (RDMAConnection::RDMA_TIMEOUT_ENABLE)
    sd->now_ms = get_wall_time<std::chrono::milliseconds>();

  struct ibv_send_wr *bad_send_wr;
  if (__glibc_unlikely(ibv_post_send(m_cm_id_->qp, wr_head, &bad_send_wr) != 0)) {
    perror("ibv_post_send fail");
    goto need_retry;
  }

  b.m_sge_wrs_.clear();

  return fu;

need_retry:
  m_sending_lock_.lock();
  if (m_msg_batch_.m_sge_wrs_.empty()) {
    m_msg_batch_.m_sge_wrs_.swap(msg_sge_wrs);
  } else {
    m_msg_batch_.m_sge_wrs_.insert(m_msg_batch_.m_sge_wrs_.end(), msg_sge_wrs.begin(),
                                   msg_sge_wrs.end());
  }
  if (m_msg_batch_.m_msg_offsets_.empty()) {
    m_msg_batch_.m_msg_offsets_.swap(sd->msg_offsets);
    m_msg_batch_.m_resp_mbs_.swap(sd->resp_mbs);
  } else {
    m_msg_batch_.m_msg_offsets_.insert(m_msg_batch_.m_msg_offsets_.end(), sd->msg_offsets.begin(),
                                       sd->msg_offsets.end());
    m_msg_batch_.m_resp_mbs_.insert(m_msg_batch_.m_resp_mbs_.end(), sd->resp_mbs.begin(),
                                    sd->resp_mbs.end());
  }
  m_sending_lock_.unlock();

  dealloc_sync_data(sd);

  fu.m_sd_ = nullptr;
  return fu;
}

void RDMAConnection::m_post_srq_wr(ibv_srq *srq, int count) {
  if (count == 0)
    return;

  static thread_local std::vector<SgeRecvWr> sge_wr;
  sge_wr.resize(count);
  ibv_recv_wr *bad_wr;

  for (int i = 0; i < count; ++i) {
    auto &sge_wr_ = sge_wr[i];

    sge_wr_ = {};
    sge_wr_.wr.sg_list = &sge_wr_.sge;
    sge_wr_.wr.num_sge = 1;
    sge_wr_.wr.next = &sge_wr[i + 1].wr;
  }
  sge_wr.back().wr.next = nullptr;
  if (__glibc_unlikely(ibv_post_srq_recv(srq, &sge_wr.front().wr, &bad_wr)) != 0) {
    perror("ibv_post_srq_recv fail");
    return;
  }
}

int RDMAConnection::m_acknowledge_sd_cqe_(int rc, ibv_wc wcs[]) {
  for (int i = 0; i < rc; ++i) {
    auto &wc = wcs[i];

    SyncData *sd = reinterpret_cast<SyncData *>(wc.wr_id);
    sd->conn->m_inflight_count_.fetch_sub(sd->inflight, std::memory_order_release);

    // printf("inflight-: %u\n", sd->conn->m_inflight_count_.load());

    if (__glibc_unlikely(sd->timeout)) {
      // ! 如果超时了，resp_buf本应该释放，但是remote仍可能发送，并造成污染
      for (size_t j = 0; j < sd->resp_mbs.size(); ++j) {
        auto &sender = sd->conn->m_sender_;
        auto &msg_offset = sd->msg_offsets[j];
        dealloc_buf_ptr<uint32_t>(msg_offset, sender.m_msg_buf_left_half_cnt_,
                                  sender.m_msg_buf_right_half_cnt_, sender.m_matched_buf_size_);
      }
      // 其他线程在轮询超时后，已经不能继续轮询，此时需要将sd删除
      dealloc_sync_data(sd);
    }
    if (__glibc_likely(IBV_WC_SUCCESS == wc.status)) {
      // Break out as operation completed successfully
      if (__glibc_likely(!sd->timeout))
        sd->wc_finish = true;
    } else {
      fprintf(stderr, "cmd_send status error: %s\n", ibv_wc_status_str(wc.status));
      return -1;
    }
  }
  return 0;
}

int RDMAFuture::try_get(std::vector<const void *> &resp_data_ptr) {
  if (__glibc_unlikely(m_sd_->timeout)) {
    errno = ETIMEDOUT;
    perror("rdma task timeout");
    return -1;
  }

  if (__glibc_unlikely(m_sd_->conn->m_poll_conn_sd_wr_() != 0)) {
    return -1;
  }

  // 轮询resp
  if (RDMAConnection::m_try_poll_resp_(m_sd_, resp_data_ptr) == 0 && m_sd_->wc_finish) {
    dealloc_sync_data(m_sd_);
    return 0;
  }

  if (RDMAConnection::RDMA_TIMEOUT_ENABLE) {
    // 超时检测
    uint32_t now = get_wall_time<std::chrono::milliseconds>();
    if (__glibc_unlikely(now - m_sd_->now_ms > RDMAConnection::RDMA_TIMEOUT_MS)) {
      m_sd_->timeout = true;
      errno = ETIMEDOUT;
      perror("rdma task timeout");
      return -1;
    }
  }

  return 1;
}

int RDMAConnection::m_poll_conn_sd_wr_() {
  struct ibv_wc wcs[RDMAConnection::POLL_ENTRY_COUNT];

  int rc = ibv_poll_cq(m_cq_, RDMAConnection::POLL_ENTRY_COUNT, wcs);
  if (__glibc_unlikely(rc < 0)) {
    perror("ibv_poll_cq fail");
    return -1;
  }

  if (__glibc_unlikely(RDMAConnection::m_acknowledge_sd_cqe_(rc, wcs) == -1)) {
    perror("acknowledge_cqe fail");
    return -1;
  }

  return 0;
}

int RDMAConnection::m_try_poll_resp_(SyncData *sd, std::vector<const void *> &resp_data_ptr) {
  for (size_t &poll_idx = sd->resp_poll_idx; poll_idx < sd->resp_mbs.size(); ++poll_idx) {
    auto &msg_offset = sd->msg_offsets[poll_idx];
    auto &resp_mb = sd->resp_mbs[poll_idx];
    if (!resp_mb->valid()) {
      return 1;
    }

    // printf("ack msg: %p %u\n", resp_mb, resp_mb->msg_size());

    if (resp_mb->size > 0) {
      // 0拷贝返回用户resp_data，由用户调用dealloc_resp_data()释放
      resp_data_ptr.push_back(resp_mb->data);
    } else {
      resp_data_ptr.push_back(nullptr);
      sd->conn->dealloc_resp_data(resp_mb->data);
    }
    // 释放msg_buf
    auto &sender = sd->conn->m_sender_;
    dealloc_buf_ptr<uint32_t>(msg_offset, sender.m_msg_buf_left_half_cnt_,
                              sender.m_msg_buf_right_half_cnt_, sender.m_matched_buf_size_);
  }
  return (sd->resp_poll_idx == sd->resp_mbs.size()) ? 0 : 1;
}

int RDMAFuture::get(std::vector<const void *> &resp_data_ptr) {
  resp_data_ptr.reserve(m_sd_->resp_mbs.size());
  while (1) {
    int ret = try_get(resp_data_ptr);
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
  uint32_t offset =
    (uint64_t)data_ptr - (uint64_t)m_sender_.m_resp_buf_->addr - offsetof(MsgBlock, data);
  dealloc_buf_ptr<uint32_t>(offset, m_sender_.m_resp_buf_left_half_cnt_,
                            m_sender_.m_resp_buf_right_half_cnt_, m_sender_.m_matched_buf_size_);
}

void RDMAMsgRTCThread::poll_msg(RDMAConnection *conn) {
  auto &recver = conn->m_recver_;

  // 1. 轮询当前msg_buf的指针指向的mb是否完成
  for (MsgBlock *msg_mb = (MsgBlock *)((char *)recver.m_msg_buf_->addr + recver.m_msg_head_);
       msg_mb->valid();
       msg_mb = (MsgBlock *)((char *)recver.m_msg_buf_->addr + recver.m_msg_head_)) {
    // printf("poll %p\n", msg_mb);

    // 如果是nop，将轮询指针回环到头
    if (msg_mb->is_buf_last) {
      // printf("nop back\n");
      memset(msg_mb, 0, msg_mb->msg_size());
      recver.m_msg_head_ = 0;
      continue;
    }

    ThreadTaskPack new_tp = {};
    new_tp.conn = conn;
    new_tp.uctx = nullptr;
    new_tp.type = ThreadTaskPack::MSG;
    new_tp.msg_mb = msg_mb;
    new_tp.is_not_last = msg_mb->not_last_end;
    m_tps_.emplace_back(new_tp);

    uint16_t msg_size = msg_mb->msg_size();

    // 2. 推进轮询msg_buf指针
    const uint32_t msg_head = recver.m_msg_head_;
    uint32_t msg_head_next;
    if (conn->m_recver_.m_matched_buf_size_ - msg_head - msg_size < MsgBlock::msg_min_size()) {
      // printf("turn back (%u %u)\n", msg_head, msg_size);
      msg_head_next = 0;
    } else {
      msg_head_next = msg_head + msg_size;
    }
    recver.m_msg_head_ = msg_head_next;

    // printf("try poll %p\n",
    //        (char *)recver.m_msg_buf_->addr + recver.m_msg_head_);
  }
}

void RDMAMsgRTCThread::poll_recv_task(RDMAConnection *represent_conn) {
  struct ibv_wc wcs[RDMAConnection::POLL_ENTRY_COUNT];
  int rc = ibv_poll_cq(represent_conn->m_recv_cq_, RDMAConnection::POLL_ENTRY_COUNT, wcs);
  if (__glibc_unlikely(rc < 0)) {
    perror("ibv_poll_cq fail");
    return;
  }

  // srq context 作为 watermark
  (uint64_t &)(represent_conn->m_srq_->srq_context) -= rc;

  for (int i = 0; i < rc; ++i) {
    auto &wc = wcs[i];
    if (__glibc_likely(IBV_WC_SUCCESS == wc.status)) {
      auto conn_it = m_qp_conn_map_.find(wc.qp_num);
      if (conn_it == m_qp_conn_map_.end()) {
        perror("ibv_poll_cq fail");
        return;
      }

      ThreadTaskPack new_tp = {};
      new_tp.conn = conn_it->second;
      new_tp.uctx = nullptr;
      new_tp.type = ThreadTaskPack::WRITE_IMM;
      new_tp.write_imm_data = wc.imm_data;
      new_tp.invalidate_data = (void *)wc.wr_id;
      m_tps_.emplace_back(new_tp);
    } else {
      fprintf(stderr, "cmd_recv status error: %s\n", ibv_wc_status_str(wc.status));
      return;
    }
  }

  represent_conn->m_post_srq_wr(represent_conn->m_srq_, rc);
}