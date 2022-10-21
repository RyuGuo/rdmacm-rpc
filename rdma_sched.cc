#include "rdma_conn.h"
#include <random>

struct task_sync_data_t {
  uint32_t seq = 0;
  uint32_t to_seq = 0;
};
// 重用task_sync_data_t的池
static thread_local std::vector<task_sync_data_t *> tsd_pool;
task_sync_data_t *alloc_task_sync_data() {
  if (tsd_pool.empty()) {
    return new task_sync_data_t();
  } else {
    task_sync_data_t *tsd = tsd_pool.back();
    tsd_pool.pop_back();
    return tsd;
  }
}
void dealloc_task_sync_data(task_sync_data_t *tsd) {
  tsd->seq = 0;
  tsd->to_seq = 0;
  tsd_pool.push_back(tsd);
}

RDMAThreadScheduler::RDMAThreadScheduler()
    : m_rpt_pool_(RDMAConnection::MAX_RECVER_THREAD_COUNT, nullptr) {
  for (uint8_t i = RDMAConnection::MAX_RECVER_THREAD_COUNT; i > 0; --i) {
    m_thread_waiting_pool_.push_back(i - 1);
    m_rpt_pool_[i - 1] = new RDMAMsgRTCThread(i - 1);
  }
}
RDMAThreadScheduler::~RDMAThreadScheduler() {
  for (auto &rpt : m_rpt_pool_) {
    if (rpt) {
      delete rpt;
    }
  }
}

rdma_thread_id_t RDMAThreadScheduler::prepick_one_thread() {
  rdma_thread_id_t tid = m_thread_waiting_pool_.back();
  m_thread_waiting_pool_.pop_back();
  // 如果waiting poll为空，则填充
  if (m_thread_waiting_pool_.empty()) {
    for (uint8_t i = RDMAConnection::MAX_RECVER_THREAD_COUNT; i > 0; --i) {
      m_thread_waiting_pool_.push_back(i - 1);
    }
  }
  return tid;
}
void RDMAThreadScheduler::register_conn_worker(rdma_thread_id_t tid,
                                               RDMAConnection *conn) {
  RDMAMsgRTCThread *&rpt = m_rpt_pool_[tid];
  rpt->join_recver_conn(conn);
}
void RDMAThreadScheduler::unregister_conn_worker(rdma_thread_id_t tid,
                                                 RDMAConnection *conn) {
  m_thread_waiting_pool_.push_back(tid);
  m_rpt_pool_[tid]->exit_recver_conn(conn);
}
void RDMAThreadScheduler::task_dispatch(
    RDMAMsgRTCThread *rpt, std::vector<RDMAMsgRTCThread::ThreadTaskPack> &tps) {
  if (tps.empty())
    return;

  // 设置task的同步序号，以备在submit前进行同步排序
  static thread_local task_sync_data_t *default_tsd = alloc_task_sync_data();
  static thread_local uint32_t *seq_ptr = &default_tsd->seq;
  static thread_local uint32_t *to_seq_ptr = &default_tsd->to_seq;

  for (auto &tp : tps) {
    tp.to_seq_ptr = to_seq_ptr;
    tp.seq = *seq_ptr;
    ++(*seq_ptr);
    if (!tp.msg_mb->not_last_end) {
      task_sync_data_t *tsd = alloc_task_sync_data();
      seq_ptr = &tsd->seq;
      to_seq_ptr = &tsd->to_seq;
    }
  }

  static thread_local std::mt19937_64 rng((uintptr_t)rpt);

  // printf("dispatch %luth task to thread %d\n", 0lu, rpt->m_th_id_);

  // 这里将第一个task放在当前线程中执行
  rpt->m_task_queue_lck_.lock();
  rpt->m_task_queue_.emplace(tps.front());
  rpt->m_task_queue_lck_.unlock();

  int i = 0;
  uint64_t ur = 0;
  for (size_t j = 1; j < tps.size(); ++j) {
    if (!(i & (sizeof(ur) / sizeof(rdma_thread_id_t) - 1)))
      ur = rng();

    RDMAMsgRTCThread *disp_recver =
        m_rpt_pool_[(uint16_t)ur % RDMAConnection::MAX_RECVER_THREAD_COUNT];

    // printf("dispatch %luth task to thread %d\n", j, disp_recver->m_th_id_);

    disp_recver->m_task_queue_lck_.lock();
    disp_recver->m_task_queue_.emplace(tps[j]);
    disp_recver->m_task_queue_lck_.unlock();

    ur >>= sizeof(rdma_thread_id_t) * 8;
    ++i;
  }
}

void RDMAThreadScheduler::flag_task_done(RDMAMsgRTCThread::ThreadTaskPack &tp) {
  dealloc_task_sync_data(
      (task_sync_data_t *)((uint64_t)tp.to_seq_ptr -
                           offsetof(task_sync_data_t, to_seq)));
}