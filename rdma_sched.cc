#include "rdma_conn.h"
#include <random>

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

  // 这里将第一个和最后一个放在当前线程中执行
  // 以在最后一个task处理时尽可能保留有效cache到下一请求

  static thread_local std::mt19937_64 rng((uintptr_t)rpt);

  // printf("dispatch %luth task to thread %d\n", 0lu, rpt->m_th_id_);

  rpt->m_task_queue_lck_.lock();
  rpt->m_task_queue_.emplace(tps.front());
  rpt->m_task_queue_lck_.unlock();

  int i = 0;
  uint64_t ur;
  for (size_t j = 1; j < tps.size() - 1; ++j) {
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

  if (tps.size() > 1) {
    // printf("dispatch %luth task to thread %d\n", tps.size() - 1,
    // rpt->m_th_id_);

    rpt->m_task_queue_lck_.lock();
    rpt->m_task_queue_.emplace(tps.back());
    rpt->m_task_queue_lck_.unlock();
  }
}