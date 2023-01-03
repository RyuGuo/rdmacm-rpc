#ifndef _COMMON_H_
#define _COMMON_H_

#include "../rdma_conn.h"

using rdma_thread_id_t = uint8_t;

#define UNLIKELY __glibc_unlikely
#define LIKELY __glibc_likely

#ifdef NDEBUG
#define DEBUGY(cond) if (false)
#else
#define DEBUGY(cond) if (UNLIKELY(cond))
#endif // NDEBUG

struct SRQHandle {
  std::map<ibv_context *, ibv_srq *> m_srq_map_;
  SpinLock m_srq_lck_;
  ~SRQHandle();
};

struct SgeRecvWr {
  ibv_sge sge;
  ibv_recv_wr wr;
};

struct RDMAMsgRTCThread {
  struct ThreadTaskPack {
    enum type_t {
      MSG,
      WRITE_IMM,
    };
    RDMAConnection *conn;
    void *uctx;
    int type;
    union {
      struct {
        MsgBlock *msg_mb;
        bool is_not_last;
        uint32_t seq;
        uint32_t *to_seq_ptr;
      };
      struct {
        uint32_t write_imm_data;
      };
    };
  };

  volatile bool m_stop_;
  rdma_thread_id_t m_th_id_;
  int16_t m_core_id_;
  SpinLock m_set_lck_;
  std::thread m_th_;
  std::vector<RDMAConnection *> m_conn_set_;
  moodycamel::ConcurrentQueue<ThreadTaskPack> m_task_queue_;
  std::unordered_map<uint32_t, RDMAConnection *> m_qp_conn_map_;

  std::vector<ThreadTaskPack> m_tps_;
  std::vector<ThreadTaskPack> m_uctx_tps_;
  std::deque<ThreadTaskPack> m_delay_submit_tps_;

  RDMAMsgRTCThread(rdma_thread_id_t tid);
  ~RDMAMsgRTCThread();
  void join_recver_conn(RDMAConnection *conn);
  void exit_recver_conn(RDMAConnection *conn);
  void thread_routine();
  void core_bind();
  void poll_msg(RDMAConnection *conn);
  void poll_recv_task(RDMAConnection *represent_conn);
  void do_task(ThreadTaskPack &tp);
};

class RDMAThreadScheduler {
public:
  static RDMAThreadScheduler &get_instance() {
    static RDMAThreadScheduler ts;
    return ts;
  }

  RDMAThreadScheduler(const RDMAThreadScheduler &) = delete;
  RDMAThreadScheduler(RDMAThreadScheduler &&) = delete;
  RDMAThreadScheduler &operator=(const RDMAThreadScheduler &) = delete;
  RDMAThreadScheduler &operator=(RDMAThreadScheduler &&) = delete;

  rdma_thread_id_t prepick_one_thread();
  void register_conn_worker(rdma_thread_id_t tid, RDMAConnection *conn);
  void unregister_conn_worker(rdma_thread_id_t tid, RDMAConnection *conn);
  void task_dispatch(RDMAMsgRTCThread *rpt, std::vector<RDMAMsgRTCThread::ThreadTaskPack> &tps);
  void flag_task_done(RDMAMsgRTCThread::ThreadTaskPack &tp);

private:
  std::vector<RDMAMsgRTCThread *> m_rpt_pool_;
  // 等待加入的线程队列
  std::vector<rdma_thread_id_t> m_thread_waiting_pool_;

  RDMAThreadScheduler();
  ~RDMAThreadScheduler();
};

#endif // _COMMON_H_