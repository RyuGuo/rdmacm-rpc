#ifndef __RDMA_CONN_H__
#define __RDMA_CONN_H__

#include <atomic>
#include <cstdint>
#include <functional>
#include <infiniband/verbs.h>
#include <map>
#include <queue>
#include <rdma/rdma_cma.h>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// rdma_event_channel 对应一个 epoll event channel
// rdma_id -->  ctx  --> cq
//               L-----> pd  --> mr
//                        L----> qp
// 一个rdma_id对应一个qp

using rdma_thread_id_t = uint8_t;

struct RDMASpinLock {
  pthread_spinlock_t spin_lck_;
  RDMASpinLock() { pthread_spin_init(&spin_lck_, PTHREAD_PROCESS_PRIVATE); }
  ~RDMASpinLock() { pthread_spin_destroy(&spin_lck_); }
  int lock() { return pthread_spin_lock(&spin_lck_); }
  int unlock() { return pthread_spin_unlock(&spin_lck_); }
};

struct RDMAEnv {
  RDMAEnv(const RDMAEnv &) = delete;
  RDMAEnv(RDMAEnv &&) = delete;
  RDMAEnv &operator=(const RDMAEnv &) = delete;
  RDMAEnv &operator=(RDMAEnv &&) = delete;

  static int init();
  static RDMAEnv &get_instance() {
    static RDMAEnv env;
    return env;
  }

  bool m_active_;
  rdma_event_channel *m_cm_channel_;
  ibv_context **m_ibv_ctxs_;
  int m_nr_dev_;

  std::map<ibv_context *, ibv_pd *> m_pd_map_;

  int __init__();

private:
  RDMAEnv() : m_active_(false) {}
  ~RDMAEnv();
};

struct MsgBlock {
  uint32_t size;
  uint32_t prep_resp_size;
  uint32_t resp_offset;
  uint8_t rpc_op;
  bool not_last_end;
  volatile uint8_t notify;
  union {
    uint8_t __padding__[1]; // 如果size=0，作为完成标记
    char data[0];
  };

  static uint32_t msg_min_size() { return sizeof(MsgBlock); }
  uint32_t msg_size() const { return sizeof(MsgBlock) + size; }
  void set_complete_byte() { data[size] = 1; }
  bool valid() const {
    return notify && __atomic_load_n(&data[size], __ATOMIC_RELAXED) == 1;
  }
};

struct CQHandle {
  std::map<ibv_context *, ibv_cq *> cq_map_;
  RDMASpinLock cq_lck_;
  CQHandle() {}
  ~CQHandle();
};

struct MsgQueueHandle {
  std::vector<ibv_sge> sges;
  std::vector<ibv_send_wr> send_wrs;
  std::vector<uint32_t> msg_offsets;
  std::vector<MsgBlock *> resp_mbs;
};

struct sync_data_t;

struct RDMAFuture {
  sync_data_t *sd;

  int get(std::vector<const void *> &resp_data_ptr);
  /**
   * @return
   *  *  0 - ok
   *  *  1 - pending
   *  * -1 - error
   */
  int try_get(std::vector<const void *> &resp_data_ptr);
};

struct RDMAConnection {
  // Global Options
  static int MAX_SEND_WR;
  static int MAX_RECV_WR;
  static int MAX_SEND_SGE;
  static int MAX_RECV_SGE;
  static int CQE_NUM;
  static int RESOLVE_TIMEOUT_MS;
  static uint8_t RETRY_COUNT;
  static int RNR_RETRY_COUNT;
  static uint8_t INITIATOR_DEPTH;
  static int RESPONDER_RESOURCES;
  static int POLL_ENTRY_COUNT;
  static uint32_t RDMA_TIMEOUT_MS;
  static uint32_t MAX_MESSAGE_BUFFER_SIZE;
  static uint32_t MSG_INLINE_THRESHOLD;
  static uint8_t MAX_RECVER_THREAD_COUNT;
  static std::vector<int16_t> VEC_RECVER_THREAD_BIND_CORE;

  RDMAConnection(CQHandle *cq_handle = nullptr, bool rpc_conn = true);
  ~RDMAConnection();

  int listen(const std::string &ip, uint16_t port);
  int connect(const std::string &ip, uint16_t port);

  ibv_mr *register_memory(void *ptr, size_t size);
  ibv_mr *register_memory(size_t size);

  // prep 操作对于同一个 qh 均为 thread-unsafety

  int prep_write(MsgQueueHandle &qh, uint64_t local_addr, uint32_t lkey,
                 uint32_t length, uint64_t remote_addr, uint32_t rkey);
  int prep_read(MsgQueueHandle &qh, uint64_t local_addr, uint32_t lkey,
                uint32_t length, uint64_t remote_addr, uint32_t rkey);
  int prep_fetch_add(MsgQueueHandle &qh, uint64_t local_addr, uint32_t lkey,
                     uint64_t remote_addr, uint32_t rkey, uint64_t n);
  int prep_cas(MsgQueueHandle &qh, uint64_t local_addr, uint32_t lkey,
               uint64_t remote_addr, uint32_t rkey, uint64_t expected,
               uint64_t desired);
  int prep_rpc_send(MsgQueueHandle &qh, uint8_t rpc_op, const void *param_data,
                    uint32_t param_data_length, uint32_t resp_data_length);
  /**
   * 准备rpc发送，返回消息buffer指针
   * 在remote_task轮询成功时自动回收
   * @warning 调用prep_rpc_send_confirm()以确认完成数据拷贝
   */
  void *prep_rpc_send_defer(MsgQueueHandle &qh, uint8_t rpc_op,
                            uint32_t param_data_length,
                            uint32_t resp_data_length);
  void prep_rpc_send_confirm();

  /**
   * 提交prep队列
   *
   * @warning
   *  * 该操作成功后会清空qh
   */
  RDMAFuture submit(MsgQueueHandle &qh);

  void dealloc_resp_data(const void *data_ptr);

  /**
   * @param rpc_op 调用操作符
   * @param rpc_func RPC处理函数
   *  * @param conn 当前连接
   *  * @param msg_data 消息
   *  * @param length 消息大小
   *  * @param resp_data 数据返回缓冲区
   *  * @return 数据返回大小
   */
  static void register_rpc_func(
      uint8_t rpc_op,
      std::function<uint32_t(RDMAConnection *conn, const void *msg_data,
                         uint32_t length, void *resp_data, uint32_t max_resp_data_length)> &&rpc_func);

  static std::unordered_map<
      uint8_t, std::function<uint32_t(RDMAConnection *conn, const void *msg_data,
                                  uint32_t length, void *resp_data, uint32_t max_resp_data_length)>>
      m_rpc_exec_map_;

  static RDMASpinLock m_core_bind_lock_;

  volatile bool m_stop_;
  bool m_rpc_conn_;
  bool m_atomic_support_;
  bool m_inline_support_;
  ibv_comp_channel *m_comp_chan_;
  rdma_cm_id *m_cm_id_;
  ibv_pd *m_pd_;
  ibv_cq *m_cq_;
  CQHandle *m_cq_handle_;
  std::thread *m_conn_handler_;
  std::atomic<uint32_t> m_inflight_count_ = {0};

  RDMASpinLock m_sending_lock_;
  MsgQueueHandle m_msg_qh_;
  std::atomic<uint32_t> m_send_defer_cnt_;

  union {
    //    sender                   // recver
    //   m_msg_buf_                  m_msg_buf_
    //  [[ msg ]    ]  --- W --->  [[ msg ]    ]
    //   ^                          ^
    //  m_msg_head_              m_msg_head_(poll here)
    //
    //   m_resp_buf_                m_resp_buf_
    //  [[ resp ]   ]  <-- W ---   [[ resp ]   ]
    //   ^                          ^
    //  m_resp_head_(poll here)  m_resp_head_

    struct {
      uint32_t m_msg_head_;
      uint32_t m_resp_head_;
      ibv_mr *m_msg_buf_;
      ibv_mr *m_resp_buf_;

      uint64_t m_peer_msg_buf_addr_;
      uint32_t m_peer_msg_buf_rkey_;

      std::atomic<uint32_t> m_msg_buf_left_half_cnt_;
      std::atomic<uint32_t> m_msg_buf_right_half_cnt_;

      std::atomic<uint32_t> m_resp_buf_left_half_cnt_;
      std::atomic<uint32_t> m_resp_buf_right_half_cnt_;
    } sender;
    struct {
      uint32_t m_msg_head_;
      uint32_t m_resp_head_;
      ibv_mr *m_msg_buf_;
      ibv_mr *m_resp_buf_;

      uint64_t m_peer_resp_buf_addr_;
      uint32_t m_peer_resp_buf_rkey_;
    } recver;
  };

  bool rdma_conn_param_valid();
  int create_ibv_connection();
  void handle_connection();
  void create_connection();
  void msg_recv_work();
  static int acknowledge_cqe(int rc, ibv_wc wcs[]);
  static int try_poll_resp(sync_data_t *sd);
};

struct RDMAMsgRTCThread {
  struct ConnContext {
    MsgQueueHandle recver_qh;
    std::deque<RDMAFuture> hdls;
    std::vector<const void *> resp_tmp;
    volatile uint64_t th_seq;
    uint64_t seq_max;
  };

  struct ThreadTaskPack {
    RDMAConnection *conn;
    RDMAMsgRTCThread::ConnContext *ctx;
    MsgBlock *msg_mb;
    uint64_t seq;
  };

  volatile bool m_stop_;
  rdma_thread_id_t m_th_id_;
  int16_t m_core_id_;
  RDMASpinLock m_set_lck_;
  std::thread m_th_;
  std::vector<std::pair<RDMAConnection *, ConnContext>> m_conn_set_;
  RDMASpinLock m_task_queue_lck_;
  std::queue<ThreadTaskPack> m_task_queue_;

  RDMAMsgRTCThread(rdma_thread_id_t tid);
  ~RDMAMsgRTCThread();
  void join_recver_conn(RDMAConnection *conn);
  void exit_recver_conn(RDMAConnection *conn);
  void thread_routine();
  void core_bind();
};

struct RDMAThreadScheduler {
  static RDMAThreadScheduler &get_instance() {
    static RDMAThreadScheduler ts;
    return ts;
  }

  rdma_thread_id_t prepick_one_thread();
  void register_conn_worker(rdma_thread_id_t tid, RDMAConnection *conn);
  void unregister_conn_worker(rdma_thread_id_t tid, RDMAConnection *conn);
  void task_dispatch(RDMAMsgRTCThread *rpt,
                     std::vector<RDMAMsgRTCThread::ThreadTaskPack> &tps);

  std::vector<RDMAMsgRTCThread *> m_rpt_pool_;
  // 等待加入的线程队列
  std::vector<rdma_thread_id_t> m_thread_waiting_pool_;

  RDMAThreadScheduler();
  ~RDMAThreadScheduler();
};

#endif // __RDMA_CONN_H__