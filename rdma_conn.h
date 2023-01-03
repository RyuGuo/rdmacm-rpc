#ifndef __RDMA_CONN_H__
#define __RDMA_CONN_H__

#include <atomic>
#include <functional>
#include <infiniband/verbs.h>
#include <map>
#include <queue>
#include <rdma/rdma_cma.h>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include "moodycamel.h"

// rdma_event_channel 对应一个 epoll event channel
// rdma_id -->  ctx  --> cq
//               L-----> pd  --> mr
//                        L----> qp
// 一个rdma_id对应一个qp

class SpinLock {
public:
  SpinLock() { pthread_spin_init(&spin_lck_, PTHREAD_PROCESS_PRIVATE); }
  ~SpinLock() { pthread_spin_destroy(&spin_lck_); }
  int lock() { return pthread_spin_lock(&spin_lck_); }
  int unlock() { return pthread_spin_unlock(&spin_lck_); }

private:
  pthread_spinlock_t spin_lck_;
};

class RDMAEnv {
public:
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

private:
  RDMAEnv() : m_active_(false) {}
  ~RDMAEnv();
  int __init__();
};

struct MsgBlock;
struct SyncData;

struct SgeWr {
  ibv_sge sge;
  ibv_send_wr wr;
};

struct CQHandle {
  std::map<ibv_context *, ibv_cq *> m_cq_map_;
  SpinLock m_cq_lck_;
  ~CQHandle();
};

struct SRQHandle;

struct RDMABatch {
  std::vector<SgeWr> m_sge_wrs_;
  std::vector<uint32_t> m_msg_offsets_;
  std::vector<MsgBlock *> m_resp_mbs_;
};

struct RDMAFuture {
  int get(std::vector<const void *> &resp_data_ptr);
  /**
   * @return
   *  *  0 - ok
   *  *  1 - pending
   *  * -1 - error
   */
  int try_get(std::vector<const void *> &resp_data_ptr);

  SyncData *m_sd_;
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
  static uint32_t MAX_MESSAGE_BUFFER_SIZE;
  static uint32_t MSG_INLINE_THRESHOLD;
  static uint8_t MAX_RECVER_THREAD_COUNT;
  static uint32_t MAX_SRQ_WR;
  static std::vector<int16_t> VEC_RECVER_THREAD_BIND_CORE;
  static bool RDMA_TIMEOUT_ENABLE;
  static uint32_t RDMA_TIMEOUT_MS;

  RDMAConnection(CQHandle *cq_handle = nullptr, bool rpc_conn = true);
  ~RDMAConnection();

  int listen(const std::string &ip, uint16_t port);
  int connect(const std::string &ip, uint16_t port);

  std::pair<std::string, in_port_t> get_local_addr();
  std::pair<std::string, in_port_t> get_peer_addr();

  ibv_mr *register_memory(void *ptr, size_t size);
  ibv_mr *register_memory(size_t size);
  ibv_mr *register_device_memory(size_t size);
  void deregister_memory(ibv_mr* mr, bool freed = true);

  // prep 操作对于同一个 batch 均为 thread-unsafety

  int prep_write(RDMABatch &b, uint64_t local_addr, uint32_t lkey, uint32_t length,
                 uint64_t remote_addr, uint32_t rkey);
  int prep_read(RDMABatch &b, uint64_t local_addr, uint32_t lkey, uint32_t length,
                uint64_t remote_addr, uint32_t rkey);
  int prep_fetch_add(RDMABatch &b, uint64_t local_addr, uint32_t lkey, uint64_t remote_addr,
                     uint32_t rkey, uint64_t n);
  int prep_cas(RDMABatch &b, uint64_t local_addr, uint32_t lkey, uint64_t remote_addr,
               uint32_t rkey, uint64_t expected, uint64_t desired);
  int prep_write_imm(RDMABatch &b, uint64_t local_addr, uint32_t lkey, uint32_t length,
                     uint32_t imm_data, uint64_t remote_addr, uint32_t rkey);
  int prep_rpc_send(RDMABatch &b, uint8_t rpc_op, const void *param_data,
                    uint32_t param_data_length, uint32_t resp_data_length);
  /**
   * 准备rpc发送，返回消息buffer指针
   * 在remote_task轮询成功时自动回收
   * @warning 调用prep_rpc_send_confirm()以确认完成数据拷贝
   */
  void *prep_rpc_send_defer(RDMABatch &b, uint8_t rpc_op, uint32_t param_data_length,
                            uint32_t resp_data_length);
  void prep_rpc_send_confirm(void *p);

  /**
   * 提交prep队列
   *
   * @warning
   *  * 该操作成功后会清空batch
   */
  RDMAFuture submit(RDMABatch &b);

  void dealloc_resp_data(const void *data_ptr);

  static const uint32_t UCTX_YIELD = -1u;

  /**
   * @param rpc_op 调用操作符
   * @param rpc_func RPC处理函数
   *  * @param conn 当前连接
   *  * @param msg_data 消息
   *  * @param length 消息大小
   *  * @param resp_data 数据返回缓冲区
   *  * @param uctx
   * 用户协程，该字段可实现协程状态暂存，需要切换协程时返回`UCTX_YIELD`（用户自行释放）
   *  * @return 数据返回大小
   */
  static void register_rpc_func(
    uint16_t rpc_op,
    std::function<uint32_t(RDMAConnection *conn, void *msg_data, uint32_t length, void *resp_data,
                           uint32_t max_resp_data_length, void **uctx)> &&rpc_func);

  static void register_rdma_write_with_imm_handle(
    std::function<uint32_t(RDMAConnection *conn, uint32_t write_imm_data, void **uctx)>
      &&write_with_imm_handle);

  static void register_connect_hook(std::function<void(RDMAConnection *conn)> &&hook_connect);
  static void register_disconnect_hook(std::function<void(RDMAConnection *conn)> &&hook_disconnect);

  static std::unordered_map<
    uint16_t, std::function<uint32_t(RDMAConnection *conn, void *msg_data, uint32_t length,
                                     void *resp_data, uint32_t max_resp_data_length, void **uctx)>>
    m_rpc_handle_map_;

  static std::function<uint32_t(RDMAConnection *conn, uint32_t write_imm_data, void **uctx)>
    m_write_with_imm_handle_;

  static std::function<void(RDMAConnection *conn)> m_hook_connect_;
  static std::function<void(RDMAConnection *conn)> m_hook_disconnect_;

  static SpinLock m_core_bind_lock_;

  enum conn_type_t {
    SENDER,
    RECVER,
    LISTENER,
  };

  uint8_t conn_type;
  volatile bool m_stop_ : 1;
  bool m_rpc_conn_ : 1;
  bool m_atomic_support_ : 1;
  bool m_inline_support_ : 1;
  std::atomic<uint32_t> m_inflight_count_ = {0}; // 与 m_sending_lock_ 间隔 >64B
  ibv_comp_channel *m_comp_chan_;
  rdma_cm_id *m_cm_id_;
  ibv_pd *m_pd_;
  ibv_cq *m_cq_;
  ibv_cq *m_recv_cq_;
  ibv_srq *m_srq_;
  ibv_dm *m_dm_;
  CQHandle *m_cq_handle_;
  CQHandle *m_recv_cq_handle_;
  SRQHandle *m_srq_handle_;
  void *context;

  SpinLock m_sending_lock_;
  moodycamel::ConcurrentQueue<SgeWr> m_msg_batch_;

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
      // copied by m_msg_buf_, use `new` and `delete` to manage life cycle
      ibv_mr *m_resp_buf_;

      uint64_t m_peer_msg_buf_addr_;
      uint32_t m_peer_msg_buf_rkey_;

      uint32_t m_matched_buf_size_;

      std::atomic<uint32_t> m_msg_buf_left_half_cnt_;
      std::atomic<uint32_t> m_msg_buf_right_half_cnt_;

      std::atomic<uint32_t> m_resp_buf_left_half_cnt_;
      std::atomic<uint32_t> m_resp_buf_right_half_cnt_;
    } m_sender_;
    struct {
      uint32_t m_msg_head_;
      uint32_t m_resp_head_;
      ibv_mr *m_msg_buf_;
      // copied by m_msg_buf_, use `new` and `delete` to manage life cycle
      ibv_mr *m_resp_buf_;

      uint64_t m_peer_resp_buf_addr_;
      uint32_t m_peer_resp_buf_rkey_;

      uint32_t m_matched_buf_size_;
    } m_recver_;
    struct {
      std::thread *m_conn_handler_;
    };
  };

  bool m_rdma_conn_param_valid_();
  int m_init_ibv_connection_();
  void m_handle_connection_();
  int m_poll_conn_sd_wr_();
  void m_init_connection_(RDMAConnection *init_conn);
  uint32_t m_msg_handle_(RDMAConnection *self, MsgBlock *msg_mb, void **uctx);
  static int m_acknowledge_sd_cqe_(int rc, ibv_wc wcs[]);
  static int m_try_poll_resp_(SyncData *sd, std::vector<const void *> &resp_data_ptr);
  static void m_post_srq_wr_(ibv_srq *srq, int count);
  int m_push_msg_wr_(uint64_t local_addr, uint32_t lkey, uint32_t length, uint64_t remote_addr,
                  uint32_t rkey);
};

#endif // __RDMA_CONN_H__