#ifndef __RDMA_CONN_H__
#define __RDMA_CONN_H__

#include <atomic>
#include <cstdint>
#include <functional>
#include <infiniband/sa.h>
#include <infiniband/verbs.h>
#include <map>
#include <mutex>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <sstream>
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
  uint16_t resp_offset;
  uint8_t rpc_op;
  bool last_end;
  volatile uint8_t notify;
  union {
    uint8_t __padding__; // 如果size=0，作为完成标记
    char data[0];
  };

  static uint32_t msg_min_size() { return sizeof(MsgBlock); }
  uint32_t msg_size() const { return sizeof(MsgBlock) + size; }
  void set_complete_byte() { data[size] = 1; }
  bool valid() const {
    return notify && __atomic_load_n(&data[size], __ATOMIC_RELAXED) == 1;
  }
};

struct MsgQueueHandle {
  std::vector<ibv_sge> sges;
  std::vector<ibv_send_wr> send_wrs;
  std::vector<uint32_t> msg_offsets;
  std::vector<MsgBlock *> resp_mbs;
};

struct RDMAConnection {
  RDMAConnection(bool rpc_conn = true);
  ~RDMAConnection();

  constexpr static int MAX_SEND_WR = 16;
  constexpr static int MAX_RECV_WR = 1;
  constexpr static int MAX_SEND_SGE = 1;
  constexpr static int MAX_RECV_SGE = 1;
  constexpr static int CQE_NUM = 2;
  constexpr static int RESOLVE_TIMEOUT_MS = 2000;
  constexpr static uint8_t RETRY_COUNT = 7;
  constexpr static int RNR_RETRY_COUNT = 7;
  constexpr static uint8_t INITIATOR_DEPTH = 2;
  constexpr static int RESPONDER_RESOURCES = 2;
  constexpr static int POLL_ENTRY_COUNT = 2;
  constexpr static uint32_t RDMA_TIMEOUT_MS = 2000;
  constexpr static size_t MAX_MESSAGE_BUFFER_SIZE = 4096;
  constexpr static uint32_t MSG_INLINE_THRESHOLD = 64;

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
   * @return task_id:
   *  * ≠0 - ok
   *  *  0 - retry
   */
  uint64_t submit(MsgQueueHandle &qh);

  /**
   * @return
   *     0 - ok
   *     1 - pending
   *    -1 - error
   */
  int remote_task_try_get(uint64_t task_id,
                          std::vector<const void *> &resp_data_ptr);
  int remote_task_wait(uint64_t task_id,
                       std::vector<const void *> &resp_data_ptr);
  void dealloc_resp_data(const void *data_ptr);

  static std::unordered_map<
      uint8_t,
      std::pair<std::function<void(RDMAConnection *conn, const void *msg_data,
                                   uint32_t size, void *resp_data)>,
                uint32_t>>
      m_rpc_exec_map_;
  static void register_rpc_func(
      uint8_t rpc_op,
      std::function<void(RDMAConnection *conn, const void *msg_data,
                         uint32_t size, void *resp_data)> &&rpc_func,
      uint32_t resp_max_size);

  volatile bool m_stop_;
  bool m_rpc_conn_;
  bool m_atomic_support_;
  bool m_inline_support_;
  rdma_cm_id *m_cm_id_;
  ibv_pd *m_pd_;
  ibv_cq *m_cq_;
  std::thread *m_conn_handler_;
  std::vector<RDMAConnection *> m_srv_conns_;
  std::atomic<uint32_t> m_inflight_count_ = {0};

  pthread_spinlock_t m_sending_lock_;
  MsgQueueHandle m_msg_qh_;
  std::atomic<uint32_t> m_send_defer_cnt_;

  struct sync_data_t {
    volatile bool wc_finish;
    bool timeout;
    uint32_t inflight;
    uint32_t now_ms;
    size_t resp_poll_idx;
    std::vector<uint32_t> msg_offsets;
    std::vector<MsgBlock *> resp_mbs;
    std::vector<const void *> resp_data_ptr;
  };

  struct conn_param_t {
    uint64_t addr;
    uint32_t rkey;
    bool rpc_conn;
  };

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

      std::atomic<uint32_t> m_resp_buf_left_half_cnt_;
      std::atomic<uint32_t> m_resp_buf_right_half_cnt_;

      std::thread *m_msg_recv_worker_;
    } recver;
  };

  bool rdma_conn_param_valid();
  void handle_connection();
  void create_connection();
  void msg_recv_work();
  int acknowledge_cqe(int rc, ibv_wc wcs[]);
  int try_poll_resp(sync_data_t *sd);
  sync_data_t *alloc_sync_data();
  void dealloc_sync_data(sync_data_t *sd);
};

#endif // __RDMA_CONN_H__