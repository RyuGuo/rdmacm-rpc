#include "rdma_conn.h"
#include <arpa/inet.h>
#include <netdb.h>
#include <set>

struct conn_param_t {
  uint64_t addr;
  uint32_t rkey;
  bool rpc_conn;
};

int RDMAConnection::MAX_SEND_WR = 32;
int RDMAConnection::MAX_RECV_WR = 1;
int RDMAConnection::MAX_SEND_SGE = 1;
int RDMAConnection::MAX_RECV_SGE = 1;
int RDMAConnection::CQE_NUM = 32;
int RDMAConnection::RESOLVE_TIMEOUT_MS = 2000;
uint8_t RDMAConnection::RETRY_COUNT = 7;
int RDMAConnection::RNR_RETRY_COUNT = 7;
uint8_t RDMAConnection::INITIATOR_DEPTH = 2;
int RDMAConnection::RESPONDER_RESOURCES = 2;
int RDMAConnection::POLL_ENTRY_COUNT = 2;
uint32_t RDMAConnection::RDMA_TIMEOUT_MS = 2000;
size_t RDMAConnection::MAX_MESSAGE_BUFFER_SIZE = 4096;
uint32_t RDMAConnection::MSG_INLINE_THRESHOLD = 64;
uint8_t RDMAConnection::MAX_RECVER_THREAD_COUNT = 16;

std::vector<int16_t> RDMAConnection::VEC_RECVER_THREAD_BIND_CORE;

bool RDMAConnection::rdma_conn_param_valid() {
  ibv_device_attr device_attr;
  if (ibv_query_device(m_cm_id_->verbs, &device_attr) != 0) {
    perror("ibv_query_device fail");
    return false;
  }
  m_atomic_support_ = device_attr.atomic_cap != IBV_ATOMIC_NONE;
  m_inline_support_ =
      m_cm_id_->verbs->device->transport_type != IBV_TRANSPORT_UNKNOWN;
  return device_attr.max_cqe >= CQE_NUM &&
         device_attr.max_qp_wr >= MAX_SEND_WR &&
         device_attr.max_qp_wr >= MAX_RECV_WR &&
         device_attr.max_sge >= MAX_SEND_SGE &&
         device_attr.max_sge >= MAX_RECV_SGE &&
         device_attr.max_qp_rd_atom >= RESPONDER_RESOURCES &&
         device_attr.max_qp_init_rd_atom >= RESPONDER_RESOURCES &&
         device_attr.max_qp_rd_atom >= INITIATOR_DEPTH &&
         device_attr.max_qp_init_rd_atom >= INITIATOR_DEPTH;
}

int RDMAEnv::init() { return get_instance().__init__(); }

int RDMAEnv::__init__() {
  m_cm_channel_ = rdma_create_event_channel();
  if (!m_cm_channel_) {
    perror("rdma_create_event_channel fail");
    return -1;
  }

  m_ibv_ctxs_ = rdma_get_devices(&m_nr_dev_);
  if (!m_ibv_ctxs_) {
    perror("rdma_get_devices fail");
    return -1;
  }

  for (int i = 0; i < m_nr_dev_; ++i) {
    ibv_pd *pd = ibv_alloc_pd(m_ibv_ctxs_[i]);
    if (!pd) {
      perror("ibv_alloc_pd fail");
      return -1;
    }
    m_pd_map_.emplace(m_ibv_ctxs_[i], pd);
  }

  m_active_ = true;

  return 0;
}

RDMAEnv::~RDMAEnv() {
  for (auto &pd : m_pd_map_) {
    ibv_dealloc_pd(pd.second);
  }
  rdma_free_devices(m_ibv_ctxs_);
  rdma_destroy_event_channel(m_cm_channel_);
}

CQHandle::~CQHandle() {
  for (auto &p : cq_map_) {
    ibv_destroy_cq(p.second);
  }
}

RDMAConnection::RDMAConnection(CQHandle *cq_handle, bool rpc_conn)
    : m_stop_(false), m_rpc_conn_(rpc_conn), m_cm_id_(nullptr), m_pd_(nullptr),
      m_cq_handle_(cq_handle), m_conn_handler_(nullptr), m_send_defer_cnt_(0) {}
RDMAConnection::~RDMAConnection() {
  m_stop_ = true;
  if (m_conn_handler_) {
    m_conn_handler_->join();
  } else {
    rdma_disconnect(m_cm_id_);
    rdma_destroy_qp(m_cm_id_);
    if (!m_cq_handle_) {
      ibv_destroy_cq(m_cq_);
    }
    ibv_destroy_comp_channel(m_comp_chan_);
    ibv_dereg_mr(sender.m_msg_buf_);
    ibv_dereg_mr(sender.m_resp_buf_);
    free(sender.m_msg_buf_->addr);
    free(sender.m_resp_buf_->addr);
  }
  rdma_destroy_id(m_cm_id_);
}

int RDMAConnection::create_ibv_connection() {
  if (!rdma_conn_param_valid()) {
    perror("rdma_conn_param_valid fail");
    return -1;
  }

  m_pd_ = RDMAEnv::get_instance().m_pd_map_[m_cm_id_->verbs];
  if (!m_pd_) {
    perror("ibv_alloc_pd fail");
    return -1;
  }

  m_comp_chan_ = ibv_create_comp_channel(m_cm_id_->verbs);
  if (!m_comp_chan_) {
    perror("ibv_create_comp_channel fail");
    return -1;
  }

  auto create_cq_fn = [verbs = m_cm_id_->verbs,
                       m_comp_chan_ = this->m_comp_chan_]() -> ibv_cq * {
    ibv_cq *cq = ibv_create_cq(verbs, CQE_NUM, nullptr, m_comp_chan_, 0);
    if (!cq) {
      perror("ibv_create_cq fail");
      return nullptr;
    }

    if (ibv_req_notify_cq(cq, 0)) {
      perror("ibv_req_notify_cq fail");
      return nullptr;
    }
    return cq;
  };

  if (m_cq_handle_) {
    m_cq_handle_->cq_lck_.lock();
    auto it = m_cq_handle_->cq_map_.find(m_cm_id_->verbs);
    if (it == m_cq_handle_->cq_map_.end()) {
      it = m_cq_handle_->cq_map_.emplace(m_cm_id_->verbs, create_cq_fn()).first;
    }
    m_cq_ = it->second;
    m_cq_handle_->cq_lck_.unlock();
  } else {
    m_cq_ = create_cq_fn();
  }

  ibv_qp_init_attr qp_attr = {};
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.cap.max_send_wr = MAX_SEND_WR;
  qp_attr.cap.max_send_sge = MAX_SEND_SGE;
  qp_attr.cap.max_recv_wr = MAX_RECV_WR;
  qp_attr.cap.max_recv_sge = MAX_RECV_SGE;
  qp_attr.cap.max_inline_data = MSG_INLINE_THRESHOLD;
  qp_attr.send_cq = m_cq_;
  qp_attr.recv_cq = m_cq_;

  if (rdma_create_qp(m_cm_id_, m_pd_, &qp_attr)) {
    perror("rdma_create_qp fail");
    return -1;
  }

  return 0;
}

int RDMAConnection::listen(const std::string &ip, uint16_t port) {
  if (rdma_create_id(RDMAEnv::get_instance().m_cm_channel_, &m_cm_id_, NULL,
                     RDMA_PS_TCP)) {
    perror("rdma_create_id fail");
    return -1;
  }

  sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_port = htons(port);
  sin.sin_addr.s_addr = inet_addr(ip.c_str());
  if (sin.sin_addr.s_addr == INADDR_NONE) {
    perror("inet_addr fail");
    return -1;
  }

  if (rdma_bind_addr(m_cm_id_, (struct sockaddr *)&sin)) {
    perror("rdma_bind_addr fail");
    return -1;
  }

  if (rdma_listen(m_cm_id_, 1)) {
    perror("rdma_listen fail");
    return -1;
  }

  m_conn_handler_ = new std::thread(&RDMAConnection::handle_connection, this);
  if (!m_conn_handler_) {
    perror("rdma connect fail");
    return -1;
  }

  return 0;
}

int RDMAConnection::connect(const std::string &ip, uint16_t port) {
  rdma_event_channel *m_cm_channel_ = RDMAEnv::get_instance().m_cm_channel_;

  if (rdma_create_id(m_cm_channel_, &m_cm_id_, NULL, RDMA_PS_TCP)) {
    perror("rdma_create_id fail");
    return -1;
  }

  addrinfo *res;
  if (getaddrinfo(ip.c_str(), std::to_string(port).c_str(), NULL, &res) < 0) {
    perror("getaddrinfo fail");
    return -1;
  }

  addrinfo *addr_tmp = nullptr;
  for (addr_tmp = res; addr_tmp; addr_tmp = addr_tmp->ai_next) {
    if (!rdma_resolve_addr(m_cm_id_, NULL, addr_tmp->ai_addr,
                           RESOLVE_TIMEOUT_MS)) {
      break;
    }
  }
  if (!addr_tmp) {
    perror("rdma_resolve_addr fail");
    return -1;
  }

  rdma_cm_event *event;
  if (rdma_get_cm_event(m_cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return -1;
  }

  if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
    perror("RDMA_CM_EVENT_ADDR_RESOLVED fail");
    return -1;
  }

  rdma_ack_cm_event(event);

  if (rdma_resolve_route(m_cm_id_, RESOLVE_TIMEOUT_MS)) {
    perror("rdma_resolve_route fail");
    return -1;
  }

  if (rdma_get_cm_event(m_cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return -1;
  }

  if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
    perror("RDMA_CM_EVENT_ROUTE_RESOLVED fail");
    return -1;
  }

  rdma_ack_cm_event(event);

  if (create_ibv_connection()) {
    return -1;
  }

  conn_param_t resp_buf_info = {};
  if (m_rpc_conn_) {
    sender.m_msg_head_ = 0;
    sender.m_resp_head_ = 0;
    sender.m_msg_buf_left_half_cnt_ = 0;
    sender.m_msg_buf_right_half_cnt_ = 0;
    sender.m_resp_buf_left_half_cnt_ = 0;
    sender.m_resp_buf_right_half_cnt_ = 0;
    sender.m_msg_buf_ = register_memory(MAX_MESSAGE_BUFFER_SIZE);
    sender.m_resp_buf_ = register_memory(MAX_MESSAGE_BUFFER_SIZE);
    memset(sender.m_msg_buf_->addr, 0, MAX_MESSAGE_BUFFER_SIZE);
    memset(sender.m_resp_buf_->addr, 0, MAX_MESSAGE_BUFFER_SIZE);

    resp_buf_info.addr = (uintptr_t)sender.m_resp_buf_->addr;
    resp_buf_info.rkey = sender.m_resp_buf_->rkey;
  }
  resp_buf_info.rpc_conn = m_rpc_conn_;

  rdma_conn_param conn_param = {};
  conn_param.responder_resources = RESPONDER_RESOURCES;
  conn_param.initiator_depth = INITIATOR_DEPTH;
  conn_param.rnr_retry_count = RNR_RETRY_COUNT;
  conn_param.retry_count = RETRY_COUNT;
  conn_param.private_data = &resp_buf_info;
  conn_param.private_data_len = sizeof(resp_buf_info);

  if (rdma_connect(m_cm_id_, &conn_param)) {
    perror("rdma_connect fail");
    return -1;
  }

  if (rdma_get_cm_event(m_cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return -1;
  }

  if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
    perror("RDMA_CM_EVENT_ESTABLISHED fail");
    return -1;
  }

  conn_param_t msg_buf_info;
  memcpy(&msg_buf_info, event->param.conn.private_data, sizeof(msg_buf_info));
  rdma_ack_cm_event(event);

  sender.m_peer_msg_buf_addr_ = msg_buf_info.addr;
  sender.m_peer_msg_buf_rkey_ = msg_buf_info.rkey;

  return 0;
}

void register_recver_conn_worker(
    std::vector<RDMAMsgPollThread *> &recver_conn_ths, rdma_thread_id_t tid,
    RDMAConnection *conn) {
  RDMAMsgPollThread *&rpt = recver_conn_ths[tid];
  if (!rpt) {
    rpt = new RDMAMsgPollThread();
  }
  rpt->join_recver_conn(conn);
}

void RDMAConnection::handle_connection() {
  struct rdma_cm_event *event;
  std::set<std::pair<RDMAConnection *, rdma_thread_id_t>> srv_conns;
  std::vector<CQHandle> cq_handles(MAX_RECVER_THREAD_COUNT);
  std::vector<RDMAMsgPollThread *> rpt_pool(MAX_RECVER_THREAD_COUNT, nullptr);
  // 等待加入的线程队列
  std::vector<rdma_thread_id_t> thread_waiting_pool;
  for (uint8_t i = MAX_RECVER_THREAD_COUNT; i > 0; --i) {
    thread_waiting_pool.push_back(i - 1);
  }

  while (!m_stop_) {
    if (rdma_get_cm_event(RDMAEnv::get_instance().m_cm_channel_, &event)) {
      perror("rdma_get_cm_event fail");
      return;
    }

    if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
      struct rdma_cm_id *cm_id = event->id;
      conn_param_t resp_buf_info;
      memcpy(&resp_buf_info, event->param.conn.private_data,
             sizeof(resp_buf_info));
      rdma_ack_cm_event(event);

      // 选出一个thread来进行poll msg
      rdma_thread_id_t tid = thread_waiting_pool.back();
      thread_waiting_pool.pop_back();
      // 如果waiting poll为空，则填充
      if (thread_waiting_pool.empty()) {
        for (uint8_t i = MAX_RECVER_THREAD_COUNT; i > 0; --i) {
          thread_waiting_pool.push_back(i - 1);
        }
      }

      RDMAConnection *conn =
          new RDMAConnection(&cq_handles[tid], resp_buf_info.rpc_conn);
      conn->m_cm_id_ = cm_id;
      conn->recver.m_peer_resp_buf_addr_ = resp_buf_info.addr;
      conn->recver.m_peer_resp_buf_rkey_ = resp_buf_info.rkey;
      conn->create_connection();
      srv_conns.emplace(conn, tid);
      register_recver_conn_worker(rpt_pool, tid, conn);

      printf("get event: %s\n", rdma_event_str(event->event));

    } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
      rdma_ack_cm_event(event);
    } else {
      struct rdma_cm_id *cm_id = event->id;
      rdma_ack_cm_event(event);

      for (auto it = srv_conns.begin(); it != srv_conns.end(); ++it) {
        if (it->first->m_cm_id_ == cm_id) {
          thread_waiting_pool.push_back(it->second);
          rpt_pool[it->second]->exit_recver_conn(it->first);
          delete it->first;
          srv_conns.erase(it);
          break;
        }
      }

      printf("get event: %s\n", rdma_event_str(event->event));
    }
  }

  for (auto &rpt : rpt_pool) {
    if (rpt) {
      delete rpt;
    }
  }

  for (auto &conn : srv_conns) {
    delete conn.first;
  }
}

void RDMAConnection::create_connection() {
  if (create_ibv_connection()) {
    return;
  }

  conn_param_t msg_buf_info = {};
  if (m_rpc_conn_) {
    recver.m_msg_head_ = 0;
    recver.m_resp_head_ = 0;
    recver.m_msg_buf_ = register_memory(MAX_MESSAGE_BUFFER_SIZE);
    recver.m_resp_buf_ = register_memory(MAX_MESSAGE_BUFFER_SIZE);
    memset(recver.m_msg_buf_->addr, 0, MAX_MESSAGE_BUFFER_SIZE);
    memset(recver.m_resp_buf_->addr, 0, MAX_MESSAGE_BUFFER_SIZE);

    msg_buf_info.addr = (uintptr_t)recver.m_msg_buf_->addr;
    msg_buf_info.rkey = recver.m_msg_buf_->rkey;
  }

  rdma_conn_param conn_param = {};
  conn_param.responder_resources = RESPONDER_RESOURCES;
  conn_param.initiator_depth = INITIATOR_DEPTH;
  conn_param.rnr_retry_count = RNR_RETRY_COUNT;
  conn_param.retry_count = RETRY_COUNT;
  conn_param.private_data = &msg_buf_info;
  conn_param.private_data_len = sizeof(msg_buf_info);

  if (rdma_accept(m_cm_id_, &conn_param)) {
    perror("rdma_accept fail");
    return;
  }
}

ibv_mr *RDMAConnection::register_memory(size_t size) {
  void *ptr = aligned_alloc(4096, size);
  if (!ptr) {
    perror("aligned_alloc fail");
    return nullptr;
  }
  return register_memory(ptr, size);
}

ibv_mr *RDMAConnection::register_memory(void *ptr, size_t size) {
  uint32_t access =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  if (m_atomic_support_) {
    access |= IBV_ACCESS_REMOTE_ATOMIC;
  }
  ibv_mr *mr = ibv_reg_mr(m_pd_, ptr, size, access);
  if (!mr) {
    perror("ibv_reg_mr fail");
    return nullptr;
  }
  return mr;
}
