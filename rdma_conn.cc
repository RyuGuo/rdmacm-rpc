#include "rdma_conn.h"
#include <arpa/inet.h>
#include <netdb.h>

struct conn_param_t {
  uint64_t addr;
  uint32_t rkey;
  uint32_t size;
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
uint32_t RDMAConnection::MAX_MESSAGE_BUFFER_SIZE = 4096;
uint32_t RDMAConnection::MSG_INLINE_THRESHOLD = 64;
uint8_t RDMAConnection::MAX_RECVER_THREAD_COUNT = 4;
uint32_t RDMAConnection::MAX_RECV_BUFFER_SIZE = 1024;
uint8_t RDMAConnection::MAX_RECV_BUFFER_CNT = 16;
uint32_t RDMAConnection::MAX_SRQ_WR = 256;

std::vector<int16_t> RDMAConnection::VEC_RECVER_THREAD_BIND_CORE;

std::function<void(RDMAConnection *conn)> RDMAConnection::m_hook_connect_;
std::function<void(RDMAConnection *conn)> RDMAConnection::m_hook_disconnect_;

static RecvBlockInfo alloc_recv_block(RDMAConnection *handler_conn, RDMAConnection *conn) {
  auto &vec = (*handler_conn->m_recv_block_pool_)[conn->m_pd_];
  if (vec.empty()) {
    ibv_mr *mr = conn->register_memory(RDMAConnection::MAX_RECV_BUFFER_SIZE
                                       * RDMAConnection::MAX_RECV_BUFFER_CNT);
    MpScRestrictBoundedMemPool<void *> *q =
      new MpScRestrictBoundedMemPool<void *>(RDMAConnection::MAX_RECV_BUFFER_CNT);
    for (uint8_t i = 0; i < RDMAConnection::MAX_RECV_BUFFER_CNT; ++i) {
      q->push((char *)mr->addr + i * RDMAConnection::MAX_RECV_BUFFER_SIZE);
    }

    return {mr, q};
  } else {
    RecvBlockInfo rb = vec.back();
    vec.pop_back();
    return rb;
  }
}
static void dealloc_recv_block(RDMAConnection *handler_conn, RecvBlockInfo &&rb) {
  (*handler_conn->m_recv_block_pool_)[rb.recv_block_mr->pd].emplace_back(
    std::forward<RecvBlockInfo>(rb));
}

bool RDMAConnection::m_rdma_conn_param_valid_() {
  ibv_device_attr device_attr;
  if (ibv_query_device(m_cm_id_->verbs, &device_attr) != 0) {
    perror("ibv_query_device fail");
    return false;
  }
  m_atomic_support_ = device_attr.atomic_cap != IBV_ATOMIC_NONE;
  m_inline_support_ = m_cm_id_->verbs->device->transport_type != IBV_TRANSPORT_UNKNOWN;
  return device_attr.max_cqe >= CQE_NUM && device_attr.max_qp_wr >= MAX_SEND_WR
         && device_attr.max_qp_wr >= MAX_RECV_WR && device_attr.max_sge >= MAX_SEND_SGE
         && device_attr.max_sge >= MAX_RECV_SGE && device_attr.max_qp_rd_atom >= RESPONDER_RESOURCES
         && device_attr.max_qp_init_rd_atom >= RESPONDER_RESOURCES
         && device_attr.max_qp_rd_atom >= INITIATOR_DEPTH
         && device_attr.max_qp_init_rd_atom >= INITIATOR_DEPTH
         && device_attr.max_mr_size >= MAX_MESSAGE_BUFFER_SIZE + MAX_MESSAGE_BUFFER_SIZE
         && device_attr.max_mr_size >= MAX_RECV_BUFFER_SIZE * MAX_RECV_BUFFER_CNT
         && device_attr.max_srq_wr >= MAX_SRQ_WR;
}

int RDMAEnv::init() {
  return get_instance().__init__();
}

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
  for (auto &p : m_cq_map_) {
    ibv_destroy_cq(p.second);
  }
}

SRQHandle::~SRQHandle() {
  for (auto &p : m_srq_map_) {
    ibv_destroy_srq(p.second);
  }
}

RDMAConnection::RDMAConnection(CQHandle *cq_handle, bool rpc_conn)
  : m_stop_(false), m_rpc_conn_(rpc_conn), m_cm_id_(nullptr), m_pd_(nullptr), m_recv_cq_(nullptr),
    m_srq_(nullptr), m_dm_(nullptr), m_cq_handle_(cq_handle), m_recv_cq_handle_(nullptr),
    m_srq_handle_(nullptr), m_conn_handler_(nullptr), m_send_defer_cnt_(0) {
}
RDMAConnection::~RDMAConnection() {
  m_stop_ = true;
  switch (conn_type) {
  case SENDER: {
    rdma_disconnect(m_cm_id_);
    rdma_destroy_qp(m_cm_id_);
    if (!m_cq_handle_) {
      ibv_destroy_cq(m_cq_);
    }
    ibv_destroy_comp_channel(m_comp_chan_);
    ibv_dereg_mr(m_sender_.m_msg_buf_);
    free(m_sender_.m_msg_buf_->addr);
    delete m_sender_.m_resp_buf_;
    if (m_dm_) {
      ibv_free_dm(m_dm_);
    }
  } break;
  case RECVER: {
    rdma_disconnect(m_cm_id_);
    rdma_destroy_qp(m_cm_id_);
    if (!m_cq_handle_) {
      ibv_destroy_cq(m_cq_);
    }
    ibv_destroy_comp_channel(m_comp_chan_);
    ibv_dereg_mr(m_recver_.m_msg_buf_);
    free(m_recver_.m_msg_buf_->addr);
    delete m_recver_.m_resp_buf_;
    if (m_dm_) {
      ibv_free_dm(m_dm_);
    }
  } break;
  case LISTENER: {
    m_conn_handler_->join();
    for (auto &p : *m_recv_block_pool_) {
      for (auto &rb : p.second) {
        ibv_dereg_mr(rb.recv_block_mr);
        delete rb.free_recv_block_pool;
      }
    }
    delete m_recv_block_pool_;
  } break;
  }
  rdma_destroy_id(m_cm_id_);
}

static ibv_cq *create_cq(ibv_context *verbs, ibv_comp_channel *comp_chan) {
  ibv_cq *cq = ibv_create_cq(verbs, RDMAConnection::CQE_NUM, nullptr, comp_chan, 0);
  if (!cq) {
    perror("ibv_create_cq fail");
    return nullptr;
  }

  if (ibv_req_notify_cq(cq, 0)) {
    perror("ibv_req_notify_cq fail");
    return nullptr;
  }

  return cq;
}

int RDMAConnection::m_init_ibv_connection_() {
  if (!m_rdma_conn_param_valid_()) {
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

  if (m_cq_handle_) {
    m_cq_handle_->m_cq_lck_.lock();
    auto it = m_cq_handle_->m_cq_map_.find(m_cm_id_->verbs);
    if (it == m_cq_handle_->m_cq_map_.end()) {
      it =
        m_cq_handle_->m_cq_map_.emplace(m_cm_id_->verbs, create_cq(m_cm_id_->verbs, m_comp_chan_))
          .first;
    }
    m_cq_ = it->second;
    m_cq_handle_->m_cq_lck_.unlock();
  } else {
    m_cq_ = create_cq(m_cm_id_->verbs, m_comp_chan_);
  }

  if (m_recv_cq_handle_) {
    m_recv_cq_handle_->m_cq_lck_.lock();
    auto it = m_recv_cq_handle_->m_cq_map_.find(m_cm_id_->verbs);
    if (it == m_recv_cq_handle_->m_cq_map_.end()) {
      it = m_recv_cq_handle_->m_cq_map_
             .emplace(m_cm_id_->verbs, create_cq(m_cm_id_->verbs, m_comp_chan_))
             .first;
    }
    m_recv_cq_ = it->second;
    m_recv_cq_handle_->m_cq_lck_.unlock();
  }

  if (m_srq_handle_) {
    m_srq_handle_->m_srq_lck_.lock();
    auto it = m_srq_handle_->m_srq_map_.find(m_cm_id_->verbs);
    if (it == m_srq_handle_->m_srq_map_.end()) {
      ibv_srq_init_attr srq_attr = {};
      srq_attr.attr.max_wr = MAX_SRQ_WR;
      srq_attr.attr.max_sge = 1;
      srq_attr.attr.srq_limit = 0;
      srq_attr.srq_context = (void *)0; // srq context 作为 watermark

      ibv_srq *srq = ibv_create_srq(m_pd_, &srq_attr);
      it = m_srq_handle_->m_srq_map_.emplace(m_cm_id_->verbs, srq).first;
    }
    m_srq_ = it->second;
    m_srq_handle_->m_srq_lck_.unlock();
  }

  ibv_qp_init_attr qp_attr = {};
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.cap.max_send_wr = MAX_SEND_WR;
  qp_attr.cap.max_send_sge = MAX_SEND_SGE;
  qp_attr.cap.max_recv_wr = MAX_RECV_WR;
  qp_attr.cap.max_recv_sge = MAX_RECV_SGE;
  qp_attr.cap.max_inline_data = MSG_INLINE_THRESHOLD;
  qp_attr.send_cq = m_cq_;
  qp_attr.recv_cq = m_recv_cq_;
  qp_attr.srq = m_srq_;

  if (rdma_create_qp(m_cm_id_, m_pd_, &qp_attr)) {
    perror("rdma_create_qp fail");
    return -1;
  }

  return 0;
}

int RDMAConnection::listen(const std::string &ip, uint16_t port) {
  conn_type = LISTENER;

  if (rdma_create_id(RDMAEnv::get_instance().m_cm_channel_, &m_cm_id_, NULL, RDMA_PS_TCP)) {
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

  m_recv_block_pool_ = new std::map<ibv_pd *, std::vector<RecvBlockInfo>>();

  m_conn_handler_ = new std::thread(&RDMAConnection::m_handle_connection_, this);
  if (!m_conn_handler_) {
    perror("rdma connect fail");
    return -1;
  }

  return 0;
}

int RDMAConnection::connect(const std::string &ip, uint16_t port) {
  conn_type = SENDER;

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
    if (!rdma_resolve_addr(m_cm_id_, NULL, addr_tmp->ai_addr, RESOLVE_TIMEOUT_MS)) {
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

  if (m_init_ibv_connection_()) {
    return -1;
  }

  conn_param_t resp_buf_info = {};
  if (m_rpc_conn_) {
    m_sender_.m_msg_head_ = 0;
    m_sender_.m_resp_head_ = 0;
    m_sender_.m_msg_buf_left_half_cnt_ = 0;
    m_sender_.m_msg_buf_right_half_cnt_ = 0;
    m_sender_.m_resp_buf_left_half_cnt_ = 0;
    m_sender_.m_resp_buf_right_half_cnt_ = 0;
    m_sender_.m_msg_buf_ = register_memory(MAX_MESSAGE_BUFFER_SIZE + MAX_MESSAGE_BUFFER_SIZE);
    m_sender_.m_resp_buf_ = new ibv_mr(*m_sender_.m_msg_buf_);
    (char *&)m_sender_.m_resp_buf_->addr += MAX_MESSAGE_BUFFER_SIZE;
    memset(m_sender_.m_msg_buf_->addr, 0, MAX_MESSAGE_BUFFER_SIZE);
    memset(m_sender_.m_resp_buf_->addr, 0, MAX_MESSAGE_BUFFER_SIZE);

    resp_buf_info.size = MAX_MESSAGE_BUFFER_SIZE;
    resp_buf_info.addr = (uintptr_t)m_sender_.m_resp_buf_->addr;
    resp_buf_info.rkey = m_sender_.m_resp_buf_->rkey;
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

  m_sender_.m_peer_msg_buf_addr_ = msg_buf_info.addr;
  m_sender_.m_peer_msg_buf_rkey_ = msg_buf_info.rkey;
  m_sender_.m_matched_buf_size_ = std::min(MAX_MESSAGE_BUFFER_SIZE, msg_buf_info.size);

  return 0;
}

void RDMAConnection::m_handle_connection_() {
  struct rdma_cm_event *event;
  std::map<rdma_cm_id *, std::pair<RDMAConnection *, rdma_thread_id_t>> srv_conns;
  std::vector<CQHandle> cq_handles(MAX_RECVER_THREAD_COUNT);
  std::vector<CQHandle> recv_cq_handles(MAX_RECVER_THREAD_COUNT);
  std::vector<SRQHandle> srq_handles(MAX_RECVER_THREAD_COUNT);

  RDMAThreadScheduler &scheduler = RDMAThreadScheduler::get_instance();

  while (!m_stop_) {
    if (rdma_get_cm_event(RDMAEnv::get_instance().m_cm_channel_, &event)) {
      perror("rdma_get_cm_event fail");
      return;
    }

    if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
      struct rdma_cm_id *cm_id = event->id;
      conn_param_t resp_buf_info;
      memcpy(&resp_buf_info, event->param.conn.private_data, sizeof(resp_buf_info));
      rdma_ack_cm_event(event);

      // 选出一个thread来进行poll msg
      rdma_thread_id_t tid = scheduler.prepick_one_thread();

      RDMAConnection *conn = new RDMAConnection(&cq_handles[tid], resp_buf_info.rpc_conn);
      conn->conn_type = RECVER;
      conn->m_cm_id_ = cm_id;
      conn->m_recver_.m_peer_resp_buf_addr_ = resp_buf_info.addr;
      conn->m_recver_.m_peer_resp_buf_rkey_ = resp_buf_info.rkey;
      conn->m_recver_.m_matched_buf_size_ =
        std::min(RDMAConnection::MAX_MESSAGE_BUFFER_SIZE, resp_buf_info.size);
      conn->m_recv_cq_handle_ = &recv_cq_handles[tid];
      conn->m_srq_handle_ = &srq_handles[tid];
      m_init_connection_(conn);

      srv_conns.emplace(cm_id, std::make_pair(conn, tid));
      scheduler.register_conn_worker(tid, conn);

      if (m_hook_connect_)
        m_hook_connect_(conn);

    } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
      rdma_ack_cm_event(event);
    } else {
      struct rdma_cm_id *cm_id = event->id;
      rdma_ack_cm_event(event);

      auto it = srv_conns.find(cm_id);
      if (m_hook_disconnect_)
        m_hook_disconnect_(it->second.first);
      scheduler.unregister_conn_worker(it->second.second, it->second.first);

      dealloc_recv_block(this, {it->second.first->m_recver_.m_recv_block_mr_,
                                it->second.first->m_recver_.m_free_recv_block_queue_});

      delete it->second.first;
      srv_conns.erase(it);
    }
  }

  for (auto &conn : srv_conns) {
    delete conn.second.first;
  }
}

void RDMAConnection::m_init_connection_(RDMAConnection *init_conn) {
  if (init_conn->m_init_ibv_connection_()) {
    return;
  }

  conn_param_t msg_buf_info = {};
  if (init_conn->m_rpc_conn_) {
    init_conn->m_recver_.m_msg_head_ = 0;
    init_conn->m_recver_.m_resp_head_ = 0;
    init_conn->m_recver_.m_msg_buf_ =
      init_conn->register_memory(MAX_MESSAGE_BUFFER_SIZE + MAX_MESSAGE_BUFFER_SIZE);
    init_conn->m_sender_.m_resp_buf_ = new ibv_mr(*init_conn->m_sender_.m_msg_buf_);
    (char *&)init_conn->m_sender_.m_resp_buf_->addr += MAX_MESSAGE_BUFFER_SIZE;
    memset(init_conn->m_recver_.m_msg_buf_->addr, 0, MAX_MESSAGE_BUFFER_SIZE);
    memset(init_conn->m_recver_.m_resp_buf_->addr, 0, MAX_MESSAGE_BUFFER_SIZE);

    RecvBlockInfo rb = alloc_recv_block(this, init_conn);
    init_conn->m_recver_.m_recv_block_mr_ = rb.recv_block_mr;
    init_conn->m_recver_.m_free_recv_block_queue_ = rb.free_recv_block_pool;

    msg_buf_info.size = MAX_MESSAGE_BUFFER_SIZE;
    msg_buf_info.addr = (uintptr_t)init_conn->m_recver_.m_msg_buf_->addr;
    msg_buf_info.rkey = init_conn->m_recver_.m_msg_buf_->rkey;
  }

  rdma_conn_param conn_param = {};
  conn_param.responder_resources = RESPONDER_RESOURCES;
  conn_param.initiator_depth = INITIATOR_DEPTH;
  conn_param.rnr_retry_count = RNR_RETRY_COUNT;
  conn_param.retry_count = RETRY_COUNT;
  conn_param.private_data = &msg_buf_info;
  conn_param.private_data_len = sizeof(msg_buf_info);

  if (rdma_accept(init_conn->m_cm_id_, &conn_param)) {
    perror("rdma_accept fail");
    return;
  }
}

std::pair<std::string, in_port_t> RDMAConnection::get_local_addr() {
  sockaddr_in *sin = (sockaddr_in *)rdma_get_local_addr(m_cm_id_);
  return std::make_pair(inet_ntoa(sin->sin_addr), (sin->sin_port));
}
std::pair<std::string, in_port_t> RDMAConnection::get_peer_addr() {
  sockaddr_in *sin = (sockaddr_in *)rdma_get_peer_addr(m_cm_id_);
  return std::make_pair(inet_ntoa(sin->sin_addr), (sin->sin_port));
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
  uint32_t access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
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

ibv_mr *RDMAConnection::register_device_memory(size_t size) {
  ibv_alloc_dm_attr dm_attr{};
  dm_attr.length = size;
  dm_attr.log_align_req = 3;
  if (!m_dm_) {
    ibv_dm *m_dm_ = ibv_alloc_dm(m_cm_id_->verbs, &dm_attr);
    if (!m_dm_) {
      perror("ibv_alloc_dm fail");
      return nullptr;
    }
  }
  ibv_mr *mr =
    ibv_reg_dm_mr(m_pd_, m_dm_, 0, size,
                  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
                    | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_ZERO_BASED);
  if (!mr) {
    perror("ibv_reg_dm_mr fail");
    return nullptr;
  }
  return mr;
}

void RDMAConnection::register_connect_hook(
  std::function<void(RDMAConnection *conn)> &&hook_connect) {
  m_hook_connect_ = std::forward<std::function<void(RDMAConnection * conn)>>(hook_connect);
}

void RDMAConnection::register_disconnect_hook(
  std::function<void(RDMAConnection *conn)> &&hook_disconnect) {
  m_hook_disconnect_ = std::forward<std::function<void(RDMAConnection * conn)>>(hook_disconnect);
}
