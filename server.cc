#include "rdma_conn.h"
#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>

using namespace std;

struct p_data_t {
  uintptr_t addr;
  uint32_t length;
  uint32_t rkey;
};

int main() {
  RDMAEnv::init();

  RDMAConnection::MAX_RECVER_THREAD_COUNT = 4;
  RDMAConnection::VEC_RECVER_THREAD_BIND_CORE = {0, 1, 2, 3};
  RDMAConnection::MAX_MESSAGE_BUFFER_SIZE = 64ul << 10;

  RDMAConnection::register_rpc_func(1, [](RDMAConnection *conn, void *data, uint32_t size,
                                          void *resp_data, uint32_t max_resp_data_length,
                                          void **uctx) {
    // cout << "get msg 1" << endl;

    ibv_mr *mr = conn->register_memory(1 << 20);
    p_data_t *pdata = (p_data_t *)resp_data;
    pdata->addr = (uintptr_t)mr->addr;
    pdata->length = mr->length;
    pdata->rkey = mr->rkey;
    return sizeof(*pdata);
  });
  RDMAConnection::register_rpc_func(2, [](RDMAConnection *conn, void *data, uint32_t size,
                                          void *resp_data, uint32_t max_resp_data_length,
                                          void **uctx) {
    // cout << "get msg 2" << endl;

    memcpy(resp_data, data, size);
    return size;
  });
  RDMAConnection::register_rpc_func(3, [](RDMAConnection *conn, void *data, uint32_t size,
                                          void *resp_data, uint32_t max_resp_data_length,
                                          void **uctx) {
    struct {
      uint64_t addr;
      uint32_t length;
    } *p = (decltype(p))data;
    memcpy((void *)(p->addr + p->length), (void *)p->addr, p->length);
    return 0;
  });
  RDMAConnection::register_rpc_func(4, [](RDMAConnection *conn, void *data, uint32_t size,
                                          void *resp_data, uint32_t max_resp_data_length,
                                          void **uctx) {
    uint64_t *s;
    // 协程睡眠10us
    if (!(*uctx)) {
      s = new uint64_t();
      *(uint64_t **)uctx = s;
      *s = std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
             .count();
      return RDMAConnection::UCTX_YIELD;
    } else {
      s = *(uint64_t **)uctx;
      uint64_t e = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
      if (e - *s < 10) {
        return RDMAConnection::UCTX_YIELD;
      }
    }
    delete s;
    return 0u;
  });
  RDMAConnection::register_rpc_func(5, [](RDMAConnection *conn, void *data, uint32_t size,
                                          void *resp_data, uint32_t max_resp_data_length,
                                          void **uctx) {
    // test end (will throw an error because a thread is not joined)
    exit(0);
    return 0;
  });

  RDMAConnection::register_rdma_write_with_imm_handle(
    [](RDMAConnection *conn, uint32_t imm_data, void **uctx) {
      assert(imm_data == 0x123);
      return 0u;
    });

  RDMAConnection::register_connect_hook([](RDMAConnection *conn) {
    cout << "Get New Connect:" << conn->get_peer_addr().first << endl;
  });
  RDMAConnection::register_disconnect_hook(
    [](RDMAConnection *conn) { cout << "Disconnect: " << conn->get_peer_addr().first << endl; });

  RDMAConnection conn;

  conn.listen("0.0.0.0", 8765);

  cout << "listen" << endl;

  // this_thread::sleep_for(3s);

  getchar();
  getchar();

  return 0;
}