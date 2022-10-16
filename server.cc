#include "rdma_conn.h"
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <sstream>
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

  RDMAConnection::register_rpc_func(
      1, [](RDMAConnection *conn, const void *data, uint32_t size,
            void *resp_data, uint32_t max_resp_data_length, void **uctx) {
        // cout << "get msg 1" << endl;

        ibv_mr *mr = conn->register_memory(1 << 20);
        p_data_t *pdata = (p_data_t *)resp_data;
        pdata->addr = (uintptr_t)mr->addr;
        pdata->length = mr->length;
        pdata->rkey = mr->rkey;
        return sizeof(*pdata);
      });
  RDMAConnection::register_rpc_func(
      2, [](RDMAConnection *conn, const void *data, uint32_t size,
            void *resp_data, uint32_t max_resp_data_length, void **uctx) {
        // cout << "get msg 2" << endl;

        memcpy(resp_data, data, size);
        return size;
      });
  RDMAConnection::register_rpc_func(
      3, [](RDMAConnection *conn, const void *data, uint32_t size,
            void *resp_data, uint32_t max_resp_data_length, void **uctx) {
        struct {
          uint64_t addr;
          uint32_t length;
        } *p = (decltype(p))data;
        memcpy((void *)(p->addr + p->length), (void *)p->addr, p->length);
        return 0;
      });
  RDMAConnection::register_rpc_func(
      4, [](RDMAConnection *conn, const void *data, uint32_t size,
            void *resp_data, uint32_t max_resp_data_length, void **uctx) {
        uint64_t *s;
        // 协程睡眠10us
        if (!(*uctx)) {
          s = new uint64_t();
          *(uint64_t **)uctx = s;
          *s = std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count();
          return -1u;
        } else {
          s = *(uint64_t **)uctx;
          uint64_t e = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
          if (e - *s < 10) {
            return -1u;
          }
        }
        delete s;
        return 0u;
      });

  RDMAConnection conn;

  conn.listen("0.0.0.0", 8765);

  cout << "listen" << endl;

  // this_thread::sleep_for(3s);

  getchar();
  getchar();

  return 0;
}