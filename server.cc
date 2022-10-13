#include "rdma_conn.h"
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <sstream>

using namespace std;

struct p_data_t {
  uintptr_t addr;
  uint32_t length;
  uint32_t rkey;
};

int main() {
  RDMAEnv::init();

  RDMAConnection::MAX_RECVER_THREAD_COUNT = 1;
  RDMAConnection::VEC_RECVER_THREAD_BIND_CORE = {0, 1, 2, 3};
  RDMAConnection::MAX_MESSAGE_BUFFER_SIZE = 2ul << 20;

  RDMAConnection::register_rpc_func(
      1, [](RDMAConnection *conn, const void *data, uint32_t size,
            void *resp_data, uint32_t max_resp_data_length) {
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
            void *resp_data, uint32_t max_resp_data_length) {
        // cout << "get msg 2" << endl;

        memcpy(resp_data, data, size);
        return size;
      });
  RDMAConnection::register_rpc_func(
      3, [](RDMAConnection *conn, const void *data, uint32_t size,
            void *resp_data, uint32_t max_resp_data_length) {
        struct {
          uint64_t addr;
          uint32_t length;
        } *p = (decltype(p))data;
        memcpy((void *)(p->addr + p->length), (void *)p->addr, p->length);
        return 0;
      });

  RDMAConnection conn;

  conn.listen("0.0.0.0", 8765);

  cout << "listen" << endl;

  getchar();
  getchar();

  return 0;
}