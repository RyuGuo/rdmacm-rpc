#include "rdma_conn.h"
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <sstream>
#include <thread>
#include <tuple>

using namespace std;

struct p_data_t {
  uintptr_t addr;
  uint32_t length;
  uint32_t rkey;
};

int main(int argc, char **argv) {
  RDMAEnv::init();

  vector<p_data_t> remote_mr_info;

  RDMAConnection conn;

  conn.connect(argv[1], 8765);

  cout << "connect ok" << endl;

  ibv_mr *mr = conn.register_memory(64);

  MsgQueueHandle qh;

  conn.prep_rpc_send(qh, 1, nullptr, 0, sizeof(p_data_t));
  uint64_t t = conn.submit(qh);

  cout << "send msg ok" << endl;

  std::vector<const void *> resp_data_ptr;
  conn.remote_task_wait(t, resp_data_ptr);

  cout << "get resp ok" << endl;

  p_data_t *pdata = (p_data_t *)resp_data_ptr[0];

  {
    *(uint64_t *)mr->addr = 0;
    conn.prep_write(qh, (uintptr_t)mr->addr, mr->lkey, 8, pdata->addr,
                    pdata->rkey);
    uint64_t t = conn.submit(qh);

    std::vector<const void *> resp_data_ptr;
    assert(conn.remote_task_wait(t, resp_data_ptr) == 0);

    cout << "write ok" << endl;
  }

  for (int i = 0; i < 4; ++i) {
    conn.prep_fetch_add(qh, (uintptr_t)mr->addr, mr->lkey, pdata->addr,
                        pdata->rkey, 1);
    uint64_t t = conn.submit(qh);

    std::vector<const void *> resp_data_ptr;
    assert(conn.remote_task_wait(t, resp_data_ptr) == 0);
  }

  cout << "fetch add ok" << endl;

  {
    conn.prep_read(qh, (uintptr_t)mr->addr, mr->lkey, 8, pdata->addr,
                   pdata->rkey);
    uint64_t t = conn.submit(qh);

    std::vector<const void *> resp_data_ptr;
    assert(conn.remote_task_wait(t, resp_data_ptr) == 0);
    assert(*(uint64_t *)mr->addr == 4);
  }

  conn.dealloc_resp_data(resp_data_ptr[0]);

  vector<thread> ths;
  uint64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
  for (int j = 0; j < 4; ++j) {
    ths.emplace_back([&conn]() {
      MsgQueueHandle qh;
      for (int i = 0; i < 40000; ++i) {
        int *p = (int *)conn.prep_rpc_send_defer(qh, 2, sizeof(i), sizeof(int));
        *p = i;
        conn.prep_rpc_send_confirm();
        p = (int *)conn.prep_rpc_send_defer(qh, 2, sizeof(i), sizeof(int));
        *p = i + 1;
        conn.prep_rpc_send_confirm();
        uint64_t t = conn.submit(qh);
        std::vector<const void *> resp_data_ptr;
        int rc = conn.remote_task_wait(t, resp_data_ptr);
        assert(rc == 0);
        assert(resp_data_ptr.size() == 2);
        assert(*(const int *)resp_data_ptr[0] == i);
        assert(*(const int *)resp_data_ptr[1] == i + 1);
        conn.dealloc_resp_data(resp_data_ptr[0]);
        conn.dealloc_resp_data(resp_data_ptr[1]);
      }
    });
  }
  for (auto &th : ths) {
    th.join();
  }

  cout << (std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
               .count() -
           now_ms) /
              40000.0 / 4 * 1000
       << "us" << endl;

  cout << "test ok" << endl;

  return 0;
}