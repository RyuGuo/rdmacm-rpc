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
  RDMAConnection::MAX_MESSAGE_BUFFER_SIZE = 2ul << 20;

  RDMAConnection conn;

  conn.connect(argv[1], 8765);

  cout << "connect ok" << endl;

  ibv_mr *mr = conn.register_memory(1 << 20);

  MsgQueueHandle qh;

  conn.prep_rpc_send(qh, 1, nullptr, 0, sizeof(p_data_t));
  RDMAFuture t = conn.submit(qh);

  cout << "send msg ok" << endl;

  std::vector<const void *> resp_data_ptr;
  t.get(resp_data_ptr);

  cout << "get resp ok" << endl;

  p_data_t pdata = *(p_data_t *)resp_data_ptr[0];
  conn.dealloc_resp_data(resp_data_ptr[0]);

  {
    *(uint64_t *)mr->addr = 0;
    conn.prep_write(qh, (uintptr_t)mr->addr, mr->lkey, 8, pdata.addr,
                    pdata.rkey);
    RDMAFuture t = conn.submit(qh);

    std::vector<const void *> resp_data_ptr;
    assert(t.get(resp_data_ptr) == 0);

    cout << "write ok" << endl;
  }

  {
    for (int i = 0; i < 4; ++i) {
      conn.prep_fetch_add(qh, (uintptr_t)mr->addr, mr->lkey, pdata.addr,
                          pdata.rkey, 1);
      RDMAFuture t = conn.submit(qh);

      std::vector<const void *> resp_data_ptr;
      assert(t.get(resp_data_ptr) == 0);
    }

    cout << "fetch add ok" << endl;
  }

  {
    conn.prep_read(qh, (uintptr_t)mr->addr, mr->lkey, 8, pdata.addr,
                   pdata.rkey);
    RDMAFuture t = conn.submit(qh);

    std::vector<const void *> resp_data_ptr;
    assert(t.get(resp_data_ptr) == 0);
    assert(*(uint64_t *)mr->addr == 4);

    cout << "read ok" << endl;
  }

  vector<thread> ths;
  {
    uint64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    for (int j = 0; j < 4; ++j) {
      ths.emplace_back([&conn]() {
        MsgQueueHandle qh;
        for (int i = 0; i < 100000; ++i) {
          int *p;
          do {
            p = (int *)conn.prep_rpc_send_defer(qh, 2, 256, 256);
          } while (p == nullptr);
          *p = i;
          conn.prep_rpc_send_confirm();
          // do {
          //   p = (int *)conn.prep_rpc_send_defer(qh, 2, sizeof(i),
          //   sizeof(int));
          // } while (p != nullptr);
          // *p = i + 1;
          // conn.prep_rpc_send_confirm();
          RDMAFuture fu = conn.submit(qh);
          std::vector<const void *> resp_data_ptr;
          int rc = fu.get(resp_data_ptr);
          assert(rc == 0);
          assert(resp_data_ptr.size() == 1);
          // assert(resp_data_ptr.size() == 2);
          assert(*(const int *)resp_data_ptr[0] == i);
          // assert(*(const int *)resp_data_ptr[1] == i + 1);
          conn.dealloc_resp_data(resp_data_ptr[0]);
          // conn.dealloc_resp_data(resp_data_ptr[1]);
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
                100000.0 / 4 * 1000
         << "us" << endl;
  }

  const size_t PSIZE = 64;
  assert(PSIZE <= mr->length / 2);

  ths.clear();
  {
    uint64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    for (int j = 0; j < 4; ++j) {
      ths.emplace_back([&conn, mr, &pdata]() {
        MsgQueueHandle qh;
        for (int i = 0; i < 100000; ++i) {
          conn.prep_write(qh, (uintptr_t)mr->addr, mr->lkey, PSIZE, pdata.addr,
                          pdata.rkey);
          conn.prep_write(qh, (uintptr_t)mr->addr, mr->lkey, PSIZE,
                          pdata.addr + PSIZE, pdata.rkey);
          int *p;
          do {
            p = (int *)conn.prep_rpc_send_defer(qh, 2, sizeof(i), sizeof(int));
          } while (p == nullptr);
          *p = i;
          conn.prep_rpc_send_confirm();
          RDMAFuture fu = conn.submit(qh);
          std::vector<const void *> resp_data_ptr;
          fu.get(resp_data_ptr);
          assert(resp_data_ptr.size() == 1);
          conn.dealloc_resp_data(resp_data_ptr[0]);
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
                100000.0 / 4 * 1000
         << "us" << endl;
  }

  ths.clear();
  {
    uint64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    for (int j = 0; j < 4; ++j) {
      ths.emplace_back([&conn, mr, &pdata]() {
        MsgQueueHandle qh;
        for (int i = 0; i < 100000; ++i) {
          conn.prep_write(qh, (uintptr_t)mr->addr, mr->lkey, PSIZE, pdata.addr,
                          pdata.rkey);
          struct {
            uint64_t addr;
            uint32_t length;
          } p = {(uintptr_t)pdata.addr, PSIZE};
          while (conn.prep_rpc_send(qh, 3, &p, sizeof(p), 0) == -1)
            ;
          RDMAFuture fu = conn.submit(qh);
          std::vector<const void *> resp_data_ptr;
          fu.get(resp_data_ptr);
          assert(resp_data_ptr.size() == 1);
          assert(resp_data_ptr[0] == nullptr);
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
                100000.0 / 4 * 1000
         << "us" << endl;
  }

  cout << "test ok" << endl;

  return 0;
}