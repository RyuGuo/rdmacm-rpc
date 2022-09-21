CPPFLAGS= -ggdb
LDLIBS=-lrdmacm -libverbs -lpci -lefa -lpthread

all: client server

client: rdma_conn.o rdma_msg.o
server: rdma_conn.o rdma_msg.o

rdma_conn.o : rdma_conn.h
rdma_msg.o : rdma_conn.h

.PHONY: all

.PHONY: clean
clean:
	rm rdma_conn.o rdma_msg.o client server