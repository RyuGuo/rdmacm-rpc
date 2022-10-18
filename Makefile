CPPFLAGS= -ggdb
LDLIBS=-lrdmacm -libverbs -lpci -lpthread

all: client server

OBJS=rdma_conn.o rdma_msg.o rdma_sched.o

client: ${OBJS}
server: ${OBJS}

${OBJS} : rdma_conn.h

.PHONY: all

.PHONY: clean
clean:
	rm ${OBJS} client server