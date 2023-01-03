CPPFLAGS= -ggdb
LDLIBS=-lrdmacm -libverbs -lpci -lpthread

all: client server

OBJS=src/rdma_conn.o src/rdma_msg.o src/rdma_sched.o

client: ${OBJS}
server: ${OBJS}

${OBJS} : rdma_conn.h

.PHONY: all

.PHONY: clean
clean:
	rm ${OBJS} client server