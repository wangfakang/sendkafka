CC ?= cc
CFLAGS += -g
CFLAGS += -Wall -Werror -Wfloat-equal -Wpointer-arith -O2 -I../
LDFLAGS += ./librdkafka-0.7/librdkafka.a
LDFLAGS += -lpthread -lrt -lz

# Profiling
#CFLAGS += -O0 -pg
#LDFLAGS += -pg

all: sendkafka


sendkafka: sendkafka.c
	@(test $@ -nt $< || \
	  $(CC) $(CFLAGS) $< -o $@ $(LDFLAGS))

install:
	install sendkafka /usr/local/bin/


clean:
	rm -rf *.o sendkafka
