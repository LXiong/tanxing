
.PHONY: default clean

default: ts

##########

CC=gcc

mysql_config_cflags=$(shell mysql_config --cflags)

mysql_config_libs=$(shell mysql_config --libs)

wloption=-Wl,-rpath=/home/work/.jumbo/lib

daemonize.o: daemonize.c daemonize.h

main.o: main.c daemonize.h cJSON.h
	${CC} -c main.c ${mysql_config_cflags}

cJSON.o: cJSON.c cJSON.h

OBJS=daemonize.o main.o cJSON.o

ts: ${OBJS}
	${CC} -o ts ${OBJS} ${mysql_config_libs} ${wloption}

clean:
	-rm *.o
	-rm ts

