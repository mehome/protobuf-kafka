objects = kafka-proto.o message.pb-c.o
CXXFLAGS = -c -g -Ddebug
all: kafka-proto.o message.pb-c.o
	gcc -o test $(objects) -lprotobuf-c -lrdkafka -lz -lpthread -lrt
kafka-proto.o:
	gcc $(CXXFLAGS) kafka-proto.c
message.pb-c.o:
	gcc $(CXXFLAGS) message.pb-c.c message.pb-c.h
.PHONY : clean
clean :
	rm test $(objects)
