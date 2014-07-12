/*
 * =====================================================================================
 *
 *       Filename:  test-out.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/09/2014 08:07:05 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <librdkafka/rdkafka.h>
#include <errno.h>
#include<unistd.h>
#include "message.pb-c.h"
#define MAX_MSG_SIZE 1024

static rd_kafka_t *rk;
void process_message(rd_kafka_message_t *rkmessage){
    AMessage *msg;
    msg = amessage__unpack(NULL, rkmessage->len, rkmessage->payload);
    if(msg)
        fprintf(stderr, "hostname = %s", msg->hostname);
}
void do_consume(rd_kafka_conf_t *conf, rd_kafka_topic_conf_t *topic_conf, char *brokers, char *topic, int partition){
    rd_kafka_topic_t *rkt;
    char errstr[512];
    int64_t start_offset = -1 ;
    if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
                    errstr, sizeof(errstr)))) {
        fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
        exit(1);
    }
    if (rd_kafka_brokers_add(rk, brokers) == 0) {
        fprintf(stderr, "%% No valid brokers specified\n");
        exit(1);
    }
    rkt = rd_kafka_topic_new(rk, topic, topic_conf);
    if (rd_kafka_consume_start(rkt, partition, start_offset) == -1){
        fprintf(stderr, "%% Failed to start consuming: %s\n", rd_kafka_err2str(rd_kafka_errno2err(errno)));
        exit(1);
    }
    while (1) {
        rd_kafka_message_t *rkmessage;
        rkmessage = rd_kafka_consume(rkt, partition, 1000);
        if (!rkmessage)
            continue;
        process_message(rkmessage);
        rd_kafka_message_destroy(rkmessage);
    }
    rd_kafka_consume_stop(rkt, partition);
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);

}

static void msg_delivered (rd_kafka_t *rk, void *payload, size_t len, 
        int error_code, void *opaque, void *msg_opaque) {
    if (error_code)
        fprintf(stderr, "%% Message delivery failed: %s\n",
                rd_kafka_err2str(error_code));
}
void do_publish(rd_kafka_conf_t *conf, rd_kafka_topic_conf_t *topic_conf, 
        char *brokers, char *topic, int partition, int len, char *buf){
    rd_kafka_topic_t *rkt;
    rd_kafka_conf_set_dr_cb(conf, msg_delivered);
    char errstr[512];
    if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
        fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
        exit(1);
    }
    if (rd_kafka_brokers_add(rk, brokers) == 0) {
        fprintf(stderr, "%% No valid brokers specified\n");
        exit(1);
    }
    rkt = rd_kafka_topic_new(rk, topic, topic_conf);
    if (rd_kafka_produce(rkt, partition,RD_KAFKA_MSG_F_COPY, 
                buf, len, NULL, 0, NULL) == -1){
        fprintf(stderr, "%% Failed to produce to topic %s partition %i: %s\n", 
                rd_kafka_topic_name(rkt), partition, 
                rd_kafka_err2str(rd_kafka_errno2err(errno)));
        rd_kafka_poll(rk, 0);
    }
    rd_kafka_poll(rk, 0);
    while (rd_kafka_outq_len(rk) > 0)
        rd_kafka_poll(rk, 100);
    rd_kafka_destroy(rk);
}

int main(int argc, char **argv){
    char mode = 'C';
    int opt;
    int len;
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topic_conf;
    conf = rd_kafka_conf_new();
    topic_conf = rd_kafka_topic_conf_new();
    int partition = 0;
    char *brokers = "localhost:9092";
    char *topic = "proto";
    char *buf;
    AMessage msg = AMESSAGE__INIT;
    while ((opt = getopt(argc, argv, "PCb:h:o:l:")) != -1) {
        switch (opt) {
            case 'P':
            case 'C':
                mode = opt;
                break;
            case 'b':
                brokers = optarg;
                break;
            case 'h':
                msg.hostname = optarg;
                break;
            case 'o':
                msg.offset = atoi(optarg);
                break;
            case 'l':
                msg.length = atoi(optarg);
                break;
        }
    }
    if (mode == 'P'){
        len = amessage__get_packed_size(&msg);
        buf = malloc(len);
        amessage__pack(&msg,buf);
        do_publish(conf, topic_conf, brokers, topic, partition, len, buf);
        free(buf);
    }
    else if(mode == 'C'){
        do_consume(conf, topic_conf, brokers, topic, partition);
    }
    return 0;
}
