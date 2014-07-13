/*
 * =====================================================================================
 *
 *       Filename:  protobuf_kafka.c
 *
 *    Description: 
 *
 *        Version:  1.0
 *        Created:  07/09/2014 08:07:05 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Jian Qiu, 
 *   Organization:  
 *
 * =====================================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
#include <errno.h>
#include<unistd.h>
#include "message.pb-c.h"
#define MAX_MSG_SIZE 1024

typedef struct MessageConf{
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topic_conf;
    char *brokers;
    int partition;
    char *topic;
} kafka_msg_t;

static kafka_msg_t *publish_conf;
static kafka_msg_t *reply_conf;
static rd_kafka_t *rk_producer;
static rd_kafka_t *rk_consumer;
void do_publish(kafka_msg_t *, int, char *);


int process_message(rd_kafka_message_t *rkmessage){
    AMessage *msg;
    msg = amessage__unpack(NULL, rkmessage->len, rkmessage->payload);
    if(msg){
        fprintf(stderr, "hostname = %s", msg->hostname);
        return 0;
    }
    return -1;
}

int process_reply(rd_kafka_message_t *rkmessage){
    if(rkmessage->len > 0 && 
            strcmp((char*)rkmessage->payload, "success") == 0){
        fprintf(stderr, "publish successful");
        return 1;
    }
    return -1;
}

void do_consume(kafka_msg_t *kafka_conf, int(*process_func)(rd_kafka_message_t *)){
    rd_kafka_topic_t *rkt;
    char errstr[512];
    int64_t start_offset = -1 ;
    int state;
    if (!rk_consumer && !(rk_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, kafka_conf->conf,
                    errstr, sizeof(errstr)))) {
        fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
        exit(1);
    }
    if (rd_kafka_brokers_add(rk_consumer, kafka_conf->brokers) == 0) {
        fprintf(stderr, "%% No valid brokers specified\n");
        exit(1);
    }
    rkt = rd_kafka_topic_new(rk_consumer, kafka_conf->topic, kafka_conf->topic_conf);
    if (rd_kafka_consume_start(rkt, kafka_conf->partition, start_offset) == -1){
        fprintf(stderr, "%% Failed to start consuming: %s\n", rd_kafka_err2str(rd_kafka_errno2err(errno)));
        exit(1);
    }
    while (1) {
        rd_kafka_message_t *rkmessage;
        rkmessage = rd_kafka_consume(rkt, kafka_conf->partition, 1000);
        if (!rkmessage)
            continue;
        state = process_func(rkmessage);
        rd_kafka_message_destroy(rkmessage);
        if(state == 1){
            break;
        }else if(state == 0){
            char *reply = "sucess";
            int len = strlen(reply);
            do_publish(reply_conf, len, reply);
        }
    }
    rd_kafka_consume_stop(rkt, kafka_conf->partition);
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk_consumer);

}

static void msg_delivered (rd_kafka_t *rk, void *payload, size_t len, 
        int error_code, void *opaque, void *msg_opaque) {
    if (error_code)
        fprintf(stderr, "%% Message delivery failed: %s\n",
                rd_kafka_err2str(error_code));
}

void do_publish(kafka_msg_t *kafka_conf, int len, char *buf){
    rd_kafka_topic_t *rkt;
    rd_kafka_conf_set_dr_cb(kafka_conf->conf, msg_delivered);
    char errstr[512];
    if (!rk_producer && !(rk_producer = rd_kafka_new(RD_KAFKA_PRODUCER, kafka_conf->conf, errstr, sizeof(errstr)))) {
        fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
        exit(1);
    }
    if (rd_kafka_brokers_add(rk_producer, kafka_conf->brokers) == 0) {
        fprintf(stderr, "%% No valid brokers specified\n");
        exit(1);
    }
    rkt = rd_kafka_topic_new(rk_producer, kafka_conf->topic, kafka_conf->topic_conf);
    if (rd_kafka_produce(rkt, kafka_conf->partition, RD_KAFKA_MSG_F_COPY, 
                buf, len, NULL, 0, NULL) == -1){
        fprintf(stderr, "%% Failed to produce to topic %s partition %i: %s\n", 
                rd_kafka_topic_name(rkt), kafka_conf->partition, 
                rd_kafka_err2str(rd_kafka_errno2err(errno)));
        rd_kafka_poll(rk_producer, 0);
    }
    rd_kafka_poll(rk_producer, 0);
    while (rd_kafka_outq_len(rk_producer) > 0)
        rd_kafka_poll(rk_producer, 100);
    rd_kafka_destroy(rk_producer);
    if(strcmp(kafka_conf->topic, reply_conf->topic) != 0 ){
        do_consume(reply_conf, process_reply);
    }
}

kafka_msg_t * kafka_msg_conf_new(int partition, char *broker, char *topic){
    kafka_msg_t *kafka_conf;
    kafka_conf = (kafka_msg_t *) malloc(sizeof(kafka_msg_t));
    kafka_conf->conf = rd_kafka_conf_new();
    kafka_conf->topic_conf = rd_kafka_topic_conf_new();
    kafka_conf->partition = partition;
    kafka_conf->brokers = broker;
    kafka_conf->topic = topic;
    return kafka_conf;
}

int main(int argc, char **argv){
    char mode = 'C';
    int opt;
    int len;
    int partition = 0;
    char *brokers = "localhost:9092";
    char *topic = "proto";
    char *reply_topic = "protoreply";
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
    publish_conf = kafka_msg_conf_new(partition, brokers, topic);
    reply_conf = kafka_msg_conf_new(partition, brokers, reply_topic);
    if (mode == 'P'){
        len = amessage__get_packed_size(&msg);
        buf = malloc(len);
        amessage__pack(&msg,buf);
        do_publish(publish_conf, len, buf);
        free(buf);
    }
    else if(mode == 'C'){
        do_consume(publish_conf, process_message);
    }
    return 0;
}
