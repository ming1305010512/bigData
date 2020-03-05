package com.alsa.kafka.producerApi;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/7
 * @Time:19:55
 * @Description: 创建生产者带回调函数
 */
public class CallBackProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","node2:9092");
        props.put("acks","all");
        props.put("retries",0);
        props.put("batch.size",16384);
        props.put("linger.ms",1);
        props.put("buffer.memory",33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(props);
        for (int i=0;i<50;i++){
            kafkaProducer.send(new ProducerRecord<String, String>("first", "hello" + i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata!=null){
                        System.err.println(recordMetadata.partition()+"------"+recordMetadata.offset());
                    }
                }
            });
        }
    }
}
