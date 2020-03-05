package com.alsa.kafka.consumerApi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/7
 * @Time:20:13
 * @Description: 创建消费者
 */
public class CustomConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //定义kafka服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers","node1:9092");
        //指定consumer group
        props.put("group.id","test");
        //是否自动确任offset
        props.put("enable.auto.commit","true");
        //自动确认offset的时间间隔
        props.put("auto.commit.interval.ms","1000");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //定义consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        //消费者订阅的topic，可同时订阅多个
        consumer.subscribe(Arrays.asList("first","second","third"));
        while (true){
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record :records){
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
