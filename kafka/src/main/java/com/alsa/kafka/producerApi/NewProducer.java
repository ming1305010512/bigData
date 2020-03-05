package com.alsa.kafka.producerApi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/7
 * @Time:19:15
 * @Description:创建生产者
 */
public class NewProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //kafka服务端的主机名和端口号
        props.put("bootstrap.servers","node2:9092");
        //等待所有副本节点的应答
        props.put("acks","all");
        //消息发送最大尝试次数
        props.put("retries",0);
        //一批消息处理大小
        props.put("batch.size",16384);
        //请求延时
        props.put("linger.ms",1);
        //发送缓冲区内存大小
        props.put("buffer.memory",33554432);
        //key序列化
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //value序列化
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        Producer<String,String> producer = new KafkaProducer<String, String>(props);
        for (int i=0;i<50;i++){
            producer.send(new ProducerRecord<String, String>("first",Integer.toString(i),"hello world-"+i));
        }
        producer.close();
    }
}
