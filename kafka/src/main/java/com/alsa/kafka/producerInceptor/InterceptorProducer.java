package com.alsa.kafka.producerInceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/8
 * @Time:13:33
 * @Description: producer主程序
 */
public class InterceptorProducer {
    public static void main(String[] args) {
        //设置配置信息
        Properties properties = new Properties();
        properties.put("bootstrap.servers","node1:9092");
        properties.put("acks","all");
        properties.put("retries",0);
        properties.put("batch.size",16384);
        properties.put("linger.ms",1);
        properties.put("buffer.memory",33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //构建拦截链
        List<String> interceptors = new ArrayList<String>();
        interceptors.add("com.alsa.kafka.producerInceptor.TimeInterceptor");
        interceptors.add("com.alsa.kafka.producerInceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);
        String topic = "first";
        Producer<String,String> producer = new KafkaProducer<String, String>(properties);
        //发送消息
        for (int i=0;i<10;i++){
            ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,"message"+i);
            producer.send(record);
        }
        //一定要关闭producer，这样才会调用interceptor的close方法
        producer.close();
    }
}
