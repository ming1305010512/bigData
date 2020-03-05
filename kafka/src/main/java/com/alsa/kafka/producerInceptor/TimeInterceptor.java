package com.alsa.kafka.producerInceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/8
 * @Time:10:28
 * @Description: 增加时间戳拦截器
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        //创建一个新的record，把时间戳写入消息体的最前部
        return new ProducerRecord<String, String>(producerRecord.topic(),producerRecord.partition(),producerRecord.timestamp(),producerRecord.key(),System.currentTimeMillis()+","+producerRecord.value().toString());
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
