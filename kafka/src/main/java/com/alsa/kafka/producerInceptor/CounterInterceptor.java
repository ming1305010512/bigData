package com.alsa.kafka.producerInceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/8
 * @Time:13:25
 * @Description: 统计发送消息成功和发送失败消息数，并在producer关闭时打印这两个计数器
 */
public class CounterInterceptor implements ProducerInterceptor<String,String> {

    private int errorCounter = 0;
    private int successCounter = 0;

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        //统计成功和失败的次数
        if (e==null){
            successCounter++;
        }else {
            errorCounter++;
        }
    }

    public void close() {
        //保存结果
        System.out.println("Successful sent: "+successCounter);
        System.out.println("Failed send: "+errorCounter);
    }

    public void configure(Map<String, ?> map) {

    }
}
