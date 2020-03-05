package com.alsa.kafka.customerPartitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/7
 * @Time:20:05
 */
public class CustomPartitioner implements Partitioner {

    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
