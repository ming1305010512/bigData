package com.alsa.kafka.consumerApi;

import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/8
 * @Time:9:39
 * @Description: 消费者低级api
 */
public class LowLevelConsumer {
    public static void main(String[] args) {
        //kafka集群
        ArrayList<String> brokers = new ArrayList<String>();
        brokers.add("node1");
        brokers.add("node2");
        brokers.add("node3");
        //连接kafka集群的端口号
        int port = 9092;
        String topic = "first";
        int partition = 0;
        //待消费的位置信息
        long offset = 0;
        getData(brokers,port,topic,partition,offset);
    }

    /**
     * 根据集群，主题，分区信息获取待消费的leader信息
     *
     * @param brokers
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    private static BrokerEndPoint getLeader(ArrayList<String> brokers,int port,String topic,int partition){
        for (String broker : brokers){
            //遍历集群，根据节点信息创建simpleConsumer
            SimpleConsumer getLeader = new SimpleConsumer(broker,port,2000,1024*4,"getLeader");
            //===============发送元数据信息请求
            //根据传入的主题信息创建元数据请求
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            //发送元数据请求得到返回值
            TopicMetadataResponse metadataResponse = getLeader.send(topicMetadataRequest);
            //===============解析元数据返回值
            List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();
            //遍历多个topic的元数据信息
            for (TopicMetadata topicMetadata : topicsMetadata){
                //一个topic由多个partition组成
                List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();
                //遍历多个分区的元数据信息
                for (PartitionMetadata partitionMetadata : partitionsMetadata){
                    //匹配传入的分区号
                    if (partition==partitionMetadata.partitionId()){
                        //匹配上则直接返回leader信息
                        return partitionMetadata.leader();
                    }
                }
            }
        }
        return null;
    }

    private static void getData(ArrayList<String> brokers,int port,String topic,int partition,long offset){
        //获取待消费分区的leader信息
        BrokerEndPoint leader = getLeader(brokers,port,topic,partition);
        if (leader==null){
            System.out.println("未找到指定主题指定分区的leader信息");
            return;
        }
        String leaderHost = leader.host();
        //根据leader信息创建simpleConsumer
        SimpleConsumer getData = new SimpleConsumer(leaderHost,port,2000,1024*1024*4,"getData");
        //==================发送抓取数据的请求
        //构建抓取数据的请求
        kafka.api.FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic,partition,offset,1024*1024*2).build();
        //发送抓取数据的请求并得到返回值
        FetchResponse fetchResponse = getData.fetch(fetchRequest);
        //================解析返回值
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic,partition);
        for (MessageAndOffset messageAndOffset : messageAndOffsets){
            ByteBuffer payLoad = messageAndOffset.message().payload();
            //将数据写入byte数据
            byte[] bytes = new byte[payLoad.limit()];
            payLoad.get(bytes);
            //打印结果
            System.out.println(new String(bytes));
        }
    }
}
