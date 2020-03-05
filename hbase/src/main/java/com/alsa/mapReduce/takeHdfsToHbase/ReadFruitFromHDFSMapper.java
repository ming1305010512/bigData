package com.alsa.mapReduce.takeHdfsToHbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/6
 * @Time:19:01
 * @Description: 读取HDFS中的文件数据
 */
public class ReadFruitFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //从HDFS中读取数据
        String lineValue = value.toString();
        //读取出来的每行数据使用\t进行分割，存于string数组
        String[] values = lineValue.split("\t");
        //根据数组中的含义取值
        String rowKey = values[0];
        String name = values[1];
        String color = values[2];
        //初始化rowKey
        ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(Bytes.toBytes(rowKey));
        //初始化put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //参数分别：列族，列，值
        put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(name));
        put.add(Bytes.toBytes("info"),Bytes.toBytes("color"),Bytes.toBytes(color));
        context.write(rowKeyWritable,put);
    }
}
