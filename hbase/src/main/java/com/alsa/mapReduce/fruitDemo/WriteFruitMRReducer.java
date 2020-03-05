package com.alsa.mapReduce.fruitDemo;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/6
 * @Time:16:36
 * @Description: 将读取到的fruit表中的数据写入到fruit_mr表中
 */
public class WriteFruitMRReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        //读出来的每一行数据写入到fruit_mr表中
        for (Put put : values){
            context.write(NullWritable.get(),put);
        }
    }
}
