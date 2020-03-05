package com.alsa.mapReduce.takeHdfsToHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/6
 * @Time:19:14
 * @Description: 组装job
 */
public class Txt2FruitRunner extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        //得到configuration
        Configuration conf = this.getConf();
        //创建job任务
        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(Txt2FruitRunner.class);
        Path inPath = new Path("hdfs://node1:9000/input_fruit/fruit.tsv");
        FileInputFormat.addInputPath(job,inPath);
        //设置Mapper
        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        //设置reducer
        TableMapReduceUtil.initTableReducerJob("fruit_hdfs",WriteFruitMRFromTxtReducer.class,job);
        //设置reducer数量，最少一个
        job.setNumReduceTasks(1);
        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess){
            throw new IOException("Job running with error");
        }
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf,new Txt2FruitRunner(),args);
        System.exit(status);
    }
}
