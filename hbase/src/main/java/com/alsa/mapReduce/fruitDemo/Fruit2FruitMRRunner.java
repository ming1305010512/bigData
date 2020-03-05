package com.alsa.mapReduce.fruitDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/6
 * @Time:16:45
 * @Description: 组装运行job任务
 */
public class Fruit2FruitMRRunner extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        //得到configuration
        Configuration conf = this.getConf();
        //创建job任务
        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        //配置job
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);
        //设置Mapper，导入的是mapreduce下的
        TableMapReduceUtil.initTableMapperJob(
                "fruit",//数据源的表名
                scan,//scan扫描控制器
                ReadFruitMapper.class,//设置mapper类
                ImmutableBytesWritable.class,//设置mapper输出key类型
                Put.class,//设置mapper输出value类型
                job
        );
        //设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr",WriteFruitMRReducer.class,job);
        job.setJarByClass(Fruit2FruitMRRunner.class);
        //设置Reduce数量，最少一个
        job.setNumReduceTasks(1);
        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess){
            throw new IOException("Job running with error");
        }
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf,new Fruit2FruitMRRunner(),args);
        System.exit(status);
    }
}
