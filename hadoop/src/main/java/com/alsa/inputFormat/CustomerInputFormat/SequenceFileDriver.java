package com.alsa.inputFormat.CustomerInputFormat;

import com.alsa.serialize.FlowSumDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/23
 * @Time: 14:39
 * @Description: 将多个文件合并成一个文件
 */
public class SequenceFileDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //设置输入输出路径
        args = new String[]{"file/shuffle/inputFormat/CustomerInputFormat/input","file/shuffle/inputFormat/CustomerInputFormat/output"};

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //指定程序jar包所在路径
        job.setJarByClass(FlowSumDriver.class);

        //指定作业要使用的mapper/reducer类
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        //指定Mapper输出数据的k,v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //设置输入的InputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        //输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //指定最终输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //指定job的输入原始文件所在的目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
