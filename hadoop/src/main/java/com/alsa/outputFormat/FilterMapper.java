package com.alsa.outputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/25
 * @Time: 15:20
 * @Description:
 */
public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {

        // 写出
        context.write(value, NullWritable.get());
    }
}