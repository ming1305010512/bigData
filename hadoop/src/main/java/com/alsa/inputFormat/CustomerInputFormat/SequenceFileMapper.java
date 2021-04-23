package com.alsa.inputFormat.CustomerInputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/23
 * @Time: 14:34
 * @Description:
 */
public class SequenceFileMapper extends Mapper<Text, BytesWritable,Text,BytesWritable> {

    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
