package com.alsa.inputFormat.CustomerInputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/23
 * @Time: 14:36
 * @Description:
 */
public class SequenceFileReducer extends Reducer<Text, BytesWritable,Text,BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,values.iterator().next());
    }
}
