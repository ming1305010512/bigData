package com.alsa.outputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/25
 * @Time: 15:21
 * @Description:
 */
public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context)throws IOException, InterruptedException {

        String line = key.toString();
        line = line + "\r\n";

        k.set(line);

        context.write(k, NullWritable.get());
    }
}
