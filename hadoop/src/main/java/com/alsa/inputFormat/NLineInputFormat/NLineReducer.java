package com.alsa.inputFormat.NLineInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/23
 * @Time: 9:50
 * @Description:
 */
public class NLineReducer extends Reducer<Text, LongWritable,Text,LongWritable> {

    LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0l;
        //汇总
        for (LongWritable value : values){
            sum += value.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
