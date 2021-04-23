package com.alsa.inputFormat.NLineInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/23
 * @Time: 9:18
 * @Description:
 */
public class NLineMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    private Text k = new Text();
    private LongWritable v = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String []splited = line.split(",");

        for (int i=0;i<splited.length;i++){
            k.set(splited[i]);
            context.write(k,v);
        }
    }
}
