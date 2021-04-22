package com.alsa.serialize;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/22
 * @Time: 14:07
 * @Description:
 */
public class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    FlowBean v = new FlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String [] fields = line.split(",");

        String phoneNum = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        k.set(phoneNum);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        v.setSumFlow(upFlow+downFlow);

        context.write(k,v);
    }
}
