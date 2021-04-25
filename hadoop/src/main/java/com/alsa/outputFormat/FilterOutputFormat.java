package com.alsa.outputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/25
 * @Time: 15:09
 * @Description:
 */
public class FilterOutputFormat extends FileOutputFormat<Text, NullWritable> {
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new FilterRecordWriter(job);
    }
}
