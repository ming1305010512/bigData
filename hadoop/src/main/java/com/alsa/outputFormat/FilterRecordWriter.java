package com.alsa.outputFormat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/25
 * @Time: 15:12
 * @Description:
 */
public class FilterRecordWriter extends RecordWriter {

    FSDataOutputStream atguiguOut = null;
    FSDataOutputStream otherOut = null;

    public FilterRecordWriter(TaskAttemptContext job) {
        // 1 获取文件系统
        FileSystem fs;

        try {
            fs = FileSystem.get(job.getConfiguration());

            // 2 创建输出文件路径
            Path atguiguPath = new Path("file/shuffle/outputFormat/atguigu.log");
            Path otherPath = new Path("file/shuffle/outputFormat/other.log");

            // 3 创建输出流
            atguiguOut = fs.create(atguiguPath);
            otherOut = fs.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Object key, Object value) throws IOException, InterruptedException {
        //判断是否包含“atguigu”输出到不同文件
        if (key.toString().contains("atguigu")){
            atguiguOut.write(key.toString().getBytes());
        }else {
            otherOut.write(key.toString().getBytes());
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // 关闭资源
        if (atguiguOut != null) {
            atguiguOut.close();
        }
        if (otherOut != null) {
            otherOut.close();
        }
    }
}
