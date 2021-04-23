package com.alsa.inputFormat.CustomerInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/23
 * @Time: 13:51
 * @Description: 自定义RecordReader类，将读取的信息key设置为路径+文件名称，value为每个文件的内容
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    private Configuration configuration;
    private FileSplit split;

    private boolean isProgress = true;

    private BytesWritable value = new BytesWritable();
    private Text k = new Text();

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (isProgress){
            byte [] contents = new byte[(int) split.getLength()];
            FileSystem fs = null;
            FSDataInputStream fis = null;
            try {
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);
                fis = fs.open(path);
                //读取文件内容
                IOUtils.readFully(fis,contents,0,contents.length);
                value.set(contents,0,contents.length);

                //获取文件路径及名称
                String name = split.getPath().toString();
                k.set(name);
            } catch (IOException e) {

            }finally {
                IOUtils.closeStream(fis);
            }
        }

        return false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}
