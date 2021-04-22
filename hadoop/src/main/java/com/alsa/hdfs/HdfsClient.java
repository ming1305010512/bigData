package com.alsa.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/22
 * @Time: 10:12
 * @Description:
 */
public class HdfsClient {

    @Test
    public void testMkdirs() throws IOException {
        //获取文件系统
        Configuration configuration = new Configuration();
        //配置在集群上运行
        configuration.set("fs.defaultFS","hdfs://node1:9000");
        FileSystem fs = FileSystem.get(configuration);
        fs.mkdirs(new Path("/1108/daxian/banzhang"));
        //关闭资源
        fs.close();
    }

    //测试文件下载
    @Test
    public void testCopyToLocalFile() throws IOException {
        //获取文件系统
        Configuration configuration = new Configuration();
        //配置在集群上运行
        configuration.set("fs.defaultFS","hdfs://node1:9000");
        FileSystem fs = FileSystem.get(configuration);
        fs.copyToLocalFile(false,new Path("/word.txt"),new Path("file/word.txt"),true);
        //关闭资源
        fs.close();
    }
}
