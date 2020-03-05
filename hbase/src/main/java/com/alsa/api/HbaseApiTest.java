package com.alsa.api;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/4
 * @Time:16:10
 */
public class HbaseApiTest {

    public static void main(String[] args) throws IOException {
        //=========判断表是否存在
        // boolean tableExist = HBaseApi.isTableExist("student");
        // if (tableExist){
        //     System.out.println("student表存在");
        // }else {
        //     System.out.println("student表不存在");
        // }
        //===========创建表
        // String[] columnFamily = {"info","situation"};
        // HBaseApi.createTable("student",columnFamily);
        //==========删除表
        // HBaseApi.dropTable("student");
        //==========向表中插入数据
        HBaseApi.addRowData("student","10003","info","name","zk");
        //==========删除多行数据
        // String [] rows = {"10001","10002"};
        // HBaseApi.deleteMultiRow("student",rows);
        //=========获取所有数据
        // HBaseApi.getAllRows("student");
        //=========获取某一行数据
        // HBaseApi.getRow("student","10002");
        //========获取某一行指定“列族：列”的数据
        // HBaseApi.getRowQualifier("student","10002","info","name");
    }
}
