package com.alsa.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/4
 * @Time:15:57
 * @Description: hBaseApi
 */
public class HBaseApi {

    private static  Configuration conf;
    static {
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.1.128");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }


    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        // HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param columnFamily  列族
     * @throws IOException
     */
    public static void createTable(String tableName,String ...columnFamily) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (isTableExist(tableName)){
            System.out.println("表"+tableName+"已存在");
        }else {
            //创建表属性对象，表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for(String cf : columnFamily){
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表"+tableName+"创建成功");
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (isTableExist(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表"+tableName+"删除成功");
        }else {
            System.out.println("表"+tableName+"不存在");
        }
    }

    /**
     * 向表中插入数据
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName,String rowkey,String columnFamily,String column,String value) throws IOException {
        //创建Table对象
        HTable hTable = new HTable(conf,tableName);
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowkey));
        //向put对象中组装数据
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }

    /**
     * 删除多行数据
     * @param tableName 表名
     * @param rows 主键
     * @throws IOException
     */
    public static void deleteMultiRow(String tableName,String ...rows) throws IOException {
        HTable hTable = new HTable(conf,tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row: rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
        System.out.println("删除数据成功");
    }

    /**
     * 获取所有数据
     * @param tableName
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf,tableName);
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultScanner实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner){
            Cell[] cells = result.rawCells();
            for (Cell cell : cells){
                //得到rowkey
                System.out.println("行建："+ Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族："+Bytes.toString(CellUtil.cloneFamily(cell)));
                //列
                System.out.println("列："+Bytes.toString(CellUtil.cloneQualifier(cell)));
                //值
                System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * 获取某一行数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName,String rowKey) throws IOException {
        HTable hTable = new HTable(conf,tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = hTable.get(get);
        for (Cell cell : result.rawCells()){
            System.out.println("行键："+Bytes.toString(result.getRow()));
            System.out.println("列族："+Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列："+Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳："+cell.getTimestamp());

        }
    }

    /**
     * 获取某一行指定“列族：列”的数据
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public static void getRowQualifier(String tableName,String rowKey,String family,String qualifier) throws IOException {
        HTable hTable = new HTable(conf,tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier));
        Result result = hTable.get(get);
        for (Cell cell: result.rawCells()){
            System.out.println("行键："+Bytes.toString(result.getRow()));
            System.out.println("列族："+Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列："+Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

}
