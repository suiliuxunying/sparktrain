package com.shao.spark.project.utils;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * HBase操作工具类 java工具类采用单例模式封装
 */
public class HBaseUtils {
    HBaseAdmin admin=null;
    Configuration configuration=null;

    /**
     * 私有构造方法
     */
    private HBaseUtils(){
        configuration =new Configuration();
        configuration.set("hbase.zookeeper.quorum","BugFly000:2181");
        configuration.set("hbase.rootdir","hdfs://BugFly000:8020/hbase");
        try {
            admin = new HBaseAdmin(configuration);
            }
        catch (IOException e){
            e.printStackTrace();
        }
    }
    private static HBaseUtils instance =null;
    //防止线程安全问题
    public static synchronized HBaseUtils getInstance(){
        if(null == instance){
            instance =new HBaseUtils();
        }
        return instance;

}

    /**
     * 根据表明获取hTable实例
     * @param tableName
     * @return
     */
    public HTable getTable(String tableName){
        HTable htable =null;
        try{
        htable =new HTable(configuration,tableName);
        }catch (IOException e){
            e.printStackTrace();
        }
        return htable;
    }

    /**
     * 添加一条记录到hbase
     * @param tableName hbase表名
     * @param rowkey hbaserow_key
     * @param cf hbase表的columnfamily
     * @param column hbase表的列
     * @param value 写入hdase的值
     */
    private void put(String tableName,String rowkey,String cf,String column,String value){
        HTable table=getTable(tableName);
        Put put =new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
       try{
        table.put(put);}catch (IOException e){
           e.printStackTrace();
       }
    }
    /**
     * 添加一条记录到hbase
     * @param tableName hbase表名
     * @param rowkey hbaserow_key
     * @param cf hbase表的columnfamily
     * @param column hbase表的列列表
     * @param value 写入hdase的值列表
     */
    private void puts(String tableName, String rowkey, String cf, ArrayList<String> column, ArrayList<String> value){
        HTable table=getTable(tableName);
        Put put =new Put(Bytes.toBytes(rowkey));
        for(int i=0;i<column.size();i++){
            put.add(Bytes.toBytes(cf),Bytes.toBytes(column.get(i)),Bytes.toBytes(value.get(i)));
        }
        try{
            table.put(put);}catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void main(String[] args){
    //HTable hTable=HBaseUtils.getInstance().getTable("course_clickcount");
    //System.out.println(hTable.getName().getNameAsString());
        String tableName="course_clickcount";
        String rowkey="20171111_88";
        String cf="info";
        ArrayList<String> column=new ArrayList<String>();
        ArrayList<String> value=new ArrayList<String>();
        String column1="click_count";
        String value1="2";
        column.add(column1);
        column.add(column1);
        value.add(value1);
        value.add(value1);
        HBaseUtils.getInstance().puts(tableName,rowkey,cf,column,value);
    }

}
