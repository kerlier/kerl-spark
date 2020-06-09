package com.fashion.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {

    HBaseAdmin hBaseAdmin = null;
    Configuration configuration = null;

    private HBaseUtil(){

        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","node1:2181");
        configuration.set("hbase.rootdir","hdfs://node1:8020/hbase");
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static HBaseUtil  instance = null;

    public static synchronized  HBaseUtil getInstance(){
        if(null == instance){
            instance = new HBaseUtil();
        }
        return instance;
    }


    public HTable getTable(String tableName){
        HTable table = null;
        try{
            table = new HTable(configuration,tableName);
        }catch (Exception e){
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 添加记录到hbase中
     * @param tableName 表名
     * @param rowKey rowKey
     * @param cf   列蔟
     * @param column 列
     * @param value 列的值
     */
    public void put(String tableName,String rowKey, String cf, String column,String value){
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));

        try {
            table.put(put);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HTable hTable = HBaseUtil.getInstance().getTable("course_clickcount");

        System.out.println(hTable.getName().getNameAsString());

        String tableName = "course_clickcount";

        String rowKey = "20201111_1";

        String cf = "info";

        String column = "click_count";

        String value = "2";

        HBaseUtil.getInstance().put(tableName,rowKey,cf,column,value);
    }
}
