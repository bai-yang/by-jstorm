package com.a.eye.by.jstorm.hbase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseOperations implements Serializable {

    private static final long serialVersionUID = -2376564775951010029L;

    private static Configuration conf;

    private static Connection conn;

    static {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "127.0.0.1");
            conf.setInt("hbase.zookeeper.property.clientPort", 2182);
            conn = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createTable(String name, String...columns) {
        try {
            Admin admin = conn.getAdmin();
            TableName table = TableName.valueOf(name);
            HTableDescriptor tableDescriptor = new HTableDescriptor(table);
            for (String column : columns) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(column);
                tableDescriptor.addFamily(columnDescriptor);
            }
            admin.createTable(tableDescriptor);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insert(String name, String rowKey, Map<String, Map<String, Object>> record) {
        Put put = new Put(Bytes.toBytes(rowKey));

        TableName tableName = TableName.valueOf(name);
        try {
            Table table = conn.getTable(tableName);
            for (String cf : record.keySet()) {
                for (String column : record.get(cf).keySet()) {
                    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column),
                            Bytes.toBytes(record.get(cf).get(column).toString()));
                }
            }
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        List<String> zks = new ArrayList<String>();
        zks.add("127.0.0.1");
        Map<String, Map<String, Object>> record = new HashMap<String, Map<String, Object>>();
        Map<String, Object> cf1 = new HashMap<String, Object>();
        cf1.put("aa", "1");
        Map<String, Object> cf2 = new HashMap<String, Object>();
        cf2.put("bb", "1");
        record.put("cf1", cf1);
        record.put("cf2", cf2);

        HBaseOperations.createTable("by-storm", "cf1", "cf2");

        System.out.println("create table success");

        HBaseOperations.insert("by-storm", UUID.randomUUID().toString(), record);

        System.out.println("insert data success");

    }
}
