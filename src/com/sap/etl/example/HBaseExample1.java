package com.sap.etl.example;

import com.sap.hadoop.conf.ConfigurationManager;
import org.apache.commons.csv.CSVUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 8/9/11
 * Time: 2:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseExample1 {
    private static HTable createTable(HBaseAdmin hbase, String tableName, Collection<String> columns, boolean dropIfExist) throws Exception {
        // Get a descriptor of the table first
        HTableDescriptor desc = new HTableDescriptor(tableName);

        // Add a column family of wanted columns to the descriptor
        for (String column : columns) {
            System.out.println("Adding " + column + " as a column...");
            HColumnDescriptor colDesc = new HColumnDescriptor(Bytes.toBytes(column));
            desc.addFamily(colDesc);
        }

        // Drop the table if needed
        if (dropIfExist && hbase.tableExists(tableName)) {
            System.out.println("About to drop table: " + tableName + "...Sleeping for 3 second before continuing");
            Thread.sleep(3 * 1000);
            if (!hbase.isTableDisabled(tableName)) {
                hbase.disableTable(tableName);
            }
            hbase.deleteTable(tableName);
        }

        // now create the table
        if (!hbase.tableExists(tableName)) {
            hbase.createTable(desc);
        }

        // return a reference
        return new HTable(hbase.getConfiguration(), tableName);
    }


    private static void loadData(HTable table, String filename) throws Exception {
        List<Put> putList = new ArrayList<Put>();
        List<String> lines = FileUtils.readLines(new File(filename));
        for (String line: lines) {
            String[] fields = CSVUtils.parseLine(line);
            String rowKey = null;
            Put row = null;
            for (String field: fields) {
                if (rowKey == null) {
                    rowKey = field;
                    row = new Put(Bytes.toBytes(rowKey));
                } else {
                    String[] keyValue = field.split("=");
                    row.add(Bytes.toBytes("attributes"), Bytes.toBytes(keyValue[0]), Bytes.toBytes(keyValue[1]));
                }
            }
            putList.add(row);
        }
        table.put(putList);
        table.flushCommits();
    }



    public static void main(String[] arg) throws Exception {
        ConfigurationManager cm = new ConfigurationManager("I827779", "hadoopsap");
        Configuration conf = cm.getConfiguration();
        Configuration hconf = HBaseConfiguration.create(conf);
        HBaseAdmin hbase = new HBaseAdmin(hconf);

        String[] columnFamilies = new String[1];
        columnFamilies[0] = "attributes";

        // Create the table
        HTable htable = createTable(hbase, "game_objects", Arrays.asList(columnFamilies), true);

        // Load the data from CSV
        loadData(htable, "c:\\data\\exported_games.csv");

        // Try to find an object based on the id as a number
        findGObject(htable, 110575L);
    }


    public static void findGObject(HTable htable, long id) throws Exception {
        Get row = new Get(getPadded(String.valueOf(id), "0", 10, false).getBytes());
        Result result = htable.get(row);
        List<KeyValue> keyValues = result.getColumn(Bytes.toBytes("attributes"), Bytes.toBytes("other_minimum_requirements"));
        System.out.println("Bytes.toString(keyValue.getValue())="+Bytes.toString(keyValues.get(0).getValue()));
    }


    private static String getPadded(String from, String padChar, int length, boolean append) {
        StringBuilder sb = new StringBuilder(from);
        while (sb.length() < length) {
            if(append) {
                sb.append(padChar);
            } else {
                sb.insert(0, padChar);
            }
        }
        return sb.toString();
    }
}

