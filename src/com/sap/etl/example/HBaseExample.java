package com.sap.etl.example;

import com.sap.data.GObject;
import com.sap.data.GobParser;
import com.sap.hadoop.conf.ConfigurationManager;
import org.apache.commons.csv.CSVUtils;
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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 7/22/11
 * Time: 10:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseExample {
    private static HTable getTable(HBaseAdmin hbase, String tableName, Collection<String> columns, boolean dropIfExist) throws Exception {
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

    private static List<Put> getPutList(Map<Long, GObject> gobMap) throws Exception {
        List<Put> puts = new ArrayList<Put>();
        Set<String> distIds = new HashSet<String>();
        for (Map.Entry<Long, GObject> entry: gobMap.entrySet()) {
            GObject gob = entry.getValue();
            //String rowKey = getRowKeyV1(gob);
            String rowKey = getRowKeyV2(gob);

            // Assign the rowkey
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes("attributes"), Bytes.toBytes("gob_id"), Bytes.toBytes(gob.getId()));

            // Add the names
            if (gob.getCommonName() != null) {
                put.add(Bytes.toBytes("attributes"), Bytes.toBytes("common_name"), Bytes.toBytes(gob.getCommonName()));
            }
            put.add(Bytes.toBytes("attributes"), Bytes.toBytes("name"), Bytes.toBytes(gob.getName()));

            for (Map.Entry<String, String> nEntry: gob.getLocalizedNames().entrySet()) {
                put.add(Bytes.toBytes("attributes"),
                        Bytes.toBytes(nEntry.getKey().toLowerCase().replace(" ", "_")),
                        Bytes.toBytes(nEntry.getValue()));
            }

            for (Map.Entry<String, String> nEntry: gob.getAlias().entrySet()) {
                put.add(Bytes.toBytes("attributes"),
                        Bytes.toBytes(nEntry.getKey().toLowerCase().replace(" ", "_")),
                        Bytes.toBytes(nEntry.getValue()));
            }

            for (Map.Entry<String, String> nEntry: gob.getMiscNames().entrySet()) {
                put.add(Bytes.toBytes("attributes"),
                        Bytes.toBytes(nEntry.getKey().toLowerCase().replace(" ", "_")),
                        Bytes.toBytes(nEntry.getValue()));
            }
            int idx = 0;
            // add the attributes
            put.add(Bytes.toBytes("attributes"), Bytes.toBytes("platform_name"), Bytes.toBytes(gob.getPlatformName()));
            for (String attribute: gob.getCollection()) {
                put.add(Bytes.toBytes("attributes"), Bytes.toBytes(String.valueOf(idx)), Bytes.toBytes(attribute));
                idx++;
            }
            puts.add(put);
            distIds.add(rowKey);
        }

        System.out.println("Found "+puts.size()+" games...");
        System.out.println("Found "+distIds.size()+" dist ids...");

        return puts;
    }

    private static String getRowKeyV1(GObject gob) {
        // iPhone___________________Zombie_Gunship_______________________________________________________________________________________________________________________________________________________0000113916
        String platformName = getPadded(gob.getPlatformName().replace(" ", "_"), "_", 25, true);
        String name = getPadded(gob.getName().replace(" ", "_"), "_", 165, true);
        String id = getPadded(String.valueOf(gob.getId()), "0", 10, false);
        StringBuilder sb = new StringBuilder(200);
        sb.append(platformName).append(name).append(id);
        return sb.toString();
    }

    private static String getRowKeyV2(GObject gob) {
        // 0014354839
        return getPadded(String.valueOf(gob.getId()), "0", 10, false);
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


    public static void main(String[] arg) throws Exception {
        ConfigurationManager cm = new ConfigurationManager("I827779", "hadoopsap");
        Configuration conf = cm.getConfiguration();
        Configuration hconf = HBaseConfiguration.create(conf);
        HBaseAdmin hbase = new HBaseAdmin(hconf);

        String[] columnFamilies = new String[1];
        columnFamilies[0] = "attributes";
        HTable htable = getTable(hbase, "game_objects", Arrays.asList(columnFamilies), true);

        GobParser parser = new GobParser();
        Map<Long, GObject> gobMap = parser.getGobMap();
        List<Put> puts = getPutList(gobMap);
        htable.put(puts);
        addInfo(htable);
        htable.flushCommits();
        exportGobject(htable);
        //findGObject(htable, 110575L);
    }

    private static void addInfo(HTable htable) throws Exception {
        List<Put> putList = new ArrayList<Put>();
        List<String[]> publisherInfo = getCsvContent("c:\\data\\gamePublisher.csv");
        List<String[]> releaseDateInfo = getCsvContent("c:\\data\\gameReleaseDate.csv");
        List<String[]> attributes = getCsvContent("c:\\data\\gameAttributes.csv");
        //String rowKeyCheck = null;

        for (String[] pubInfo: publisherInfo) {
            String id = pubInfo[0].trim();
            String rowKey = getPadded(id, "0", 10, false);
            Put put = new Put(Bytes.toBytes(rowKey));
            byte[] family = Bytes.toBytes("attributes");
            put.add(family, Bytes.toBytes("publisher_id"), Bytes.toBytes(pubInfo[1]));
            put.add(family, Bytes.toBytes("publisher_name"), Bytes.toBytes(pubInfo[2]));
            putList.add(put);
        }

        for (String[] relDateInfo: releaseDateInfo) {
            String id = relDateInfo[0].trim();
            String rowKey = getPadded(id, "0", 10, false);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes("attributes"), Bytes.toBytes("us_release_date"), Bytes.toBytes(relDateInfo[1]));
            putList.add(put);
        }

        for (String[] att: attributes) {
            String id = att[0].trim();
            String rowKey = getPadded(id, "0", 10, false);
            Put put = new Put(Bytes.toBytes(rowKey));
            for (int i=1; i<att.length; i++) {
                String[] ats = att[i].split("=");
                put.add(Bytes.toBytes("attributes"), Bytes.toBytes(ats[0].trim().toLowerCase().replace(" ", "_")), Bytes.toBytes(ats[1]));
            }
            putList.add(put);
        }
        htable.put(putList);
        /*
        Get get = new Get(Bytes.toBytes(rowKeyCheck));
        get.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("publisher_id"));
        get.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("publisher_name"));
        Result result = htable.get(get);
        System.out.println("checking row key: "+rowKeyCheck);
        System.out.println("publisher_name size()="+result.getColumn(Bytes.toBytes("attributes"), Bytes.toBytes("publisher_name")).size());
        System.out.println("publisher_name=" + Bytes.toString(result.getColumn(Bytes.toBytes("attributes"), Bytes.toBytes("publisher_name")).get(0).getValue()));
        */
    }

    private static List<String[]> getCsvContent(String filename) throws Exception {
        List<String> lines = IOUtils.readLines(new FileInputStream(filename));
        List<String[]> contents = new ArrayList<String[]>();
        for (String line: lines) {
            contents.add(CSVUtils.parseLine(line));
        }
        return contents;
    }

    public static void findGObject(HTable htable, long id) throws Exception {
        Get row = new Get(getPadded(String.valueOf(id), "0", 10, false).getBytes());
        Result result = htable.get(row);
        System.out.println("result="+result);

        List<KeyValue> keyValues = result.getColumn("attributes".getBytes(), "other_minimum_requirements".getBytes());
        System.out.println("Bytes.toString(keyValue.getValue())="+Bytes.toString(keyValues.get(0).getValue()));
    }




    public static void exportGobject(HTable htable) throws Exception {
        Scan scan = new Scan();
        // Like the select statement
        FileOutputStream out = new FileOutputStream("c:\\data\\exported_games.csv");
        ResultScanner scanner = htable.getScanner(scan);
        StringBuilder sb = new StringBuilder();
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            String rowKey = Bytes.toString(result.getRow());
            sb.append(rowKey).append(",");
            Map<byte[], byte[]> attsMap = result.getFamilyMap(Bytes.toBytes("attributes"));
            for (Map.Entry<byte[], byte[]> entry: attsMap.entrySet()) {
                String key = Bytes.toString(entry.getKey());
                String val = null;
                if ("gob_id".equals(key)) {
                    val = String.valueOf(Bytes.toLong(entry.getValue()));
                } else {
                    val = Bytes.toString(entry.getValue());
                }
                sb.append("\"").append(key).append("=").append(val.replace("=", "\\=").replace("\"","\\\"")).append("\",");
            }
            sb.deleteCharAt(sb.length()-1).append("\n");
            IOUtils.write(sb.toString(), out);
            sb = new StringBuilder();

        }
        IOUtils.closeQuietly(out);
    }
}

