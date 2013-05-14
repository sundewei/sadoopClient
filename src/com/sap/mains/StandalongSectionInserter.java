package com.sap.mains;

import com.sap.demo.pos.DatabaseUtility;
import com.sap.hadoop.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 4/27/12
 * Time: 11:07 AM
 * To change this template use File | Settings | File Templates.
 */
public class StandalongSectionInserter {
    public static void main(String[] arg) throws Exception {
        ConfigurationManager cm = new ConfigurationManager("lroot", "abcd1234");
        Configuration hBaseConfiguration = HBaseConfiguration.create(cm.getConfiguration());

        HBaseAdmin hBaseAdmin = new HBaseAdmin(hBaseConfiguration);
        HbaseHelper.createTable(hBaseAdmin, "sections", Arrays.asList("c"), true);
        HTable sectionsTable = new HTable(hBaseAdmin.getConfiguration(), "sections");

        Connection conn = DatabaseUtility.getCprConnection();
        PreparedStatement stmt = conn.prepareStatement(" INSERT INTO SYSTEM.SECTIONS VALUES ( ?, ?, ?, ?, ?)");
        BufferedReader reader = new BufferedReader(new FileReader("z:\\data\\sections.tsv"));
        String line = reader.readLine();
        long lineCount = 0;
        int batchCount = 0;
        List<Put> puts = new ArrayList<Put>(1000);
        while (line != null) {

            String[] values = line.split("\t");
            stmt.setLong(1, Long.parseLong(values[0]));
            long parentId = -1;
            try {
                parentId = Long.parseLong(values[1]);
            } catch (Exception e) {
            }
            stmt.setLong(2, parentId);
            stmt.setInt(3, Integer.parseInt(values[2]));
            stmt.setLong(4, Long.parseLong(values[3]));
            if (values.length >= 5) {
                stmt.setString(5, values[4]);
            } else {
                stmt.setObject(5, null);
            }
            stmt.addBatch();


            Put put = new Put(Bytes.toBytes(values[0]));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("section_id"), Bytes.toBytes(Long.parseLong(values[0])));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("parent_id"), Bytes.toBytes(parentId));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("level"), Bytes.toBytes(Integer.parseInt(values[2])));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("article_id"), Bytes.toBytes(Integer.parseInt(values[3])));
            if (values.length >= 5) {
                put.add(Bytes.toBytes("c"), Bytes.toBytes("name"), Bytes.toBytes(values[4]));
            }
            puts.add(put);

            lineCount++;
            batchCount++;

            if (lineCount % 1000 == 0) {
                stmt.executeBatch();
                batchCount = 0;
            }
            if (puts.size() >= 1000) {
                sectionsTable.put(puts);
                puts = new ArrayList<Put>(1000);
            }
            line = reader.readLine();
        }
        if (batchCount > 0) {
            stmt.executeBatch();
        }
        stmt.close();
        conn.close();
        if (puts.size() > 0) {
            sectionsTable.put(puts);
        }
    }

}
