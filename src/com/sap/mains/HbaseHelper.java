package com.sap.mains;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 4/27/12
 * Time: 11:09 AM
 * To change this template use File | Settings | File Templates.
 */
public class HbaseHelper {

    public static void createTable(HBaseAdmin hBaseAdmin, String tableName,
                                   List<String> columnFamilies, boolean dropIfExist) throws IOException {
        if (columnFamilies.size() == 0) {
            throw new IOException("Column family count cannot be 0.");
        }

        // Get a descriptor of the table first
        HTableDescriptor desc = getHTableDescriptor(tableName, columnFamilies);

        // Drop the table if needed
        if (dropIfExist && hBaseAdmin.tableExists(tableName)) {
            dropTable(hBaseAdmin, tableName);
        }

        // now create the table
        if (!hBaseAdmin.tableExists(tableName)) {
            hBaseAdmin.createTable(desc);
        }
    }

    public static HTableDescriptor getHTableDescriptor(String tableName, Collection<String> columnFamilies) {
        // Get a descriptor of the table first
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

        // Add a column family of wanted columns to the descriptor
        for (String columnFamilie : columnFamilies) {
            HColumnDescriptor colDesc = new HColumnDescriptor(Bytes.toBytes(columnFamilie));
            hTableDescriptor.addFamily(colDesc);
        }

        return hTableDescriptor;
    }

    public static HTable getHTable(HBaseAdmin hBaseAdmin, String tableName) throws IOException {
        if (hBaseAdmin.tableExists(tableName)) {
            // return a reference
            return new HTable(hBaseAdmin.getConfiguration(), tableName);
        } else {
            return null;
        }
    }

    public static void dropTable(HBaseAdmin hBaseAdmin, String tableName) throws IOException {
        sleepQuietly(3000);
        if (!hBaseAdmin.isTableDisabled(tableName)) {
            hBaseAdmin.disableTable(tableName);
        }
        hBaseAdmin.deleteTable(tableName);
    }

    public static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            // do nothing
        }
    }
}
