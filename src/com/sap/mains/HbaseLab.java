package com.sap.mains;

import com.sap.hadoop.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 8/6/12
 * Time: 3:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class HbaseLab {
    public static void main(String[] arg) throws Exception {
        ConfigurationManager cm = new ConfigurationManager("hadoop", "abcd1234");
        Configuration hBaseConfiguration = HBaseConfiguration.create(cm.getConfiguration());

        HBaseAdmin hBaseAdmin = new HBaseAdmin(hBaseConfiguration);

        String[] columnFamilies =
                new String[]{"attributes"};
        //new String[] {"DATE_STRING", "USER_ID", "OTHER_ID", "EVENT_TYPE", "COMMENTS"};

        HbaseHelper.createTable(hBaseAdmin, "EVENT_LOGS", Arrays.asList(columnFamilies), true);

        HTable hTable = HbaseHelper.getHTable(hBaseAdmin, "EVENT_LOGS");
        List<Put> puts = new ArrayList<Put>(1000);

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(arg[0])));
        String line = reader.readLine();

        while (line != null) {
            String[] values = line.split(",");

            line = reader.readLine();
        }
    }
}
