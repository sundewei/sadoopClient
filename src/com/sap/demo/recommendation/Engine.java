package com.sap.demo.recommendation;

import com.sap.demo.Utility;
import com.sap.demo.robject.DatedItemSessionAffinity;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/9/11
 * Time: 3:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class Engine {

    public static class ItemAffinityMapper extends Mapper<LongWritable, DatedItemSessionAffinity, Text, IntWritable> {
        private static final Text OUTPUT_KEY = new Text();
        private static final IntWritable OUTPUT_VALUE = new IntWritable();

        public void map(LongWritable inKey, DatedItemSessionAffinity record, Context context)
                throws IOException, InterruptedException {

            context.write(OUTPUT_KEY, OUTPUT_VALUE);
        }
    }

    public static void getDatedItemAffinity() throws Exception {
        Connection conn = Utility.getConnection();
        String query1 =  " select to_char(calendar_date, 'YYYY-MM-DD'), \n" +
                         "        item_lookup, \n" +
                         "        affinity_item_lookup, \n" +
                         "        affinity_count," +
                         "        session_count \n" +
                         " from session_item_affinity " +
                         " order by 1 asc  ";
        PreparedStatement pstmt = conn.prepareStatement(query1);
System.out.println("About to run : \n" + query1);
        ResultSet rs = pstmt.executeQuery();
System.out.println("ResultSet ready, start to loop it.");
        int count = 0;
        String nowDateString = null;
        String oldDateString = null;
        StringBuilder sb = new StringBuilder();
        while (rs.next()) {
            ItemPair itemPair = new ItemPair();
            itemPair.dateString = rs.getString(1);
            itemPair.itemLookup = rs.getString(2);
            itemPair.affinityItemLookup = rs.getString(3);
            itemPair.affinityCount = rs.getInt(4);
            itemPair.sessionCount = rs.getInt(5);
            count++;
            nowDateString = itemPair.dateString;
            if (oldDateString == null) {
                oldDateString = nowDateString;
            }
            sb.append(itemPair.itemLookup).append(",").append(itemPair.affinityItemLookup).append(",").append(itemPair.affinityCount).append(",").append(itemPair.sessionCount).append("\n");
            if (!nowDateString.equals(oldDateString)) {
System.out.println("Working on " + oldDateString + " with row Count = " + count);
                String filename = "C:\\projects\\data\\itemAffinity\\" + oldDateString + ".csv";
                BufferedWriter out = new BufferedWriter(new FileWriter(filename, true));
                out.write(sb.toString());
                out.close();
                oldDateString = nowDateString;
                sb = new StringBuilder();
            }
        }

        // Write the last date string
        String filename = "C:\\projects\\data\\itemAffinity\\" + nowDateString + "csv";
        BufferedWriter out = new BufferedWriter(new FileWriter(filename, true));
        out.write(sb.toString());
        out.close();

        rs.close();
        pstmt.close();
        conn.close();
    }
}
