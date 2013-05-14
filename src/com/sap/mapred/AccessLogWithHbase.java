package com.sap.mapred;

import com.sap.demo.AccessEntry;
import com.sap.demo.Session;
import com.sap.demo.Utility;
import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.task.ITask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/9/12
 * Time: 10:56 AM
 * To change this template use File | Settings | File Templates.
 */
public class AccessLogWithHbase implements ITask {
    private static final String REDUCE_VALUE_DELIMITER = ",";

    /**
     * The map class
     */
    public static class AccessLogMapper extends TableMapper<Text, Text> {
        private static final Text MAP_OUT_VALUE = new Text();
        private static final Text MAP_OUT_KEY = new Text();
        private static final StringBuilder BUFFER = new StringBuilder();

        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            // process data for the row from the Result instance.
            AccessEntry accessEntry = getAccessEntry(value);
            String itemLookup = getItemAsin(accessEntry.getAttribute("resource"));
            if (itemLookup != null) {
                BUFFER.setLength(0);
                MAP_OUT_KEY.set(accessEntry.getAttribute("ip"));
                BUFFER.append(accessEntry.getAttribute("timestamp")).append(REDUCE_VALUE_DELIMITER).append(itemLookup);
                MAP_OUT_VALUE.set(BUFFER.toString());
                context.write(MAP_OUT_KEY, MAP_OUT_VALUE);
            }
        }
    }

    private static AccessEntry getAccessEntry(Result value) {
        AccessEntry accessEntry = new AccessEntry();
        accessEntry.resource = Bytes.toString(value.getColumnLatest(Bytes.toBytes("cf"), Bytes.toBytes("resource")).getValue());
        accessEntry.timestamp = new Timestamp(Bytes.toLong(value.getColumnLatest(Bytes.toBytes("cf"), Bytes.toBytes("timestamp")).getValue()));
        accessEntry.ip = Bytes.toString(value.getColumnLatest(Bytes.toBytes("cf"), Bytes.toBytes("ip")).getValue());
        return accessEntry;
    }

    public static String getItemAsin(String line) {
        String key = "PPSID=";
        int dpIdx = line.indexOf("PPSID=");
        int idEnd = line.indexOf("&", dpIdx + 1);
        if (idEnd < 0) {
            line.indexOf("\"", dpIdx + 1);
            idEnd = line.length();
        }
        if (dpIdx >= 0) {
            return line.substring(dpIdx + key.length(), idEnd);
        }
        return null;
    }

    /**
     * The reduce class
     */
    public static class AccessLogReducer extends Reducer<Text, Text, Text, Text> {
        private static final Text EMPTY_STRING_VALUE = new Text("");
        private static final Text ITEM_LOOKUP_CSV = new Text();

        public void reduce(Text inKey, Iterable<Text> inValues, Context context)
                throws IOException, InterruptedException {
            TreeMap<Long, String> timedItemLookupMap = new TreeMap<Long, String>();
            Iterator<Text> inValueIt = inValues.iterator();
            while (inValueIt.hasNext()) {
                String reduceValue = inValueIt.next().toString();
                String[] values = reduceValue.split(REDUCE_VALUE_DELIMITER);
                timedItemLookupMap.put(Long.parseLong(values[0]), values[1]);
            }

            List<Session> sessions = Utility.getSessions(timedItemLookupMap, 30);
            for (Session session : sessions) {
                if (session.timestamps.size() > 0) {
                    StringBuilder sb = new StringBuilder();
                    for (String itemLookup : session.itemLookups) {
                        sb.append(itemLookup).append(",");
                    }
                    // Set the first timestamp at the end
                    sb.append(Utility.SIMPLE_DATE_FORMAT.format(session.timestamps.get(0)));
                    ITEM_LOOKUP_CSV.set(sb.toString());
                    context.write(ITEM_LOOKUP_CSV, EMPTY_STRING_VALUE);
                }
            }
        }
    }

    public Job getMapReduceJob() throws Exception {
        ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "abcd1234");
        Configuration configuration = configurationManager.getConfiguration();
        Configuration hbaseConfig = HBaseConfiguration.create(configuration);
        Job job = new Job(hbaseConfig, "AccessLogFromHbase");
        job.setJarByClass(AccessLogWithHbase.class);     // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("resource"));
        scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ip"));
        scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("timestamp"));
        scan.setFilter(getFilterList("true"));

        TableMapReduceUtil.initTableMapperJob(
                "accessLog2012",        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                AccessLogMapper.class,   // mapper
                Text.class,             // mapper output key
                Text.class,             // mapper output value
                job);

        job.setReducerClass(AccessLogReducer.class);    // reducer class
        job.setNumReduceTasks(5);    // at least one, adjust as required
        FileOutputFormat.setOutputPath(job, new Path("/data/sessions"));  // adjust directories as required

        // Now set the TextOutputFormat as our OutputFormat class, again,
        // TextOutputFormat is a subclass of FileOutputFormat so the OutputPath
        // we set to FileOutputFormat will be used here
        job.setOutputFormatClass(TextOutputFormat.class);

        // Reducer's output key-value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static void main(String[] arg) throws Exception {
        AccessLogWithHbase accessLogWithHbase = new AccessLogWithHbase();
        accessLogWithHbase.getMapReduceJob().waitForCompletion(true);
    }

    private FilterList getFilterList(String metadataFlag) {
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("md"), Bytes.toBytes("sessionization"), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(metadataFlag));

        RegexStringComparator comp = new RegexStringComparator("^/productDetails.jsp\\?PPSID=.*");
        Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("resource"), CompareFilter.CompareOp.EQUAL, comp);

        list.addFilter(filter1);
        list.addFilter(filter2);
        return list;
    }
}
