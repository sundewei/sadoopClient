package com.sap.demo;

import com.sap.demo.robject.HanaDBOutputFormat;
import com.sap.demo.robject.RequestRow;
import com.sap.hadoop.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/18/11
 * Time: 4:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class SessionFinder {

    private static String REDUCE_VALUE_DELIMITER = "_";

    /**
     * The reduce class
     */
    public static class SessionReducer extends Reducer<Text, Text, RequestRow, NullWritable> {
        private static final NullWritable REDUCE_VALUE = NullWritable.get();
        private TreeMap<Long, String> sortedMap = new TreeMap<Long, String>();
        private int SESSION_LENGTH_MIN = -1;

        protected void init(Context context) {
            if (SESSION_LENGTH_MIN <= 0) {
                SESSION_LENGTH_MIN = context.getConfiguration().getInt("SESSION_LENGTH_MIN", 30);
            }
        }

        public void reduce(Text inKey, Iterable<Text> inValues, Context context)
                throws IOException, InterruptedException {
            init(context);
            sortedMap = new TreeMap<Long, String>();
            Iterator<Text> inValueIt = inValues.iterator();
            while (inValueIt.hasNext()) {
                String[] values = inValueIt.next().toString().split(REDUCE_VALUE_DELIMITER);
                sortedMap.put(Long.parseLong(values[0]), values[1]);
            }

            List<Session> sessions = Utility.getSessions(sortedMap, SESSION_LENGTH_MIN);
            for (Session session : sessions) {
                for (int i = 0; i < session.timestamps.size(); i++) {
                    RequestRow rr = new RequestRow();
                    rr.ip = inKey.toString();
                    rr.requestMs = session.timestamps.get(i);
                    rr.startMs = session.timestamps.get(0);
                    rr.endMs = session.timestamps.get(session.timestamps.size() - 1);
                    rr.requestedPage = session.itemLookups.get(i);
                    rr.sessionLengthMs = rr.endMs - rr.startMs;
                    rr.itemLookup = Utility.getItemLookup(session.itemLookups.get(i));
                    rr.ipGeoNum = Utility.getIpGeoNum(rr.ip);
                    rr.sessionNum = rr.startMs + "_" + rr.ip;
                    context.write(rr, REDUCE_VALUE);
                }
            }
        }
    }

    /**
     * The map class, the content of the log is fed as line number (KEY) and line content (VALUE)
     */
    public static class SessionMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text MAP_OUT_VALUE = new Text();
        private static final Text MAP_OUT_KEY = new Text();
        private static final StringBuilder BUFFER = new StringBuilder();

        public void map(LongWritable inKey, Text inValue, Context context)
                throws IOException, InterruptedException {
            AccessEntry accessData = Utility.getAccessEntry(inValue.toString());
            if (accessData != null && Utility.getItemLookup(accessData.getAttribute("resource")) != null) {
                BUFFER.setLength(0);
                MAP_OUT_KEY.set(accessData.getAttribute("ip"));
                BUFFER.append(accessData.getAttribute("timestamp")).append(REDUCE_VALUE_DELIMITER).append(accessData.getAttribute("resource"));
                MAP_OUT_VALUE.set(BUFFER.toString());
                context.write(MAP_OUT_KEY, MAP_OUT_VALUE);
            }
        }
    }

    public static void main(String[] arg) throws Exception {
        final ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "hadoop");
        final Configuration conf = configurationManager.getConfiguration();
        Job job = new Job(conf, "SessionFinder");

        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        DBConfiguration.configureDB(job.getConfiguration(), "com.sap.db.jdbc.Driver", "jdbc:sap://llnpal056:35015/SPORTMART", "HAHADM", "Welcome1");
        conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.sap.db.jdbc.Driver");
        conf.setInt("SESSION_LENGTH_MIN", 30);

        String inputPath = configurationManager.getRemoteFolder() + "/accessLogs/";
        FileInputFormat.addInputPath(job, new Path(inputPath));

        job.setJarByClass(SessionFinder.class);

        job.setMapperClass(SessionMapper.class);
        job.setReducerClass(SessionReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HanaDBOutputFormat.class);

        // Map's outputs
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reduce's outputs
        job.setOutputKeyClass(RequestRow.class);
        job.setOutputValueClass(NullWritable.class);

        HanaDBOutputFormat.setOutput(job, "SPORTMART.SESSION_ITEMS", RequestRow.FIELDS);
        job.waitForCompletion(true);
    }
}
