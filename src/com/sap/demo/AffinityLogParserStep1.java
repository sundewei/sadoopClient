package com.sap.demo;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.conf.IFileSystem;
import com.sap.hadoop.task.ITask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/3/11
 * Time: 9:31 PM
 * To change this template use File | Settings | File Templates.
 */
public class AffinityLogParserStep1 implements ITask {

    private static String REDUCE_VALUE_DELIMITER = "_";

    private static String INPUT_PATH;
    private static String OUTPUT_PATH;

    public AffinityLogParserStep1(String inputPath) {
        if (!inputPath.endsWith("/")) {
            inputPath = inputPath + "/";
        }
        INPUT_PATH = inputPath;
    }

    /**
     * The reduce class
     */
    public static class Step1Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        private static final Text EMPTY_STRING_KEY = new Text("");
        private static final Text ITEM_LOOKUP_CSV = new Text();
        private TreeMap<Long, String> timedItemLookupMap = new TreeMap<Long, String>();
        private int SESSION_LENGTH_MIN = -1;

        protected void init(Context context) {
            if (SESSION_LENGTH_MIN <= 0) {
                SESSION_LENGTH_MIN = context.getConfiguration().getInt("SESSION_LENGTH_MIN", 30);
            }
        }

        public void reduce(Text inKey, Iterable<Text> inValues, Context context)
                throws IOException, InterruptedException {
            init(context);
            timedItemLookupMap = new TreeMap<Long, String>();
            Iterator<Text> inValueIt = inValues.iterator();
            while (inValueIt.hasNext()) {
                String[] values = inValueIt.next().toString().split(REDUCE_VALUE_DELIMITER);
                timedItemLookupMap.put(Long.parseLong(values[0]), values[1]);
            }

            List<Session> sessions = Utility.getSessions(timedItemLookupMap, SESSION_LENGTH_MIN);
            for (Session session : sessions) {
                if (session.timestamps.size() > 0) {
                    StringBuilder sb = new StringBuilder();
                    for (String itemLookup: session.itemLookups) {
                        sb.append(itemLookup).append(",");
                    }
                    // Set the first timestamp at the end
                    sb.append(Utility.SIMPLE_DATE_FORMAT.format(session.timestamps.get(0)));
                    ITEM_LOOKUP_CSV.set(sb.toString());
                    context.write(EMPTY_STRING_KEY, ITEM_LOOKUP_CSV);
                }
            }
        }
    }

    /**
     * The map class, the content of the log is fed as line number (KEY) and line content (VALUE)
     */
    public static class Step1Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
        private static final Text MAP_OUT_VALUE = new Text();
        private static final Text MAP_OUT_KEY = new Text();
        private static final StringBuilder BUFFER = new StringBuilder();

        public void map(LongWritable inKey, Text inValue, Context context)
                throws IOException, InterruptedException {
            AccessEntry accessData = Utility.getAccessEntry(inValue.toString());
            if (accessData != null) {
                String itemLookup = Utility.getItemLookup(accessData.getAttribute("resource"));
                if (itemLookup != null) {
                    BUFFER.setLength(0);
                    MAP_OUT_KEY.set(accessData.getAttribute("ip"));
                    BUFFER.append(accessData.getAttribute("timestamp")).append(REDUCE_VALUE_DELIMITER).append(itemLookup);
                    MAP_OUT_VALUE.set(BUFFER.toString());
                    context.write(MAP_OUT_KEY, MAP_OUT_VALUE);
                }
            }
        }
    }

    public Job getMapReduceJob() throws Exception {
        final ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "hadoop");
        final Configuration conf = configurationManager.getConfiguration();

        Job job = new Job(conf, "AffinityLogParserStep1 for " + INPUT_PATH);

        conf.setInt("SESSION_LENGTH_MIN", 30);

        OUTPUT_PATH = INPUT_PATH + "sessionItems/";
        Utility.initOutputPath(configurationManager.getFileSystem(), OUTPUT_PATH);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.setJarByClass(AffinityLogParserStep1.class);

        job.setMapperClass(Step1Mapper.class);
        job.setReducerClass(Step1Reducer.class);

        // Read from a folder of log access
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        // Write to a folder with many files
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        // Map's outputs
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reduce's outputs
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public String getOutputPath() {
        return OUTPUT_PATH;
    }

    public static void main(String[] arg) throws Exception {
        AffinityLogParserStep1 step1 = new AffinityLogParserStep1(arg[0]);
        step1.getMapReduceJob().waitForCompletion(true);
    }
}
