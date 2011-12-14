package com.sap.demo;

import com.sap.demo.robject.AffinityRow;
import com.sap.demo.robject.HanaDBOutputFormat;
import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.task.ITask;
import org.apache.commons.csv.CSVUtils;
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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/3/11
 * Time: 10:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class AffinityLogParserStep2 implements ITask {

    private static String INPUT_PATH;

    public AffinityLogParserStep2(String inputPath) {
        INPUT_PATH = inputPath;
    }

    /**
     * The map class, the content of the log is fed as line number (KEY) and line content (VALUE)
     */
    public static class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text MAP_OUT_VALUE = new Text();
        private static final Text MAP_OUT_KEY = new Text();
        private static final StringBuilder BUFFER = new StringBuilder();

        public void map(LongWritable inKey, Text inValue, Context context)
                throws IOException, InterruptedException {
            String line = inValue.toString().trim();
            String[] itemLookups = CSVUtils.parseLine(line);

            // The session start millisecond is at the end of the CSV
            String dateString = itemLookups[itemLookups.length - 1];

            for (int i = 0; i < itemLookups.length - 1; i++) {
                String itemLookup1 = itemLookups[i];
                for (int j = 0; j < itemLookups.length - 1; j++) {
                    String itemLookup2 = itemLookups[j];
                    if (!itemLookup2.equals(itemLookup1)) {
                        BUFFER.append(itemLookup2).append(",");
                    }
                }
                if (BUFFER.length() > 0) {
                    // Delete the last comma
                    BUFFER.deleteCharAt(BUFFER.length() - 1);

                    // Set the Map output key and value
                    MAP_OUT_KEY.set(itemLookup1 + "_" + dateString);
                    MAP_OUT_VALUE.set(BUFFER.toString());

                    // Write to the context
                    context.write(MAP_OUT_KEY, MAP_OUT_VALUE);
                }
                // Clear up the buffer
                BUFFER.setLength(0);
            }
        }
    }

    /**
     * The reduce class
     */
    public static class Step2Reducer extends Reducer<Text, Text, AffinityRow, NullWritable> {
        private static final NullWritable REDUCE_VALUE = NullWritable.get();

        public void reduce(Text inKey, Iterable<Text> inValues, Context context)
                throws IOException, InterruptedException {
            Map<String, Map<String, Integer>> dailyAffinityItemCountMap = new HashMap<String, Map<String, Integer>>();
            Map<String, Integer> dailySessionCount = new HashMap<String, Integer>();
            String mainItemLookup = inKey.toString().split("_")[0];
            String dateString = inKey.toString().split("_")[1];
            for (Text text : inValues) {
                String[] affItemLookups = CSVUtils.parseLine(text.toString());
                int sessionCount = 1;
                if (dailySessionCount.containsKey(dateString)) {
                    sessionCount += dailySessionCount.get(dateString);
                }
                dailySessionCount.put(dateString, sessionCount);

                Map<String, Integer> affinityItemCountMap = dailyAffinityItemCountMap.get(dateString);
                if (affinityItemCountMap == null) {
                    affinityItemCountMap = new HashMap<String, Integer>();
                }
                for (int i = 0; i < affItemLookups.length - 1; i++) {
                    String affItemLookup = affItemLookups[i];
                    Integer count = 1;

                    if (affinityItemCountMap.containsKey(affItemLookup)) {
                        count += affinityItemCountMap.get(affItemLookup);
                    }
                    affinityItemCountMap.put(affItemLookup, count);
                }
                dailyAffinityItemCountMap.put(dateString, affinityItemCountMap);
            }

            for (Map.Entry<String, Map<String, Integer>> entry : dailyAffinityItemCountMap.entrySet()) {
                dateString = entry.getKey();
                Map<String, Integer> itemCountMap = entry.getValue();
                for (Map.Entry<String, Integer> entryInner : itemCountMap.entrySet()) {
                    AffinityRow ar = new AffinityRow();
                    ar.calendarDate = dateString;
                    ar.itemLookup = mainItemLookup;
                    ar.affinityItemLookup = entryInner.getKey();
                    ar.affinityCount = entryInner.getValue();
                    ar.sessionCount = dailySessionCount.get(dateString);
                    context.write(ar, REDUCE_VALUE);
                }
            }
        }
    }

    public Job getMapReduceJob() throws Exception {
        final ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "hadoop");
        final Configuration conf = configurationManager.getConfiguration();
        Job job = new Job(conf, "AffinityLogParserStep2");

        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        DBConfiguration.configureDB(job.getConfiguration(), "com.sap.db.jdbc.Driver", "jdbc:sap://llnpal056:35015/SYSTEM", "SYSTEM", "Hadoophana123");
        conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.sap.db.jdbc.Driver");

        String inputPath = INPUT_PATH;
        FileInputFormat.addInputPath(job, new Path(inputPath));

        job.setJarByClass(AffinityLogParserStep2.class);

        job.setMapperClass(AffinityLogParserStep2.Step2Mapper.class);
        job.setReducerClass(AffinityLogParserStep2.Step2Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HanaDBOutputFormat.class);

        // Map's outputs
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reduce's outputs
        job.setOutputKeyClass(AffinityRow.class);
        job.setOutputValueClass(NullWritable.class);

        HanaDBOutputFormat.setOutput(job, "SYSTEM.SESSION_ITEM_AFFINITY", AffinityRow.FIELDS);

        return job;
    }

    public static void main(String[] arg) throws Exception {
        AffinityLogParserStep2 step2 = new AffinityLogParserStep2(arg[0]);
        step2.getMapReduceJob().waitForCompletion(true);
    }
}