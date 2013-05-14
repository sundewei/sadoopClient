package com.sap.demo;

import com.sap.demo.robject.HanaDBOutputFormat;
import com.sap.demo.robject.ItemSessionRow;
import com.sap.hadoop.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/28/12
 * Time: 9:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class ItemSessionAggregator {

    private static String INPUT_PATH;

    public ItemSessionAggregator(String inputPath) {
        INPUT_PATH = inputPath;
    }

    public static class DbReducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, ItemSessionRow, NullWritable> {
        private static final NullWritable REDUCE_OUT_VALUE = NullWritable.get();
        private static final ItemSessionRow REDUCE_OUT_KEY = new ItemSessionRow();

        public void reduce(Text inKey, Iterable<IntWritable> inValues, Context context)
                throws IOException, InterruptedException {
            String line = inKey.toString().trim();
            int sessionCount = 0;
            for (IntWritable inValue : inValues) {
                sessionCount += inValue.get();
            }
            String[] csvValues = line.split(",");
            String dateString = csvValues[0];
            String itemAsin = csvValues[1];
            REDUCE_OUT_KEY.dateString = dateString;
            REDUCE_OUT_KEY.itemAsin = itemAsin;
            REDUCE_OUT_KEY.sessionCount = sessionCount;
            context.write(REDUCE_OUT_KEY, REDUCE_OUT_VALUE);
        }
    }

    /**
     * The map class, the content of the log is fed as line number (KEY) and line content (VALUE)
     */
    public static class AggMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable MAP_OUT_VALUE = new IntWritable();
        private static final Text MAP_OUT_KEY = new Text();

        public void map(LongWritable inKey, Text inValue, Context context)
                throws IOException, InterruptedException {
            String line = inValue.toString().trim();
            String[] csvValues = line.split(",");
            String dateStringAsin = csvValues[0] + "," + csvValues[1];
            int count = Integer.parseInt(csvValues[2]);
            MAP_OUT_VALUE.set(count);
            MAP_OUT_KEY.set(dateStringAsin);
            context.write(MAP_OUT_KEY, MAP_OUT_VALUE);
        }
    }

    public Job getMapReduceJob() throws Exception {
        final ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "hadoop");
        final Configuration conf = configurationManager.getConfiguration();
        Job job = new Job(conf, "ItemSessionAggregator for " + INPUT_PATH);

        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        DBConfiguration.configureDB(job.getConfiguration(), "com.sap.db.jdbc.Driver", "jdbc:sap://llnpal056:35015/SYSTEM", "SYSTEM", "Hadoophana123");
        conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.sap.db.jdbc.Driver");


        conf.setStrings(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, ItemSessionRow.FIELDS);

        String inputPath = INPUT_PATH;
        FileInputFormat.addInputPath(job, new Path(inputPath));

        job.setJarByClass(AffinityLogParserStep2.class);

        job.setMapperClass(ItemSessionAggregator.AggMapper.class);
        job.setReducerClass(ItemSessionAggregator.DbReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HanaDBOutputFormat.class);

        // Map's outputs
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reduce's outputs
        job.setOutputKeyClass(ItemSessionRow.class);
        job.setOutputValueClass(NullWritable.class);

        HanaDBOutputFormat.setOutput(job, "HADOOP.ITEM_SESSIONS", ItemSessionRow.FIELDS);

        return job;
    }

    public static void main(String[] arg) throws Exception {
        ItemSessionAggregator aggregator = new ItemSessionAggregator(arg[0]);
        aggregator.getMapReduceJob().waitForCompletion(true);
    }
}
