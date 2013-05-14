package com.sap.demo;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.task.ITask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/23/12
 * Time: 3:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class ItemSessionCounter implements ITask {

    private static String INPUT_PATH;
    private static String OUTPUT_PATH;

    public ItemSessionCounter(String inputPath) {
        if (!inputPath.endsWith("/")) {
            inputPath = inputPath + "/";
        }
        INPUT_PATH = inputPath;
    }

    public static class ItemSessionMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Text MAP_OUT_KEY = new Text();
        private static final IntWritable ONE = new IntWritable(1);

        public void map(LongWritable inKey, Text inValue, Context context)
                throws IOException, InterruptedException {
            String line = inValue.toString();
            line = line.trim();
            String[] csvValues = line.split(",");
            // Last one is the date string
            String dateString = csvValues[csvValues.length - 1];
            for (int i = 0; i < csvValues.length - 1; i++) {
                MAP_OUT_KEY.set(dateString + "," + csvValues[i]);
                context.write(MAP_OUT_KEY, ONE);
            }
        }
    }

    public static class ItemSessionReducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, NullWritable> {
        private static final Text REDUCE_OUT_KEY = new Text();
        private static final NullWritable NULL = NullWritable.get();

        public void reduce(Text inKey, Iterable<IntWritable> inValues, Context context)
                throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable count : inValues) {
                total += count.get();
            }
            REDUCE_OUT_KEY.set(inKey.toString() + "," + total);
            context.write(REDUCE_OUT_KEY, NULL);
        }
    }


    public Job getMapReduceJob() throws Exception {
        final ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "hadoop");
        final Configuration conf = configurationManager.getConfiguration();

        Job job = new Job(conf, "SessionCounter: " + INPUT_PATH);

        if (INPUT_PATH.endsWith("/")) {
            OUTPUT_PATH = INPUT_PATH + "counts/";
        } else {
            OUTPUT_PATH = INPUT_PATH + "/counts/";
        }

        Utility.initOutputPath(configurationManager.getFileSystem(), OUTPUT_PATH);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.setJarByClass(AffinityLogParserStep1.class);

        job.setMapperClass(ItemSessionMapper.class);
        job.setReducerClass(ItemSessionReducer.class);

        // Read from a folder of log access
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        // Write to a folder with many files
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        // Map's outputs
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reduce's outputs
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job;
    }

    public static void main(String[] arg) throws Exception {
        ItemSessionCounter itemSessionCounter = new ItemSessionCounter(arg[0]);
        itemSessionCounter.getMapReduceJob().waitForCompletion(true);
    }
}
