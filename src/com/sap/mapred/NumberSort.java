package com.sap.mapred;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.task.ITask;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class NumberSort implements ITask {

    private static String CMD_RANDOM_NUM_FILE = null;

    /**
     * The map class
     */
    public static class SortMap extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
        private final static LongWritable outKey = new LongWritable(1);
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException {
            String line = inValue.toString();
            String[] strNumbers = line.split(",");
            for (String strNumber : strNumbers) {
                long number = Long.parseLong(strNumber);
                outKey.set(number);
                // Each number will has a count of 1 as the output value
                context.write(outKey, one);
            }
        }
    }

    /**
     * The reduce class
     */
    public static class SortReducer
            extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
        public void reduce(LongWritable inKey, Iterable<IntWritable> inValues, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            Iterator<IntWritable> iterator = inValues.iterator();
            // Each number has a list of "one"s as the value, here we sum them up
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            IntWritable outValue = new IntWritable(count);
            context.write(inKey, outValue);
        }
    }

    public Job getMapReduceJob() throws Exception {
        // Get a configuration manager
        ConfigurationManager cm = new ConfigurationManager("I123456", "hadoopsap");

        String outputFolder = cm.getRemoteFolder() + "output/";
        String randomNumberFile = "randomNumbers.csv";

        if (CMD_RANDOM_NUM_FILE != null) {
            randomNumberFile = CMD_RANDOM_NUM_FILE;
        }

        // The output folder MUST NOT be created, Hadoop will do it automatically
        Path outputPath = new Path(outputFolder);
        FileSystem filesystem = outputPath.getFileSystem(cm.getConfiguration());
        // Delete the output directory if it already exists
        if (filesystem.exists(outputPath)) {
            filesystem.delete(outputPath, true);
        }

        Job job = new Job(cm.getConfiguration(), "sort1");
        // This is a must step to tell Hadoop to load the jar file containing this class
        job.setJarByClass(NumberSort.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(NumberSort.SortMap.class);
        job.setReducerClass(NumberSort.SortReducer.class);
        job.setCombinerClass(NumberSort.SortReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(cm.getRemoteFolder() + randomNumberFile));
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    public static void main(String[] args) throws Exception {
        NumberSort ns = new NumberSort();
        if (args != null && args.length >= 1) {
            CMD_RANDOM_NUM_FILE = args[0];
        }
        ns.getMapReduceJob().waitForCompletion(true);
    }
}
