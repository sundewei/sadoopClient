package com.sap.mapred;

import com.sap.hadoop.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
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

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 1/28/12
 * Time: 12:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class JsNumberSort {
    private static String CMD_RANDOM_NUM_FILE = null;

    private static String readHdfsFile(Configuration configuration, String filename) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(filename))));
        String line = reader.readLine();
        StringBuilder sb = new StringBuilder();
        while (line != null) {
            sb.append(line).append("\n");
            line = reader.readLine();
        }
        reader.close();
        return sb.toString();
    }


    /**
     * The map class
     */
    public static class SortMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
        protected final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        protected ScriptEngine scriptEngine;

        public void setup(Mapper<LongWritable, Text, LongWritable, IntWritable>.Context context)
                throws java.io.IOException, java.lang.InterruptedException {
            String hdfsMapJs = "/user/lroot/map.js";
            scriptEngine = scriptEngineManager.getEngineByName("JavaScript");
            try {
                String mapJsContent = readHdfsFile(context.getConfiguration(), hdfsMapJs);
                scriptEngine.eval(mapJsContent);
                scriptEngine.put("mapOutKey", new LongWritable());
                scriptEngine.put("mapOutValue", new LongWritable(0));
                System.out.println("Js Content = \n" + mapJsContent);

            } catch (ScriptException se) {
                IOException ioe = new IOException(se);
                ioe.setStackTrace(se.getStackTrace());
                throw ioe;

            }
        }

        public void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException {
            try {
                callFunction("map", inKey, inValue, context);
            } catch (Exception e) {
                IOException ioe = new IOException(e);
                ioe.setStackTrace(e.getStackTrace());
                throw ioe;
            }
        }

        private Object callFunction(String functionName, Object... args) throws Exception {
            return ((Invocable) scriptEngine).invokeFunction(functionName, args);
        }
    }

    /**
     * The reduce class
     */
    public static class SortReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
        protected final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        protected ScriptEngine scriptEngine;

        public void setup(Reducer<LongWritable, IntWritable, LongWritable, IntWritable>.Context context)
                throws java.io.IOException, java.lang.InterruptedException {
            String hdfsReduceJs = "/user/lroot/reduce.js";
            scriptEngine = scriptEngineManager.getEngineByName("JavaScript");
            try {
                String reduceJsContent = readHdfsFile(context.getConfiguration(), hdfsReduceJs);
                scriptEngine.eval(reduceJsContent);
                scriptEngine.put("reduceOutKey", new LongWritable());
                scriptEngine.put("reduceOutValue", new IntWritable(0));
                System.out.println("Js Content = \n" + reduceJsContent);
            } catch (ScriptException se) {
                IOException ioe = new IOException(se);
                ioe.setStackTrace(se.getStackTrace());
                throw ioe;
            }
        }

        public void reduce(LongWritable inKey, Iterable<IntWritable> inValues, Reducer.Context context)
                throws IOException, InterruptedException {
            try {
                callFunction("reduce", inKey, inValues, context);
            } catch (Exception e) {
                IOException ioe = new IOException(e);
                ioe.setStackTrace(e.getStackTrace());
                throw ioe;
            }
        }

        private Object callFunction(String functionName, Object... args) throws Exception {
            return ((Invocable) scriptEngine).invokeFunction(functionName, args);
        }
    }

    public Job getMapReduceJob() throws Exception {
        // Get a configuration manager
        ConfigurationManager cm = new ConfigurationManager("lroot", "abcd1234");

        String outputFolder = cm.getRemoteFolder() + "output/";
        String randomNumberFile = "randomNumbers.txt";

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

        Job job = new Job(cm.getConfiguration(), "sort in js...");
        // This is a must step to tell Hadoop to load the jar file containing this class
        job.setJarByClass(NumberSort.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(JsNumberSort.SortMapper.class);
        job.setReducerClass(JsNumberSort.SortReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(cm.getRemoteFolder() + randomNumberFile));
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    public static void main(String[] args) throws Exception {
        JsNumberSort ns = new JsNumberSort();
        if (args != null && args.length >= 1) {
            CMD_RANDOM_NUM_FILE = args[0];
        }
        ns.getMapReduceJob().waitForCompletion(true);
        //ns.callFunction("map", new LongWritable(1), new Text("30"), null);
    }
}
