package com.sap.mains;

import com.sap.hadoop.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 4/26/12
 * Time: 2:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class SectionInserter {
    private static HTable SECTIONS;
    private static String INPUT_PATH;
    private static ConfigurationManager CONFM;
    private static HBaseAdmin HBASE_ADMIN;
    private static Configuration CONFIGURATION;
    private static Configuration HBASE_CONFIGURATION;

    public SectionInserter(String inPath) {
        INPUT_PATH = inPath;
    }

    public static class SectionReducer extends Reducer<Text, Text, NullWritable, NullWritable> {

        public void reduce(Text inKey, Iterable<Text> inValues, Context context)
                throws IOException, InterruptedException {
            List<Put> puts = new ArrayList<Put>();
            for (Text inValue : inValues) {
                String line = inValue.toString();
                if (line.length() > 0 && line.contains("\t")) {
                    System.out.println("inValue.toString()=" + inValue.toString());
                    String[] values = inValue.toString().split("\t");
                    Put put = new Put(Bytes.toBytes(values[0]));
                    put.add(Bytes.toBytes("c"), Bytes.toBytes("section_id"), Bytes.toBytes(Long.parseLong(values[0])));
                    long parentId = -1;
                    try {
                        parentId = Long.parseLong(values[1]);
                    } catch (Exception e) {
                    }
                    put.add(Bytes.toBytes("c"), Bytes.toBytes("parent_id"), Bytes.toBytes(parentId));
                    put.add(Bytes.toBytes("c"), Bytes.toBytes("level"), Bytes.toBytes(Integer.parseInt(values[2])));
                    put.add(Bytes.toBytes("c"), Bytes.toBytes("article_id"), Bytes.toBytes(Integer.parseInt(values[3])));
                    if (values.length == 5) {
                        put.add(Bytes.toBytes("c"), Bytes.toBytes("name"), Bytes.toBytes(values[4]));
                    }
                    puts.add(put);
                }
                if (puts.size() > 1000) {
                    getSectionHtable().put(puts);
                    puts = new ArrayList<Put>();
                }
            }
            if (puts.size() > 0) {
                getSectionHtable().put(puts);
            }
        }
    }

    public static class SectionMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text MAP_OUT_KEY = new Text("OUT");
        private static final Text MAP_OUT_VALUE = new Text();

        public void map(LongWritable inKey, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] values = inValue.toString().split("\t");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < values.length - 1; i++) {
                sb.append(values[i]);
                if (i < values.length - 2) {
                    sb.append("\t");
                }
            }
            MAP_OUT_VALUE.set(sb.toString());
            context.write(MAP_OUT_KEY, MAP_OUT_VALUE);
        }
    }

    private static HTable getSectionHtable() throws IOException {
        return new HTable(HBASE_CONFIGURATION, "sections");
    }

    public Job getMapReduceJob() throws Exception {
        CONFM = new ConfigurationManager("lroot", "abcd1234");
        CONFIGURATION = CONFM.getConfiguration();
        HBASE_CONFIGURATION = HBaseConfiguration.create(CONFIGURATION);
        HBASE_ADMIN = new HBaseAdmin(HBASE_CONFIGURATION);

        System.out.println("CONFM=" + CONFM);
        System.out.println("CONFIGURATION=" + CONFIGURATION);
        System.out.println("HBASE_CONFIGURATION=" + HBASE_CONFIGURATION);
        System.out.println("HBASE_ADMIN=" + HBASE_ADMIN);

        HbaseHelper.createTable(HBASE_ADMIN, "sections", Arrays.asList("c"), true);
        Job job = new Job(CONFIGURATION, "SectionInserter for " + INPUT_PATH);

        job.setJarByClass(SectionInserter.class);

        job.setMapperClass(SectionMapper.class);
        job.setReducerClass(SectionReducer.class);

        // Read from a folder of log access
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path("/user/lroot/ds/SectionInserter.job"));

        // Map's outputs
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reduce's outputs
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        return job;
    }

    public static void main(String[] arg) throws Exception {
        SectionInserter step1 = new SectionInserter(arg[0]);
        step1.getMapReduceJob().waitForCompletion(true);
    }


}
