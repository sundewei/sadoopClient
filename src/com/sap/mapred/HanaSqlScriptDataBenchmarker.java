package com.sap.mapred;

import com.sap.demo.pos.DatabaseUtility;
import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.mapred.input.RecursiveFilenameInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/21/13
 * Time: 1:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class HanaSqlScriptDataBenchmarker {
    private static final String INPUT_PATH = "/data/posDataSmall/";

    public static class MyMapper extends Mapper<NullWritable, Text, Text, Text> {
        private static final Text KEY = new Text();
        private static final Text VALUE = new Text();

        public void map(NullWritable inKey, Text filename, Context context)
                throws IOException, InterruptedException {
            Connection connection = null;
            String query = " IMPORT FROM '/usr/local/mountedHdfs%s%s%s' \n" +
                    " WITH THREADS 1 \n" +
                    " BATCH 1000000";
            System.out.println("query====" + String.format(query, INPUT_PATH, Path.SEPARATOR, filename.toString()));
            try {
                connection = DatabaseUtility.getConnection();
                PreparedStatement pstmt = connection.prepareStatement(String.format(query, INPUT_PATH, Path.SEPARATOR, filename.toString()));
                long start = System.currentTimeMillis();
                pstmt.execute();
                long end = System.currentTimeMillis();
                pstmt.close();
                connection.close();
                KEY.set(filename.toString());
                VALUE.set(String.valueOf((end - start)));
                context.write(KEY, VALUE);
            } catch (Exception e) {
                IOException ioe = new IOException(e.getMessage());
                ioe.setStackTrace(e.getStackTrace());
            }
        }
    }

    /**
     * The reduce class
     */
    public static class MyReducer extends Reducer<Text, Text, Text, Text> {


        public void reduce(Text inKey, Iterable<Text> execTimeMs, Context context)
                throws IOException, InterruptedException {
            context.write(inKey, execTimeMs.iterator().next());
        }
    }

    private static void truncateTable(String table) throws Exception {
        Connection conn = DatabaseUtility.getConnection();
        String query = " truncate table " + table;
        PreparedStatement pstmt = conn.prepareStatement(query);
        pstmt.execute();
        pstmt.close();
        conn.close();
    }

    public static void main(String[] arg) throws Exception {
        final ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "abcd1234");
        final Configuration configuration = configurationManager.getConfiguration();

        truncateTable("SYSTEM.POS_ROWS_SS");

        Job job = new Job(configuration, "DataLoadingBenchmarkViaSqlScript: " + INPUT_PATH);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileInputFormat.setInputPathFilter(job, CtlPathFilter.class);

        Path outPath = new Path(INPUT_PATH + Path.SEPARATOR + "result");
        outPath.getFileSystem(configuration).delete(outPath, true);

        FileOutputFormat.setOutputPath(job, new Path(INPUT_PATH + Path.SEPARATOR + "result"));

        job.setJarByClass(HanaSqlScriptDataBenchmarker.class);

        job.setInputFormatClass(RecursiveFilenameInputFormat.class);

        job.setMapperClass(HanaSqlScriptDataBenchmarker.MyMapper.class);
        job.setReducerClass(HanaSqlScriptDataBenchmarker.MyReducer.class);

        // Map's outputs
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }

    static class CtlPathFilter implements PathFilter {
        public boolean accept(Path path) {
            return !path.getName().endsWith(".csv");
        }
    }
}


