package com.sap.mapred;

import com.sap.demo.pos.DatabaseUtility;
import com.sap.mapred.db.HanaDBOutputFormat;
import com.sap.mapred.db.PosRow;
import org.apache.commons.csv.CSVUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/18/13
 * Time: 3:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class HanaMrDataBenchmarker {
    private static final String INPUT_PATH = "/data/posDataSmall/";

    public static class DbRecordMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private static final Text OUTPUT_VALUE = new Text();
        public static final List<IntWritable> KEYS = new ArrayList<IntWritable>();

        private static int index = 0;

        static {
            for (int i = 0; i < 4; i++) {
                KEYS.add(new IntWritable(i));
            }
        }

        public void map(LongWritable inKey, Text csvLine, Context context)
                throws IOException, InterruptedException {
            OUTPUT_VALUE.set(csvLine);
            context.write(getKey(), OUTPUT_VALUE);
        }

        private static IntWritable getKey() {
            index++;
            if (index == KEYS.size()) {
                index = 0;
            }
            return KEYS.get(index);
        }
    }

    /**
     * The reduce class
     */
    public static class DbRecordReducer extends Reducer<IntWritable, Text, PosRow, NullWritable> {
        private static final NullWritable NULL = NullWritable.get();

        public void reduce(IntWritable inKey, Iterable<Text> posLines, Context context)
                throws IOException, InterruptedException {
            for (Text posLine : posLines) {
                String[] csvValues = CSVUtils.parseLine(posLine.toString());
                PosRow posRow = new PosRow();
                posRow.transactionId = Long.parseLong(csvValues[0]);
                posRow.categoryId1 = Integer.parseInt(csvValues[1]);
                posRow.categoryId2 = Integer.parseInt(csvValues[2]);
                posRow.categoryName1 = csvValues[3];
                posRow.categoryName2 = csvValues[4];
                posRow.cost = Float.parseFloat(csvValues[5]);
                posRow.price = Float.parseFloat(csvValues[6]);
                posRow.quantity = Integer.parseInt(csvValues[7]);
                context.write(posRow, NULL);
            }
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
        //final ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "abcd1234");
        //final Configuration configuration = configurationManager.getConfiguration();
        final Configuration configuration = new Configuration();
        truncateTable("SYSTEM.POS_ROWS_MR");

        Job job = new Job(configuration, "DataLoadingBenchmarkViaDbInputFormat: " + INPUT_PATH);
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        DBConfiguration.configureDB(job.getConfiguration(), "com.sap.db.jdbc.Driver", "jdbc:sap://LSPAL134.pal.sap.corp:31015", "SYSTEM", "Hana1234");
        configuration.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.sap.db.jdbc.Driver");

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileInputFormat.setInputPathFilter(job, CtlPathFilter.class);

        Path outPath = new Path(INPUT_PATH + Path.SEPARATOR + "result");
        if (outPath.getFileSystem(configuration).exists(outPath)) {
            outPath.getFileSystem(configuration).delete(outPath, true);
        }

        job.setOutputFormatClass(HanaDBOutputFormat.class);
        HanaDBOutputFormat.setOutput(job, "SYSTEM.POS_ROWS_MR", PosRow.FIELDS);

        job.setJarByClass(HanaMrDataBenchmarker.class);

        job.setMapperClass(HanaMrDataBenchmarker.DbRecordMapper.class);
        job.setReducerClass(HanaMrDataBenchmarker.DbRecordReducer.class);

        // Map's outputs
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(PosRow.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(4);
        job.waitForCompletion(true);
    }

    static class CtlPathFilter implements PathFilter {
        public boolean accept(Path path) {
            return path.getName().endsWith("2011-01-01.csv") || path.getName().endsWith("posDataSmall");
            //return !path.getName().endsWith(".ctl");
        }
    }
}

