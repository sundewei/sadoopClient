package com.sap.mains;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.task.ITask;
import com.sap.mains.robject.EventLogRow;
import com.sap.mapred.db.HanaDBOutputFormat;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 7/5/12
 * Time: 10:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class CsvDbMrLoader implements ITask {
    private static final String PROCES_NAME = "mapReduceJavaLoader";

    private static int NUM_OF_REDUCER;
    private static String TABLE;
    private static String DB_DRIVER;
    private static String CONNECTION_STRING;
    private static String DB_USER;
    private static String DB_PASSWORD;
    private static String CSV_HDFS_PATH;
    private static String HDFS_USER;
    private static String HDFS_PASSWORD;

    public CsvDbMrLoader() {
    }

    public static class LoaderMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable inKey, Text inValue, Context context)
                throws IOException, InterruptedException {
            context.write(inKey, inValue);
        }
    }

    public static class LoaderReducer extends Reducer<LongWritable, Text, EventLogRow, NullWritable> {
        private static final NullWritable OUT_VALUE = NullWritable.get();

        public void reduce(LongWritable inKey, Iterable<Text> inValues, Context context)
                throws IOException, InterruptedException {
            String line = inValues.iterator().next().toString();
            try {
                String[] values = CSVUtils.parseLine(line);
                EventLogRow row = new EventLogRow();
                row.dateString = values[0];
                row.userId = values[1];
                row.otherId = values[2];
                row.eventType = Integer.parseInt(values[3]);
                row.comments = values[4];
                context.write(row, OUT_VALUE);
            } catch (ArrayIndexOutOfBoundsException ae) {
                System.out.println("\n\n\n\n\nFound ArrayIndexOutOfBoundsException, line = \n " + line + "\n");
                ae.printStackTrace();
            }
        }
    }

    public static Connection getConnection() throws Exception {
        Class.forName(DB_DRIVER).newInstance();
        return DriverManager.getConnection(CONNECTION_STRING, DB_USER, DB_PASSWORD);
    }

    public Job getMapReduceJob() throws Exception {
        final ConfigurationManager configurationManager = new ConfigurationManager(HDFS_USER, HDFS_PASSWORD);
        final Configuration configuration = configurationManager.getConfiguration();

        Job job = new Job(configuration, "Event Log MR DB Loader for " + CSV_HDFS_PATH);
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        DBConfiguration.configureDB(job.getConfiguration(), DB_DRIVER, CONNECTION_STRING, DB_USER, DB_PASSWORD);
        configuration.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.sap.db.jdbc.Driver");

        FileInputFormat.addInputPath(job, new Path(CSV_HDFS_PATH));
        job.setOutputFormatClass(HanaDBOutputFormat.class);
        HanaDBOutputFormat.setOutput(job, TABLE, EventLogRow.FIELDS);

        job.setJarByClass(CsvDbMrLoader.class);

        job.setMapperClass(CsvDbMrLoader.LoaderMapper.class);
        job.setReducerClass(CsvDbMrLoader.LoaderReducer.class);

        // Map's outputs
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(EventLogRow.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(NUM_OF_REDUCER);

        return job;
    }

    private static void truncateTable(Connection connection) throws Exception {
        String query = "truncate table " + TABLE;
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    public static void main(String[] arg) throws Exception {
        Properties properties = new Properties();
        if (arg.length > 0) {
            properties.load(new FileInputStream(arg[0]));
        }

        String batchId = properties.getProperty("batchId");
        String hadoopPlatform = properties.getProperty("hadoopPlatform");
        String executionEnvironment = properties.getProperty("executionEnvironment");
        int numOfMapper = Integer.parseInt(properties.getProperty("numOfMapper"));

        boolean enableStat = Boolean.parseBoolean(properties.getProperty("enableStat", "false"));
        boolean enableDetailedStat = Boolean.parseBoolean(properties.getProperty("enableDetailedStat", "false"));
        long statFreqMs = Long.parseLong(properties.getProperty("statFreqMs", "20000"));

        TABLE = properties.getProperty("table");
        DB_DRIVER = properties.getProperty("dbDriver");
        CONNECTION_STRING = properties.getProperty("connectionString");
        DB_USER = properties.getProperty("dbUser");
        DB_PASSWORD = properties.getProperty("dbPassword");
        CSV_HDFS_PATH = properties.getProperty("csvHdfsPath");
        NUM_OF_REDUCER = Integer.parseInt(properties.getProperty("numOfReducer"));
        HDFS_USER = properties.getProperty("hdfsUser");
        HDFS_PASSWORD = properties.getProperty("hdfsPassword");

        Connection connection = getConnection();

        System.out.println("About to truncate " + TABLE);
        truncateTable(connection);
        System.out.println("Done truncating " + TABLE);
        CsvDbMrLoader csvDbMrLoader = new CsvDbMrLoader();
        MainUtility.BenchmarkResult result = new MainUtility.BenchmarkResult();
        result.batchId = batchId;
        result.hadoopPlatform = hadoopPlatform;
        result.executionEnvironment = executionEnvironment;
        result.loadingMethod = PROCES_NAME;
        result.setExecutionId();

        long startMs = System.currentTimeMillis();
        csvDbMrLoader.getMapReduceJob().waitForCompletion(true);
        long endMs = System.currentTimeMillis();

        result.hadoopNumOfMapper = numOfMapper;
        result.executionTimeMs = (endMs - startMs);
        result.startTimeMs = startMs;
        result.endTimeMs = endMs;
        result.numOfRows = MainUtility.getRowCount(connection, TABLE);
        result.hadoopNumOfReducer = NUM_OF_REDUCER;
        MainUtility.insertBenchmarkResult(connection, result);
        connection.close();

        System.out.println("\n\n\n\n\n\n\n\n\n\n\n");
        System.out.println("The input file is: " + CSV_HDFS_PATH);
        System.out.println("Loading took " + (endMs - startMs) + " milliseconds");
    }
}
