package com.sap.mapred;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.conf.IFileSystem;
import com.sap.hadoop.task.ITask;
import org.apache.commons.csv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 8/17/11
 * Time: 2:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class LogParser implements ITask {
    private static final Logger LOG = Logger.getLogger(LogParser.class.getName());
    private static DateFormat DATA_FORMAT = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss Z]");

    private String outputPath;
    private String inputPath;

    public LogParser(String customizedFolder) {
        if (customizedFolder != null && !customizedFolder.endsWith(File.separator)) {
            customizedFolder = customizedFolder + File.separator;
        }
        this.inputPath = customizedFolder;
    }

    /**
     * The map class
     */
    public static class AccessParserMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text MAP_OUT_VALUE = new Text();
        private static final Text MAP_OUT_KEY = new Text();

        public void map(LongWritable inKey, Text inValue, Context context)
                throws IOException, InterruptedException {
            String line = inValue.toString();
            AccessData accessData = getAccessData(line);
            if (accessData != null && accessData.httpCode == 200) {
                MAP_OUT_KEY.set(accessData.ip);
                MAP_OUT_VALUE.set(accessData.timestamp.getTime() + "_" + getProductId(accessData.resource));
                context.write(MAP_OUT_KEY, MAP_OUT_VALUE);
            }
        }
    }

    private static String getProductId(String line) {
        int dpIdx = line.indexOf("/dp/");
        String id = null;
        if (line.endsWith("/")) {
            id = line.substring(dpIdx + 4, line.length() - 1);
        } else {
            id = line.substring(dpIdx + 4, line.length());
        }
        return id;
    }

    /**
     * The reduce class
     */
    public static class AccessParserReducer
            extends Reducer<Text, Text, Text, Text> {
        private static final Text VISITED_PRODUCTS = new Text();

        public void reduce(Text inKey, Iterable<Text> inValues, Context context)
                throws IOException, InterruptedException {
            Set<String> sortedValues = new TreeSet<String>();
            StringBuilder sb = new StringBuilder();
            while (inValues.iterator().hasNext()) {
                sortedValues.add(inValues.iterator().next().toString());
            }
            for (String value : sortedValues) {
                sb.append(value).append(",");
            }
            VISITED_PRODUCTS.set(sb.toString());
            context.write(inKey, VISITED_PRODUCTS);
        }
    }

    private static AccessData getAccessData(String line) {
        // An AccessData object will be created for each line if possible
        AccessData accessData = null;
        try {
            accessData = new AccessData();
            // Parse the value separated line using space as the delimiter
            CSVParser csvParser = new CSVParser(new StringReader(line));
            csvParser.getStrategy().setDelimiter(' ');

            // Now get all the values from the line
            String[] values = csvParser.getLine();

            // Get the IP
            accessData.ip = values[0];

            // The time is split into 2 values so they have to be combined
            // then sent to match the time regular expression
            // "[02/Aug/2011:00:00:04" + " -0700]" = "[02/Aug/2011:00:00:04 -0700]"
            accessData.timestamp = new Timestamp(DATA_FORMAT.parse(values[3] + " " + values[4]).getTime());

            // The resource filed has 3 fields (HTTP Method, Page and HTTP protocol)
            // so it has to be further split by spaces
            String reqInfo = values[5];
            String[] reqInfoArr = reqInfo.split(" ");

            // Get the HTTP method
            accessData.method = reqInfoArr[0];

            // Get the page requested
            accessData.resource = reqInfoArr[1];

            // Get the HTTP response code
            accessData.httpCode = Integer.parseInt(values[6]);

            // Try to get the response data size in bytes, if a hyphen shows up,
            // that means the client has a cache of this page and no data is
            // sent back
            try {
                accessData.dataLength = Long.parseLong(values[7]);
            } catch (NumberFormatException nfe) {
                accessData.dataLength = 0;
            }

            return accessData;
        } catch (IOException ioe) {
            LOG.info(ioe);
            return null;
        } catch (ParseException pe) {
            LOG.info(pe);
            return null;
        }
    }


    public Job getMapReduceJob() throws Exception {
        // Get a configuration from the Hadoop jars in the classpath at the server side
        ConfigurationManager cm = new ConfigurationManager("I827779", "hadoopsap");
        Configuration configuration = cm.getConfiguration();

        // The output folder MUST NOT be created, Hadoop will do it automatically
        IFileSystem filesystem = cm.getFileSystem();

        if (inputPath == null) {
            inputPath = cm.getRemoteFolder() + "accessLogs/";
        }
        outputPath = inputPath + "parsed/";

        // Delete the output directory if it already exists
        if (filesystem.exists(outputPath)) {
            filesystem.deleteDirectory(outputPath);
        }
        Job job = new Job(configuration, "AccessLogParser");
        // This is a must step to tell Hadoop to load the jar file containing this class
        job.setJarByClass(LogParser.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(LogParser.AccessParserMapper.class);
        job.setReducerClass(LogParser.AccessParserReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    public static class AccessData {
        private String ip;
        private Timestamp timestamp;
        private String method;
        private String resource;
        private int httpCode;
        private long dataLength;

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("IP : ").append(ip).append("\n");
            sb.append("Timestamp : ").append(timestamp).append("\n");
            sb.append("Method : ").append(method).append("\n");
            sb.append("Resource : ").append(resource).append("\n");
            sb.append("HttpCode : ").append(httpCode).append("\n");
            sb.append("Length : ").append(dataLength).append("\n");
            return sb.toString();
        }
    }

    public String getOutputPath() {
        return outputPath;
    }

    public String getInputPath() {
        return inputPath;
    }

    public static void main(String[] arg) throws Exception {
        LogParser lp;
        if (arg.length > 0) {
            lp = new LogParser(arg[0]);
        } else {
            lp = new LogParser(null);
        }
        lp.getMapReduceJob().waitForCompletion(true);
    }
}
