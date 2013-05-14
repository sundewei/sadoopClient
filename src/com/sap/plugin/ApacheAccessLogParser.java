package com.sap.plugin;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.conf.IFileSystem;
import com.sap.hadoop.task.ITask;
import com.sap.mapred.LogParser;
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 9/2/11
 * Time: 10:53 AM
 * To change this template use File | Settings | File Templates.
 */
public class ApacheAccessLogParser implements ITask {
    private static final Logger LOG = Logger.getLogger(LogParser.class.getName());
    private static final DateFormat DATA_FORMAT = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss Z]");

    private static String OUTPUT_PATH;
    private static String INPUT_PATH;

    private static String MAP_KEY_COLUMN;
    private static List<String> MAP_VALUE_COLUMNS = new ArrayList<String>();
    private static String REDUCE_KEY_COLUMN;
    private static List<String> REDUCE_VALUE_COLUMNS = new ArrayList<String>();

    private static String REDUCE_VALUE_DELIMITER = "_";
    private static String REDUCE_VALUE_SORT_ORDER = "asc";

    private static Properties PROPERTIES;

    public ApacheAccessLogParser(String propFile) throws IOException {
        PROPERTIES = new Properties();
        PROPERTIES.load(new FileInputStream(propFile));

        OUTPUT_PATH = PROPERTIES.getProperty("outputPath");
        INPUT_PATH = PROPERTIES.getProperty("inputPath");

        if (PROPERTIES.getProperty("mapValueColumns") != null) {
            MAP_VALUE_COLUMNS.addAll(Arrays.asList(PROPERTIES.getProperty("mapValueColumns").split(",")));
        } else {
            MAP_VALUE_COLUMNS.add("timestamp");
            MAP_VALUE_COLUMNS.add("resource");
        }
        if (PROPERTIES.getProperty("mapKeyColumn") != null) {
            MAP_KEY_COLUMN = PROPERTIES.getProperty("mapKeyColumn");
        } else {
            MAP_KEY_COLUMN = "ip";
        }

        if (PROPERTIES.getProperty("reduceValueColumns") != null) {
            REDUCE_VALUE_COLUMNS.addAll(Arrays.asList(PROPERTIES.getProperty("reduceValueColumns").split(",")));
        } else {
            REDUCE_VALUE_COLUMNS.add("resource");
        }

        if (PROPERTIES.getProperty("reduceKeyColumn") != null) {
            REDUCE_KEY_COLUMN = PROPERTIES.getProperty("reduceKeyColumn");
        } else {
            REDUCE_KEY_COLUMN = "ip";
        }

        REDUCE_VALUE_DELIMITER =
                PROPERTIES.getProperty("reduceValueDelimiter") != null ? PROPERTIES.getProperty("reduceValueDelimiter") : "_";
        REDUCE_VALUE_SORT_ORDER =
                PROPERTIES.getProperty("reduceValueSortOrder") != null ? PROPERTIES.getProperty("reduceValueSortOrder") : "desc";
    }

    /**
     * The map class
     */
    public static class AccessParserMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String mapKeyColumn;
        private List<String> mapValueColumns;
        private final Text MAP_OUT_VALUE = new Text();
        private final Text MAP_OUT_KEY = new Text();

        protected void init(Mapper.Context context) {
            if (this.mapKeyColumn == null) {
                this.mapKeyColumn = context.getConfiguration().get("MAP_KEY_COLUMN");
                this.mapValueColumns = getStringList(context.getConfiguration(), "MAP_VALUE_COLUMNS");
            }
        }

        public void map(LongWritable inKey, Text inValue, Context context)
                throws IOException, InterruptedException {
            init(context);
            String line = inValue.toString();
            AccessData accessData = getAccessData(line);
            if (accessData != null) {
                MAP_OUT_KEY.set(accessData.getAttribute(mapKeyColumn));
                StringBuilder sb = new StringBuilder();
                for (String mapValCol : mapValueColumns) {
                    sb.append(accessData.getAttribute(mapValCol)).append("_");
                }
                MAP_OUT_VALUE.set(sb.deleteCharAt(sb.length() - 1).toString());
                context.write(MAP_OUT_KEY, MAP_OUT_VALUE);
            }
        }
    }

    /**
     * The reduce class
     */
    public static class AccessParserReducer extends Reducer<Text, Text, Text, Text> {
        private String reduceValueSortOrder;
        private List<String> reduceValueColumns;
        private List<String> mapValueColumns;
        private String mapKeyColumn;
        private String reduceKeyColumn;

        private final Text VISITED_PRODUCTS = new Text();
        private Text REDUCE_KEY = new Text();

        protected void init(Context context) {
            if (this.mapKeyColumn == null) {
                this.mapKeyColumn = context.getConfiguration().get("MAP_KEY_COLUMN");
                this.mapValueColumns = getStringList(context.getConfiguration(), "MAP_VALUE_COLUMNS");
                this.reduceKeyColumn = context.getConfiguration().get("REDUCE_KEY_COLUMN");
                this.reduceValueColumns = getStringList(context.getConfiguration(), "REDUCE_VALUE_COLUMNS");
                this.reduceValueSortOrder = context.getConfiguration().get("REDUCE_VALUE_SORT_ORDER");
            }
        }

        public void reduce(Text inKey, Iterable<Text> inValues, Context context)
                throws IOException, InterruptedException {
            init(context);
            Set<String> sortedValues;
            if ("asc".equalsIgnoreCase(reduceValueSortOrder)) {
                sortedValues = new TreeSet<String>();
            } else {
                sortedValues = new TreeSet<String>(Collections.reverseOrder());
            }
            new TreeSet<String>();
            List<Integer> valueIndexes = new ArrayList<Integer>();
            for (String reduceValueColumn : reduceValueColumns) {
                valueIndexes.add(getIndex(reduceValueColumn, mapValueColumns));
            }
            boolean doneReduceKey = false;
            while (inValues.iterator().hasNext()) {
                String[] valueArray = inValues.iterator().next().toString().split("_");
                if (!doneReduceKey) {
                    if (mapKeyColumn.equalsIgnoreCase(reduceKeyColumn)) {
                        REDUCE_KEY = inKey;
                    } else {
                        REDUCE_KEY.set(getReduceValue(valueArray, Arrays.asList(getIndex(mapKeyColumn, mapValueColumns))));
                    }
                    doneReduceKey = true;
                }
                String reduceValue = getReduceValue(valueArray, valueIndexes);
                sortedValues.add(reduceValue);
            }

            StringBuilder sb = new StringBuilder();

            for (String sortedValue : sortedValues) {
                sb.append(sortedValue).append(",");
            }

            VISITED_PRODUCTS.set(sb.deleteCharAt(sb.length() - 1).toString());
            context.write(REDUCE_KEY, VISITED_PRODUCTS);
        }
    }

    private static List<String> getStringList(Configuration conf, String prefix) {
        List<String> list = new ArrayList<String>();
        String string = conf.get(prefix + "_0");
        int i = 1;
        while (string != null) {
            list.add(string);
            string = conf.get(prefix + "_" + i);
            i++;
        }
        return list;
    }

    private static void setStringList(Configuration conf, List<String> list, String prefix) {
        for (int i = 0; i < list.size(); i++) {
            conf.set(prefix + "_" + i, list.get(i));
        }
    }

    private static String getReduceValue(String[] values, List<Integer> indexes) {
        StringBuilder sb = new StringBuilder();
        for (int index : indexes) {
            sb.append(values[index]).append(REDUCE_VALUE_DELIMITER);
        }
        return sb.deleteCharAt(sb.length() - 1).toString();
    }

    private static int getIndex(String column, List<String> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (column.equalsIgnoreCase(columns.get(i))) {
                return i;
            }
        }
        return -1;
    }

    public Job getMapReduceJob() throws Exception {
        // Get a configuration from the Hadoop jars in the classpath at the server side
        ConfigurationManager cm = new ConfigurationManager("I827779", "hadoopsap");
        Configuration configuration = cm.getConfiguration();
        System.out.println("GGG 1");
        configuration.set("MAP_KEY_COLUMN", MAP_KEY_COLUMN);
        setStringList(configuration, MAP_VALUE_COLUMNS, "MAP_VALUE_COLUMNS");
        System.out.println("GGG 2");
        configuration.set("REDUCE_KEY_COLUMN", REDUCE_KEY_COLUMN);
        setStringList(configuration, REDUCE_VALUE_COLUMNS, "REDUCE_VALUE_COLUMNS");
        System.out.println("GGG 3");
        configuration.set("REDUCE_VALUE_DELIMITER", REDUCE_VALUE_DELIMITER);
        configuration.set("REDUCE_VALUE_SORT_ORDER", REDUCE_VALUE_SORT_ORDER);
        System.out.println("GGG 4");
        // The output folder MUST NOT be created, Hadoop will do it automatically
        IFileSystem filesystem = cm.getFileSystem();

        if (!INPUT_PATH.endsWith(File.separator)) {
            INPUT_PATH = INPUT_PATH + File.separator;
        }
        System.out.println("GGG 5");
        if (!OUTPUT_PATH.endsWith(File.separator)) {
            OUTPUT_PATH = OUTPUT_PATH + File.separator;
        }
        System.out.println("GGG 6");
        // Delete the output directory if it already exists
        if (filesystem.exists(OUTPUT_PATH)) {
            filesystem.deleteDirectory(OUTPUT_PATH);
        }
        System.out.println("GGG 7");
        Job job = new Job(configuration, "ApacheAccessLogParser");
        // This is a must step to tell Hadoop to load the jar file containing this class
        job.setJarByClass(ApacheAccessLogParser.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ApacheAccessLogParser.AccessParserMapper.class);
        job.setReducerClass(ApacheAccessLogParser.AccessParserReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        return job;
    }

    public static void main(String[] arg) throws Exception {
        ApacheAccessLogParser aalp;
        if (arg != null && arg.length > 0) {
            aalp = new ApacheAccessLogParser(arg[0]);
        } else {
            throw new Exception("The first parameter should be the properties file!");
        }
        aalp.getMapReduceJob().waitForCompletion(true);
    }

    public static class AccessData {
        private String ip;
        private Timestamp timestamp;
        private String method;
        private String resource;
        private int httpCode;
        private long dataLength;

        public String getAttribute(String name) {
            if ("ip".equalsIgnoreCase(name)) {
                return ip;
            } else if ("timestamp".equalsIgnoreCase(name)) {
                return String.valueOf(timestamp.getTime());
            } else if ("method".equalsIgnoreCase(name)) {
                return method;
            } else if ("resource".equalsIgnoreCase(name)) {
                return resource;
            } else if ("httpCode".equalsIgnoreCase(name)) {
                return String.valueOf(httpCode);
            } else if ("dataLength".equalsIgnoreCase(name)) {
                return String.valueOf(dataLength);
            }
            return null;
        }

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
}
