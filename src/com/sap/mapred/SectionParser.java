package com.sap.mapred;

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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 5/18/11
 * Time: 10:09 AM
 * To change this template use File | Settings | File Templates.
 */
public class SectionParser {

    private static String CMD_FILE = null;

    private static final Logger LOG = Logger.getLogger(SectionParser.class.getName());

    // <a href(.*?)</a>
    // <link type="external" href="

    private static Pattern PATTERN = Pattern.compile("<link type=\"external\" href=\"([^\"]*)\"");

    /**
     * The map class
     */
    public static class SectionExternalLinkMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Text HOST = new Text();
        private static final IntWritable ARTICLE_WPID = new IntWritable();

        public void map(LongWritable inKey, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\t");
            int articleWpid = -1;
            try {
                if (fields.length == 6) {
                    articleWpid = Integer.parseInt(fields[3]);
                    String sectionText = fields[5];
                    String[] hosts = getExternalLinkHosts(sectionText);
                    for (String host : hosts) {
                        if (host.length() > 0 && host.indexOf(".") >= 0) {
                            HOST.set(host);
                            ARTICLE_WPID.set(articleWpid);
                            context.write(HOST, ARTICLE_WPID);
                        }
                    }
                }
            } catch (NumberFormatException nfe) {
                LOG.info(nfe);
            }
        }
    }

    /**
     * The reduce class
     */
    public static class SectionExternalLinkReducer
            extends Reducer<Text, IntWritable, Text, Text> {

        private static final Text ARTICLE_WPIDS = new Text();

        public void reduce(Text inKey, Iterable<IntWritable> inValues, Context context)
                throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            while (inValues.iterator().hasNext()) {
                sb.append(inValues.iterator().next().get()).append(",");
            }
            ARTICLE_WPIDS.set(sb.toString());
            context.write(inKey, ARTICLE_WPIDS);
        }
    }

    public Job getMapReduceJob() throws Exception {
        // Get a configuration from the Hadoop jars in the classpath at the server side
        Configuration configuration = new Configuration();

        String hdfsPersonalFolder = "/user/I827779/";
        String outputFolder = hdfsPersonalFolder + "output/";

        // The output folder MUST NOT be created, Hadoop will do it automatically
        Path outputPath = new Path(outputFolder);
        FileSystem filesystem = outputPath.getFileSystem(configuration);
        // Delete the output directory if it already exists
        if (filesystem.exists(outputPath)) {
            filesystem.delete(outputPath, true);
        }

        Job job = new Job(configuration, "section");
        // This is a must step to tell Hadoop to load the jar file containing this class
        job.setJarByClass(SectionParser.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(SectionParser.SectionExternalLinkMapper.class);
        job.setReducerClass(SectionParser.SectionExternalLinkReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(CMD_FILE));
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    private static String[] getExternalLinkHosts(String content) throws MalformedURLException {
        Matcher matcher = PATTERN.matcher(content);
        Set<String> urls = new HashSet<String>();
        while (matcher.find()) {
            String matchedText = matcher.group();
            URL url = null;
            try {
                url = new URL(matchedText.substring(28, matchedText.length() - 2));
            } catch (MalformedURLException me) {
                LOG.info(me.getMessage());
            }
            if (url != null) {
                urls.add(url.getHost());
            }
        }
        return urls.toArray(new String[urls.size()]);
    }

    public static void main(String[] args) throws Exception {
        SectionParser sp = new SectionParser();
        if (args != null && args.length >= 1) {
            CMD_FILE = args[0];
        }
        sp.getMapReduceJob().waitForCompletion(true);
    }
}
