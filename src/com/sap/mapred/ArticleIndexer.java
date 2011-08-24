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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 5/20/11
 * Time: 11:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class ArticleIndexer {
    private static String CMD_FILE = null;

    private static final Logger LOG = Logger.getLogger(ArticleIndexer.class.getName());

    private static final Analyzer TEXT_ANALYZER = new TextAnalyzer();

    /**
     * The map class
     */
    public static class IndexMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final Text KEYWORD = new Text();
        private static final IntWritable WPID = new IntWritable();

        public void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException {
            String inText = inValue.toString();
            String[] fields = inText.split("\t");
            int wpid = -1;
            try {
                wpid = Integer.parseInt(fields[0]);
            } catch (NumberFormatException nfe) {
                LOG.info(nfe);
            }
            if (wpid > 0 && fields.length == 5) {
                WPID.set(wpid);
                String articleText = fields[4];
                Set<String> keywords = getKeywords(articleText);
                for (String keyword : keywords) {
                    KEYWORD.set(keyword);
                    context.write(KEYWORD, WPID);
                }
            }
        }
    }

    /**
     * The reduce class
     */
    public static class IndexReducer
            extends Reducer<Text, IntWritable, Text, Text> {

        private static final Text OUT_VALUE = new Text();

        public void reduce(Text inKey, Iterable<IntWritable> inValues, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> it = inValues.iterator();
            StringBuilder sb = new StringBuilder();
            int count = 0;
            while (it.hasNext()) {
                int wpid = it.next().get();
                sb.append(wpid).append(",");
                count++;
            }
            sb.deleteCharAt(sb.length() - 1).insert(0, count + " : ");
            OUT_VALUE.set(sb.toString());
            context.write(inKey, OUT_VALUE);
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

        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(hdfsPersonalFolder + CMD_FILE));
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    private static Set<String> getKeywords(String text) throws IOException {
        Set<String> keywords = new HashSet<String>();
        TokenStream tokenStream = TEXT_ANALYZER.tokenStream(null, new StringReader(text));
        TermAttribute termAttribute = tokenStream.getAttribute(TermAttribute.class);
        while (tokenStream.incrementToken()) {
            String term = termAttribute.term();
            keywords.add(term);
        }
        return keywords;
    }

    public static void main(String[] args) throws Exception {
        ArticleIndexer sp = new ArticleIndexer();
        if (args != null && args.length >= 1) {
            CMD_FILE = args[0];
        }
        sp.getMapReduceJob().waitForCompletion(true);
    }
}
