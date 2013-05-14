package com.sap.mapred.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/21/13
 * Time: 2:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class RecursiveFilenameInputFormat extends FileInputFormat<NullWritable, Text> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<NullWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new RecursiveFilenameRecordReader();
    }
}
