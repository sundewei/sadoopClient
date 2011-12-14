package com.sap.mapred;

import org.apache.hadoop.mapreduce.Job;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 8/23/11
 * Time: 10:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class LogProcessingExample {
    public static void main(String[] arg) throws Exception {
        LogParser lp;
        if (arg.length > 0) {
            lp = new LogParser(arg[0]);
        } else {
            lp = new LogParser(null);
        }
        Job job1 = lp.getMapReduceJob();

        ProductTrend pt = new ProductTrend(lp.getOutputPath());
        Job job2 = pt.getMapReduceJob();
        if (job1.waitForCompletion(true)) {
            job2.waitForCompletion(false);
        }
    }
}
