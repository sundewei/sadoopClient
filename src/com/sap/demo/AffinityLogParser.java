package com.sap.demo;


import org.apache.hadoop.mapreduce.Job;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/6/11
 * Time: 10:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class AffinityLogParser {
    public static void main(String[] arg) throws Exception {
        AffinityLogParserStep1 step1 = new AffinityLogParserStep1(arg[0]);
        Job step1Job = step1.getMapReduceJob();

        AffinityLogParserStep2 step2 = new AffinityLogParserStep2(step1.getOutputPath());
        Job step2Job = step2.getMapReduceJob();

        if (step1Job.waitForCompletion(true)) {
            step2Job.waitForCompletion(false);
        }
    }
}
