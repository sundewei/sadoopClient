package com.sap.mapred;

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
        ProductTrend pt = new ProductTrend(lp.getOutputPath());

        boolean doneWithoutError = lp.getMapReduceJob().waitForCompletion(true);
System.out.println("lp.getInputPath()="+lp.getInputPath());
System.out.println("lp.getOutputPath()="+lp.getOutputPath());
        if (doneWithoutError) {
            pt.getMapReduceJob().waitForCompletion(true);
System.out.println("pt.getInputPath()="+pt.getInputPath());
System.out.println("pt.getOutputPath()="+pt.getOutputPath());
        }
    }
}
