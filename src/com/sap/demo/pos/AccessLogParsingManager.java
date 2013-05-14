package com.sap.demo.pos;

import com.sap.demo.AffinityLogParserStep1;
import com.sap.demo.ItemSessionCounter;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Calendar;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/26/12
 * Time: 10:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class AccessLogParsingManager {
    public static void main(String[] arg) throws Exception {
        // /hadoop/user/hadoop/posDemo/accessLogs
        String folder = getFolder(arg);
        File flagFileFolder = getFlagFolderFile(folder);
        File folderFile = new File(folder);
        String resultFolder = "/hadoop/user/hadoop/posDemo/item_session_result/";
        File resultFolderFile = new File(resultFolder);
        if (!resultFolderFile.exists()) {
            resultFolderFile.mkdir();
        }
        Calendar todayCalendar = Calendar.getInstance();
        String todayDateString = com.sap.demo.Utility.SIMPLE_DATE_FORMAT.format(todayCalendar.getTime());

        System.out.println("####   Iterate through all folders in " + folder);
        System.out.println("####   folderFile: " + folderFile);
        System.out.println("####   folder files: " + folderFile.listFiles());
        int count = 0;
        for (File subFolder : folderFile.listFiles()) {
            System.out.println("####   " + count + "th folder...will stop when submitting 10 folders");
            System.out.println("####   Found folder : " + subFolder.getAbsolutePath());
            File runFlagFile = new File(flagFileFolder.getAbsolutePath() + File.separator + subFolder.getName() + ".mapreduceRun");
            if (subFolder.isDirectory() &&
                    !subFolder.getAbsolutePath().equals(flagFileFolder.getAbsolutePath()) &&
                    !runFlagFile.exists() /*&& count < 10*/) {
                System.out.println("####   Creating flag file: " + runFlagFile.getAbsolutePath());
                if (!todayDateString.startsWith(subFolder.getName())) {
                    runFlagFile.createNewFile();
                }
                count++;
                String targetFolder = subFolder.getAbsolutePath();
                if (!targetFolder.endsWith(File.separator)) {
                    targetFolder += File.separator;
                }

                targetFolder = getHdfsPath(targetFolder);

                System.out.println("####   About to submit MR job for " + targetFolder);
                AffinityLogParserStep1 step1 = new AffinityLogParserStep1(targetFolder);
                System.out.println("####   step1: " + step1);
                Job step1Job = step1.getMapReduceJob();
                System.out.println("####   step1Job: " + step1Job);
                if (step1Job.waitForCompletion(true)) {
                    String counterInputPath = subFolder.getAbsolutePath() + File.separator + "sessionItems/";
                    String hdfsCounterInputPath = getHdfsPath(counterInputPath);
                    ItemSessionCounter itemSessionCounter = new ItemSessionCounter(hdfsCounterInputPath);
                    if (itemSessionCounter.getMapReduceJob().waitForCompletion(true)) {
                        String resultInputPath = counterInputPath + "counts/part-r-00000";
                        String destFilename = resultFolder + subFolder.getName() + ".csv";
                        System.out.println("####   About to move " + resultInputPath + " to " + destFilename);
                        copyFile(new File(resultInputPath), new File(destFilename));
                        File doneFlagFile = new File(flagFileFolder.getAbsolutePath() + File.separator + subFolder.getName() + ".mapreduceDone");
                        doneFlagFile.createNewFile();
                        System.out.println("####   Done with " + subFolder.getName());
                    }
                }
            }
        }
    }

    private static String getHdfsPath(String path) {
        return path.replaceFirst("/hadoop", "");
    }

    private static String getFolder(String[] arg) {
        String folder = null;
        if (arg != null && arg.length > 0) {
            folder = arg[0];
            if (!folder.endsWith(File.separator)) {
                folder += File.separator;
            }
        } else {
            folder = "/hadoop/user/hadoop/posDemo/accessLogs/";
        }
        return folder;
    }

    private static File getFlagFolderFile(String folder) {
        File flagFileFolder = new File(folder + "parsedFlags" + File.separator);
        if (!flagFileFolder.exists()) {
            flagFileFolder.mkdir();
        }
        return new File(flagFileFolder + File.separator);
    }

    public static void copyFile(File in, File out) throws IOException {
        FileChannel inChannel = new FileInputStream(in).getChannel();
        FileChannel outChannel = new FileOutputStream(out).getChannel();
        try {
            inChannel.transferTo(0, inChannel.size(), outChannel);
        } finally {
            if (inChannel != null) inChannel.close();
            if (outChannel != null) outChannel.close();
        }
    }

}
