package com.sap.demo.pos;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 4/12/12
 * Time: 3:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccessLogReplacer {
    public static void main(String[] arg) throws Exception {
        File flagFolder = new File("/hadoop/user/hadoop/posDemo/anonymize/");
        Collection<File> files = new LinkedHashSet<File>(190000);
        String folder = "/hadoop/user/hadoop/posDemo/accessLogs/";
        files = getAccessLogFiles(files, folder);
        System.out.println("Found " + files.size() + " access log files in folder : " + folder);
        for (File file : files) {
            File flagFile = new File(flagFolder, file.getName());
            System.out.println("flagFile : " + flagFile.getAbsolutePath() + "...exist? " + flagFile.exists());
            if (!flagFile.exists()) {
                flagFile.createNewFile();
                List<String> lines = FileUtils.readLines(file);
                File tempFile = new File(folder + file.getName() + ".temp");
                boolean renameDone = file.renameTo(tempFile);
                System.out.println("Renaming... " + renameDone + " -> " + file.getAbsolutePath() + " to " + tempFile.getAbsolutePath());
                List<String> newLines = getReplacedLines(lines);
                FileUtils.writeLines(file, newLines);
                System.out.println("Done writing new lines to " + file.getAbsolutePath());
                tempFile.delete();
                System.out.println("Done deleting temp file: " + tempFile.getAbsolutePath());
            }

        }
    }

    private static List<String> getReplacedLines(List<String> oldLines) {
        List<String> newLines = new ArrayList<String>();
        for (String oldLine : oldLines) {
            newLines.add(replaceLine(oldLine));
        }
        return newLines;
    }

    private static String replaceLine(String oldLine) {
        return oldLine.replaceAll("amazon", "wholeSales");
    }

    private static Collection<File> getAccessLogFiles(Collection<File> files, String folder) throws Exception {
        if (files == null) {
            files = new HashSet<File>();
        }
        File folderFile = new File(folder);
        for (File file : folderFile.listFiles()) {
            if (file.isDirectory() && !file.getName().contains("sessionItems")) {
                System.out.println("About to add files in folder : " + file.getAbsolutePath());
                getAccessLogFiles(files, file.getAbsolutePath());
            } else if (file.isFile()) {
                if (file.getCanonicalPath().contains("hr-") && file.getAbsolutePath().endsWith(".log")) {
                    files.add(file);
                }
            }
        }
        return files;
    }
}
