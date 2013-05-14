package com.sap.demo.pos;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/22/12
 * Time: 4:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileOrganizer {
    public static void main(String[] arg) throws Exception {
        // G:\posDemo\accessLogs\
        String baseFilename = arg[0];
        if (!baseFilename.endsWith("\\")) {
            baseFilename += "\\";
        }
        File baseFile = new File(baseFilename);
        int count = 0;
        for (File file : baseFile.listFiles()) {
            if (file.isFile() && file.getName().startsWith("access_")) {
                // access_2008-06-21_hr-15-10.log
                String destFoldername = getDestFolderName(baseFilename, file);
                File destFolderFile = new File(destFoldername);
                if (!destFolderFile.exists()) {
                    destFolderFile.mkdir();
                }
                count++;
                System.out.println(count + " About to move " + file.getName() + " to " + destFoldername);
                FileUtils.moveFile(file, new File(destFoldername + file.getName()));

                if (count % 10 == 0) {
                    try {
                        Thread.sleep(5000);
                    } catch (Exception e) {
                    }
                }
            }
        }
    }

    private static String getDestFolderName(String baseFilename, File file) {
        String simpleFilename = file.getName();
        String destFoldername = simpleFilename.substring(7, 16);
        return baseFilename + destFoldername + "\\";
    }
}
