package com.sap.mains;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.conf.IFileSystem;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/10/12
 * Time: 11:30 AM
 * To change this template use File | Settings | File Templates.
 */
public class HdfsCopyToLocal {
    public static void main(String[] args) throws Exception {
        ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "abcd1234");
        IFileSystem iFileSystem = configurationManager.getFileSystem();
        long bytes = iFileSystem.getSize(args[0]);
        long start = System.currentTimeMillis();
        IOUtils.copyLarge(iFileSystem.getInputStream(args[0]), FileUtils.openOutputStream(new File(args[1])));
        long end = System.currentTimeMillis();
        double lenMb = (double) bytes / (double) (1024 * 1000);
        System.out.println("bytes: " + bytes);
        System.out.println("lenMb: " + lenMb);
        double seconds = ((double) end - (double) start) / (double) 1000;
        System.out.println("seconds: " + seconds);
        System.out.println("Throughput: " + (lenMb / seconds) + " MB/s");
    }
}
