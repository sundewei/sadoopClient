package com.sap.etl.example;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.etl.ContextFactory;
import com.sap.hadoop.etl.IContext;
import com.sap.hadoop.etl.SQLStep;
import com.sap.hadoop.etl.UploadStep;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 7/5/11
 * Time: 3:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class Tester {
    public static void main(String[] arg) throws Exception {
        JarFile jarFile = new JarFile(arg[0]);
        Enumeration<JarEntry> entries = jarFile.entries();
        List<String> resourceEntries = new ArrayList<String>();

        while (entries.hasMoreElements()) {
            resourceEntries.add(entries.nextElement().toString());

        }
        if (resourceEntries.size() == 0) {
            throw new IOException("No jar entry found in the uploaded jar file: " + arg[0]);
        }

        for (String c: resourceEntries){
            System.out.println("cccc=="+c);
        }
    }
}
