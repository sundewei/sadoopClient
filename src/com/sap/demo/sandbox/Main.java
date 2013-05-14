package com.sap.demo.sandbox;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/29/12
 * Time: 11:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class Main {
    public static void main(String[] arg) throws Exception {
        Random random = new Random();
        String filename = "C:\\temp\\recommendation.txt";
        List<String> lines = FileUtils.readLines(new File(filename));
        Set<String> users = new HashSet<String>();
        for (String line : lines) {
            String[] values = line.split("\t");
            users.add(values[0]);
        }
        System.out.println("var users = new Array();");
        int index = 0;
        int count = 0;
        for (String user : users) {
            if (user.length() > 5 && user.length() <= 15 && !user.contains("\"") && !user.contains("$") && user.contains(" ") && !user.contains("\\") && !user.contains(",") && !user.contains(".")) {
                if (random.nextDouble() > 0.4d && count < 200) {
                    System.out.println("users[" + index + "]=\"" + user + "\";");
                    index++;
                }
                count++;
            }
        }
    }
}
