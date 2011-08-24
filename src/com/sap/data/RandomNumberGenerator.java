package com.sap.data;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 5/5/11
 * Time: 4:35 PM
 * To change this template use File | Settings | File Templates.
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Jan 20, 2011
 * Time: 4:22:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class RandomNumberGenerator {
    private static String BASE_FILENAME = "c:\\temp\\numbersToSort";

    public static void main(String[] arg) throws Exception {
        int oneMillion = 1000000;
        int numberPerFile = 300 * oneMillion;
        int numberOfFiles = 1;
        int writeFreq = 1000;

        int fileCount = 0;
        int numberCount = 0;
        List<Long> numberInFile = new ArrayList<Long>(numberPerFile);
        String filename = BASE_FILENAME + "_" + getPaddedString(fileCount, 6, '0');

        while (true) {
            long number = (long) (Math.random() * Integer.MAX_VALUE * 100);
            numberInFile.add(number);
            numberCount++;
/*
if (numberInFile.size() > 0 && numberInFile.size() % oneMillion == 0){
System.out.println("numberInFile.size()="+numberInFile.size());
}
*/
            if (numberCount % writeFreq == 0) {
                appendData(filename + ".txt", getLine(numberInFile, ',', false));
                appendData(filename + ".txt", "\n");
                numberInFile = new ArrayList<Long>();
            }

            if (numberCount >= numberPerFile) {
                fileCount++;
                if (fileCount > numberOfFiles - 1) {
                    break;
                }
                appendData(filename + ".txt", getLine(numberInFile, ',', false));
                filename = BASE_FILENAME + "_" + getPaddedString(fileCount, 6, '0');
                numberInFile = new ArrayList<Long>();
                numberCount = 0;
            }

        }
        System.exit(0);
    }

    private static String getPaddedString(int num, int width, char paddedChar) {
        String strNumber = String.valueOf(num);
        StringBuilder sb = new StringBuilder();
        int paddedLength = width - strNumber.length();
        if (paddedLength > 0) {
            for (int i = 0; i < paddedLength; i++) {
                sb.append(paddedChar);
            }
        }
        return sb.append(strNumber).toString();
    }

    private static String getLine(Collection<Long> numbers, char delimiter, boolean addDelimiter) {
        StringBuilder sb = new StringBuilder();
        for (long number : numbers) {
            sb.append(String.valueOf(number)).append(delimiter);
        }
        if (!addDelimiter && sb.length() > 0 && sb.charAt(sb.length() - 1) == delimiter) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    private static void appendData(String filename, String data) throws IOException {
        File file = new File(filename);
        BufferedWriter fileWriter = new BufferedWriter(new FileWriter(file, true));
        fileWriter.write(data);
        fileWriter.close();
    }

}