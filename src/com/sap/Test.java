package com.sap;


import com.sap.demo.Utility;
import com.sap.demo.pos.DatabaseUtility;
import com.sap.demo.pos.beans.AmazonProduct;
import com.sap.demo.pos.beans.Category;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/28/12
 * Time: 3:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class Test {
    private static final Calendar START_CALENDAR = Calendar.getInstance();
    private static final Calendar END_CALENDAR = Calendar.getInstance();

    static {
        START_CALENDAR.set(Calendar.YEAR, 2008);
        START_CALENDAR.set(Calendar.MONTH, Calendar.JANUARY);
        START_CALENDAR.set(Calendar.DAY_OF_MONTH, 1);
        START_CALENDAR.set(Calendar.HOUR_OF_DAY, 0);
        START_CALENDAR.set(Calendar.MINUTE, 0);
        START_CALENDAR.set(Calendar.SECOND, 0);
        START_CALENDAR.set(Calendar.MILLISECOND, 0);

        END_CALENDAR.set(Calendar.YEAR, 2012);
        END_CALENDAR.set(Calendar.MONTH, Calendar.MAY);
        END_CALENDAR.set(Calendar.DAY_OF_MONTH, 16);
        END_CALENDAR.set(Calendar.HOUR_OF_DAY, 23);
        END_CALENDAR.set(Calendar.MINUTE, 59);
        END_CALENDAR.set(Calendar.SECOND, 59);
        END_CALENDAR.set(Calendar.MILLISECOND, 999);
    }

    public static Map<String, Integer> getDateStringId() throws Exception {
        Map<String, Integer> map = new HashMap<String, Integer>();
        Connection conn = DatabaseUtility.getConnection();
        String sql = " SELECT DATE_STRING, ID FROM HADOOP.DATE_DIM ";
        PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            map.put(rs.getString(1), rs.getInt(2));
        }
        rs.close();
        stmt.close();
        conn.close();
        return map;
    }

    public static Map<String, String> getOldNewNames() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        /*
        Connection conn = DatabaseUtility.getCprConnection();
        String sql = " select * from hadoop.companies ";
        PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            map.put(rs.getString(2), rs.getString(3));
        }
        rs.close();
        stmt.close();
        conn.close();
        */
        map.put("TOSHIBA", "Tototto");
        return map;
    }

    public static void datifyToMonth(String[] arg) throws Exception {
        Map<String, Integer> distCountMap = new HashMap<String, Integer>();
        File folder = new File("C:\\projects\\dataCollector\\amazonData\\aggSessionCount\\");
        File aggStage1Folder = new File("C:\\projects\\dataCollector\\amazonData\\aggSessionCount\\aggStage1\\");
        for (File file : folder.listFiles()) {
            if (file.isFile()) {
                System.out.println("Working on " + file.getAbsolutePath());
                List<String> lines = IOUtils.readLines(new FileInputStream(file));
                for (String line : lines) {
                    String[] values = line.split(",");
                    StringBuilder keySb = new StringBuilder(values[0]);
                    keySb.append(",").append(values[1]).append(",");
                    if (!distCountMap.containsKey(keySb.toString())) {
                        distCountMap.put(keySb.toString(), Integer.parseInt(values[2]));
                    } else {
                        distCountMap.put(keySb.toString(), distCountMap.get(keySb.toString()) + Integer.parseInt(values[2]));
                    }
                }
            }
        }

        Set<String> doneMonth = new HashSet<String>();
        while (START_CALENDAR.before(END_CALENDAR)) {
            String monthString = Utility.SIMPLE_MONTH_FORMAT.format(START_CALENDAR.getTime());
            if (!doneMonth.contains(monthString)) {
                File outFile = new File(aggStage1Folder + File.separator + monthString + ".csv");
                List<String> outContent = new ArrayList<String>();
                for (Map.Entry<String, Integer> entry : distCountMap.entrySet()) {
                    if (entry.getKey().startsWith(monthString)) {
                        outContent.add(entry.getKey() + entry.getValue());
                    }
                }
                IOUtils.writeLines(outContent, "\n", new FileOutputStream(outFile));
                System.out.println("Done outputting " + outFile.getAbsolutePath());
                doneMonth.add(monthString);
            }
            START_CALENDAR.add(Calendar.DATE, 1);
        }

    }

    public static void main2(String[] arg) throws Exception {
        Connection conn = DatabaseUtility.getConnection();
        String sql = " INSERT INTO HADOOP.ITEM_SESSIONS VALUES (?, ?, ?, ?) ";
        PreparedStatement stmt = conn.prepareStatement(sql);
        int batchCount = 0;
        File folder = new File("C:\\projects\\dataCollector\\amazonData\\aggSessionCount\\aggStage2\\");
        boolean hasBatch = false;
        for (File file : folder.listFiles()) {
            System.out.println("Working on " + file.getName());
            List<String> lines = IOUtils.readLines(new FileInputStream(file));
            for (String line : lines) {
                String[] values = line.split(",");
                stmt.setString(1, values[0]);
                stmt.setString(2, values[1]);
                stmt.setInt(3, Integer.parseInt(values[2]));
                stmt.setString(4, values[3]);
                stmt.addBatch();
                batchCount++;
                hasBatch = true;
                if (batchCount % 1000 == 0) {
                    System.out.println("Insert 1000 rows, batchCount = " + batchCount);
                    stmt.executeBatch();
                    hasBatch = false;
                }
            }
        }
        if (hasBatch) {
            System.out.println("Last batch, batchCount = " + batchCount);
            stmt.executeBatch();
        }
        stmt.close();
        conn.close();
    }

    public static void datify(String[] arg) throws Exception {
        Map<String, Integer> dateStringIds = getDateStringId();
        File folder = new File("C:\\projects\\dataCollector\\amazonData\\aggSessionCount\\aggStage1\\");
        File aggStage2Folder = new File("C:\\projects\\dataCollector\\amazonData\\aggSessionCount\\aggStage2\\");
        for (File file : folder.listFiles()) {
            File outFile = new File(aggStage2Folder.getAbsolutePath() + File.separator + file.getName());
            if (!outFile.exists()) {
                outFile.createNewFile();
                System.out.println("Working on " + file.getAbsolutePath());
                List<String> lines = IOUtils.readLines(new FileInputStream(file));
                List<String> newLines = new ArrayList<String>(lines.size());
                for (String line : lines) {
                    String[] values = line.split(",");
                    int dateId = dateStringIds.get(values[0]);
                    StringBuilder sb = new StringBuilder(line);
                    sb.append(",").append(dateId);
                    newLines.add(sb.toString());
                }
                IOUtils.writeLines(newLines, "\n", new FileOutputStream(outFile));
                System.out.println("Done writing to " + outFile.getAbsolutePath());
            }
        }
    }

    public static void main(String[] arg) throws Exception {
        String query = "select id, asin, num_of_reviews, Round(avg_rating, 2), top_category_id \n" +
                "from hadoop.item_dim";
        Connection conn = DatabaseUtility.getCprConnection();
        PreparedStatement stmt = conn.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();
        StringBuilder sb = new StringBuilder();
        while (rs.next()) {
            sb.append(rs.getString(1)).append(",")
                    .append(rs.getString(2)).append(",")
                    .append(rs.getString(3)).append(",")
                    .append(rs.getString(4)).append(",")
                    .append(rs.getString(5)).append("\n");
        }
        FileUtils.write(new File("C:\\projects\\SapPoc\\amazonProductMap.csv"), sb.toString());
    }

    public static void updateBottomCategoryName() throws Exception {
        Connection conn = DatabaseUtility.getConnection();
        PreparedStatement stmt = conn.prepareStatement(" update hadoop.item_dim set bottom_category_name = ? where id = ? ");
        Map<Integer, AmazonProduct> products = DatabaseUtility.getAmazonProductMap();
        for (Map.Entry<Integer, AmazonProduct> entry : products.entrySet()) {
            int itemId = entry.getKey();
            AmazonProduct product = entry.getValue();
            String bottomCategoryName = null;
            for (int level = 6; level >= 0 && bottomCategoryName == null; level--) {
                List<Category> categories = product.getCategories(level);
                if (categories != null && categories.size() > 0) {
                    bottomCategoryName = categories.get(0).getName();
                }
            }
            stmt.setString(1, bottomCategoryName);
            stmt.setInt(2, itemId);
            stmt.executeUpdate();
        }
        stmt.close();
        conn.close();
    }


    public static void updateProductTitle() throws Exception {
        Map<String, String> oldNewNames = getOldNewNames();
        Connection conn = DatabaseUtility.getConnection();
        PreparedStatement stmt = conn.prepareStatement(" update hadoop.item_dim set title = ? where id = ? ");
        Map<Integer, AmazonProduct> products = DatabaseUtility.getAmazonProductMap();
        int i = 0;
        for (Map.Entry<Integer, AmazonProduct> entry : products.entrySet()) {
            int itemId = entry.getKey();
            AmazonProduct product = entry.getValue();
            String title = product.getTitle();
            for (Map.Entry<String, String> names : oldNewNames.entrySet()) {
                if (title.contains(names.getKey())) {
                    title = title.replaceAll(names.getKey(), names.getValue());
                }
            }
            stmt.setString(1, title);
            stmt.setInt(2, itemId);
            stmt.executeUpdate();
            i++;
            System.out.println(i + "/" + products.size());
        }
        stmt.close();
        conn.close();
    }


    private static Map<String, List<String>> dictMap = new HashMap<String, List<String>>();

    public static void main1(String[] arg) throws Exception {
        File namesFile = new File("c:\\data\\NAMES.DIC");
        List<String> names = FileUtils.readLines(namesFile);

        for (String name : names) {
            name = name.toUpperCase();
            if (name.length() > 4 && !name.contains(" ") && !name.contains("'")) {
                List<String> nameList = dictMap.get(name.substring(0, 1));
                if (nameList == null) {
                    nameList = new ArrayList<String>();
                }
                nameList.add(name);
                dictMap.put(name.substring(0, 1), nameList);
            }
        }

        Connection conn = DatabaseUtility.getConnection();
        String selectQuery = " select id, name from hadoop.companies ";
        PreparedStatement stmt = conn.prepareStatement(selectQuery);
        ResultSet rs = stmt.executeQuery();
        Map<Integer, String> companies = new HashMap<Integer, String>();
        while (rs.next()) {
            int id = rs.getInt(1);
            String originalName = rs.getString(2);
            companies.put(id, originalName);
        }
        rs.close();
        stmt.close();


        for (Map.Entry<Integer, String> entry : companies.entrySet()) {
            stmt = conn.prepareStatement(" UPDATE HADOOP.companies set ANONYMIZED_NAME = ? where id = ? ");
            String name = getName(entry.getValue());
            name = name.substring(0, 1) + name.substring(1, name.length()).toLowerCase();
            System.out.println("For " + entry.getValue() + ", we got " + name);
            stmt.setString(1, name);
            stmt.setInt(2, entry.getKey());
            stmt.executeUpdate();
            stmt.close();
        }
        conn.close();

        System.out.println(getName("Apple"));
    }

    private static String getName(String originalName) {
        System.out.println("originalName=" + originalName);
        originalName = originalName.toUpperCase();
        System.out.println("dictMap.get(" + originalName.substring(0, 1) + ")=" + dictMap.get(originalName.substring(0, 1)));
        List<String> names = dictMap.get(originalName.substring(0, 1));
        List<String> sameLenNames = new ArrayList<String>();
        if (names == null) {
            return dictMap.get("K").get(0);
        }
        for (String name : names) {
            if (name.length() == originalName.length()) {
                sameLenNames.add(name);
            }
        }

        if (sameLenNames.size() == 0) {
            return names.get(((int) (Math.random() * 100)) % names.size());
        } else {
            return sameLenNames.get(((int) (Math.random() * 100)) % sameLenNames.size());
        }
    }
}
