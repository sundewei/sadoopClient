package com.sap.demo.pos;

import com.sap.demo.pos.beans.AmazonProduct;
import com.sap.demo.pos.beans.Category;
import org.apache.commons.csv.CSVUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 2/8/12
 * Time: 3:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class DatabaseUtility {
    public static final DateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    public static Connection getConnection() throws Exception {
        //Class.forName("org.postgresql.Driver");
        //String url = "jdbc:postgresql://pald00473749a.dhcp.pal.sap.corp:5432/postgres";
        //Connection conn = DriverManager.getConnection(url, "admin", "abcd1234");

        //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        //Connection conn = DriverManager.getConnection("jdbc:sqlserver://10.48.101.84:1433", "dewei", "dewei");
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        String url = "jdbc:sap://10.48.144.94:31015?reconnect=true";
        return DriverManager.getConnection(url, "SYSTEM", "Hana1234");
    }

    public static Connection getCprConnection() throws Exception {
        //Class.forName("org.postgresql.Driver");
        //String url = "jdbc:postgresql://pald00473749a.dhcp.pal.sap.corp:5432/postgres";
        //Connection conn = DriverManager.getConnection(url, "admin", "abcd1234");

        //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        //Connection conn = DriverManager.getConnection("jdbc:sqlserver://10.48.101.84:1433", "dewei", "dewei");
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        String url = "jdbc:sap://10.165.27.75:31015?reconnect=true";
        return DriverManager.getConnection(url, "SYSTEM", "Admin123");
    }

    public static void close(PreparedStatement pstmt) throws SQLException {
        if (pstmt != null) {
            pstmt.close();
        }
    }

    public static void close(ResultSet rs) throws SQLException {
        if (rs != null) {
            rs.close();
        }
    }

    public static void close(Connection conn) throws Exception {
        if (conn != null) {
            conn.clearWarnings();
            conn.close();
        }
    }

    public static Map<Integer, String> getItemIdAsinMap() throws Exception {
        Connection connection = getConnection();
        Map<Integer, String> idAsinMap = new HashMap<Integer, String>();
        String sql1 = " select id, asin from HADOOP.item_dim ";

        PreparedStatement stmt = connection.prepareStatement(sql1);
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            idAsinMap.put(rs.getInt(1), rs.getString(2));
        }
        rs.close();
        stmt.close();
        connection.close();
        return idAsinMap;
    }

    public static void insertPosRows(Connection conn, Collection<PosRowGenerator.PosRow> posRows) throws Exception {
        String sql = " INSERT INTO HADOOP.POS_FACT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        int count = 1;
        boolean needInserting = true;

        for (PosRowGenerator.PosRow posRow : posRows) {
            needInserting = true;
            preparedStatement.setInt(1, posRow.dateId);
            preparedStatement.setInt(2, posRow.itemId);
            preparedStatement.setInt(3, posRow.locationId);
            preparedStatement.setInt(4, posRow.customerId);
            preparedStatement.setLong(5, posRow.transactionNumber);
            preparedStatement.setInt(6, posRow.salesQuantity);
            preparedStatement.setDouble(7, posRow.salesDollars / 100);
            preparedStatement.setDouble(8, posRow.costDollars / 100);
            preparedStatement.setDouble(9, posRow.profitDollars / 100);
            preparedStatement.setString(10, PosRowGenerator.AMAZON_PRODUCT_MAP.get(posRow.itemId).getAsin());
            preparedStatement.setString(11, PosRowGenerator.ID_DATE_STRING_MAP.get(posRow.dateId));
            preparedStatement.addBatch();

            if (count % 250 == 0) {
                try {
                    preparedStatement.executeBatch();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                needInserting = false;
            }
            count++;
        }
        if (needInserting) {
            preparedStatement.executeBatch();
            //    conn.commit();
        }
        //conn.setAutoCommit(true);
        preparedStatement.close();
    }

    public static Map<Integer, AmazonProduct> getAmazonProductMap() throws Exception {
        Connection connection = getConnection();
        Map<Integer, AmazonProduct> amazonProductMap = new HashMap<Integer, AmazonProduct>();
        String sql1 = " select * from HADOOP.item_dim ";
        String sql2 = " select item_id, category_id, category_level, name \n" +
                "from HADOOP.item_categories ic, HADOOP.categories c \n" +
                "where ic.category_id = c.id ";
        PreparedStatement pstmt = connection.prepareStatement(sql1);
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            AmazonProduct amazonProduct = new AmazonProduct();
            amazonProduct.setId(rs.getInt(1));
            amazonProduct.setDescription(rs.getString(2));
            amazonProduct.setAsin(rs.getString(3));
            amazonProduct.setById(rs.getInt(4));
            amazonProduct.setTitle(rs.getString(5));
            amazonProduct.setNumOfReviews(rs.getInt(6));
            amazonProduct.setAvgRating(rs.getFloat(7));
            amazonProduct.setCost(rs.getDouble(8));
            amazonProduct.setPrice(rs.getDouble(9));
            amazonProduct.setTopCategoryId(rs.getInt(10));
            amazonProductMap.put(amazonProduct.getId(), amazonProduct);
        }
        rs.close();
        pstmt.close();

        Map<Integer, Category> categoryMap = new HashMap<Integer, Category>();
        pstmt = connection.prepareStatement(sql2);
        rs = pstmt.executeQuery();
        while (rs.next()) {
            Category category = categoryMap.get(rs.getInt(2));
            if (category == null) {
                category = new Category();
                category.setId(rs.getInt(2));
                category.setName(rs.getString(4));
                categoryMap.put(category.getId(), category);
            }

            AmazonProduct amazonProduct = amazonProductMap.get(rs.getInt(1));
            amazonProduct.addCategory(rs.getInt(3), category);
        }
        rs.close();
        pstmt.close();
        connection.close();
        return amazonProductMap;
    }

    public static Map<String, List<Integer>> getStateLocationIdMap() throws Exception {
        Map<String, List<Integer>> map = new HashMap<String, List<Integer>>();
        Connection conn = getConnection();
        String sql = "select region, id from HADOOP.location_dim where country = 'US' and region != ''";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();

        while (rs.next()) {
            List<Integer> locIds = map.get(rs.getString(1));
            if (locIds == null) {
                locIds = new ArrayList<Integer>();
            }
            locIds.add(rs.getInt(2));
            map.put(rs.getString(1), locIds);
        }
        rs.close();
        pstmt.close();
        conn.close();
        return map;
    }

    public static Map<String, Integer> getDateStringIdMap() throws Exception {
        Map<String, Integer> map = new HashMap<String, Integer>();
        String sql = " select date_string, id from HADOOP.date_dim ";
        Connection conn = getConnection();
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        ResultSet rs = preparedStatement.executeQuery();
        while (rs.next()) {
            map.put(rs.getString(1), rs.getInt(2));
        }
        return map;
    }

    public static Map<String, Integer> getIdMap(Connection conn, String sql) throws Exception {
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        Map<String, Integer> map = new HashMap<String, Integer>();
        ResultSet rs = preparedStatement.executeQuery();
        while (rs.next()) {
            map.put(rs.getString(1), rs.getInt(2));
        }
        preparedStatement.close();
        return map;
    }

    public static void populateCategory() throws Exception {
        Connection conn = getConnection();
        String folder = Utility.BASE_FOLDER + "dataCollector/amazonData/products/info/";
        File folderFile = new File(folder);
        //String filename = Utility.BASE_FOLDER + "dataCollector/amazonData/targetted/newInserted.txt";
        Map<String, Integer> companyIdMap = getIdMap(conn, "select name, id from HADOOP.companies");
        Map<String, Integer> categoryIdMap = getIdMap(conn, "select name, id from HADOOP.categories");
        int index = 0;
        //List<String> asinLines = FileUtils.readLines(new File(filename));
        int maxCatId = 0;
        int maxComId = 0;
        for (File file : folderFile.listFiles()) {
            //for (String asinLine: asinLines) {
            //File file = new File(folder + asinLine + ".category");
            //File file = new File("C:/projects/dataCollector/amazonData/products/info/B001HAYP2Q.category");
            index++;
            if (file.isFile()) {
                String name = file.getName();
                System.out.println(index + "," + name);
                if (name.contains(".byCompany")) {
                    String asin = name.replace(".byCompany", "");
                    if (!exist(conn, asin)) {
                        System.out.println("--->Skipping " + name);
                        continue;
                    }
                    List<String> lines = FileUtils.readLines(file);
                    String[] values = CSVUtils.parseLine(lines.get(0));
                    boolean newCompany = false;
                    if (!companyIdMap.containsKey(values[1])) {
                        companyIdMap.put(values[1], maxComId + 1);
                        newCompany = true;
                        maxComId++;
                    }
                    insertProductCompany(conn, asin, companyIdMap.get(values[1]), values[1], newCompany);

                } else if (name.contains(".category")) {
                    String asin = name.replace(".category", "");
                    if (!exist(conn, asin)) {
                        System.out.println("--->Skipping " + name);
                        continue;
                    }
                    List<String> lines = trimCategory(FileUtils.readLines(file));

                    if (lines.size() > 1) {
                        boolean addedCategory = false;
                        for (String line : lines) {
                            String[] values = line.split(" > ");
                            int level = 1;
                            for (String value : values) {
                                if (value.length() == 0) {
                                    continue;
                                }
                                boolean newCategory = false;
                                if (!categoryIdMap.containsKey(value)) {
                                    categoryIdMap.put(value, maxCatId + 1);
                                    newCategory = true;
                                    maxCatId++;
                                }
//System.out.println("NNNN CAT = " + value);
                                insertProductCatgeory(conn, asin, categoryIdMap.get(value), value, level, newCategory);
                                level++;
                                addedCategory = true;
                            }
                            if (addedCategory) {
                                break;
                            }
                        }
                    } else {
                        int level = 1;
                        boolean newCategory = false;
                        String cat = lines.get(0).replace(">", "").trim();
//System.out.println("1111 CAT = " + cat);
                        if (!categoryIdMap.containsKey(cat)) {
                            categoryIdMap.put(cat, maxCatId + 1);
                            newCategory = true;
                            maxCatId++;
                        }
                        insertProductCatgeory(conn, asin, categoryIdMap.get(cat), cat, level, newCategory);
                    }
                }
            }
        }
    }

    private static void insertProductCompany(Connection conn, String asin, int comId, String comName, boolean newCompany)
            throws Exception {
        String query1 = "INSERT INTO HADOOP.COMPANIES VALUES (?, ?) ";
        String query2 = " UPDATE HADOOP.ITEM_DIM SET BY = ? WHERE asin = ?";
        PreparedStatement pstmt1;
        PreparedStatement pstmt2;
        if (newCompany) {
            pstmt1 = conn.prepareStatement(query1);
            pstmt1.setInt(1, comId);
            pstmt1.setString(2, comName);
            pstmt1.execute();
            pstmt1.close();
        }
        pstmt2 = conn.prepareStatement(query2);
        pstmt2.setInt(1, comId);
        pstmt2.setString(2, asin);
        pstmt2.execute();
        pstmt2.close();
    }

    private static boolean exist(Connection conn, String asin) throws Exception {
        String query = " SELECT count(0) from HADOOP.ITEM_DIM where asin = ? ";
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setString(1, asin);
        ResultSet rs = stmt.executeQuery();
        int count = 0;
        while (rs.next()) {
            count = rs.getInt(1);
        }
        rs.close();
        stmt.close();
        return count > 0;
    }

    private static void insertProductCatgeory(Connection conn, String asin, int catId, String catName, int level, boolean newCategory)
            throws Exception {
        String query1 = "INSERT INTO HADOOP.CATEGORIES VALUES (?, ?) ";
        String query2 = " INSERT INTO HADOOP.ITEM_CATEGORIES VALUES ((select id from HADOOP.item_dim where asin = ?), ?, ?)";
        String query3 = "SELECT COUNT(*) from HADOOP.ITEM_CATEGORIES WHERE item_id = (select id from HADOOP.item_dim where asin = ?) and category_id = ? and category_level = ?";
        String query4 = "UPDATE HADOOP.ITEM_DIM SET TOP_CATEGORY_ID = ? WHERE ASIN = ?";
        PreparedStatement pstmt1;
        PreparedStatement pstmt2;
        PreparedStatement pstmt3;
        PreparedStatement pstmt4;
        if (newCategory) {
            pstmt1 = conn.prepareStatement(query1);
            System.out.println("About to insert new category with id = " + catId + ", name = " + catName);
            pstmt1.setInt(1, catId);
            pstmt1.setString(2, catName);
            pstmt1.execute();
            pstmt1.close();
        }
        boolean exist = false;
        pstmt3 = conn.prepareStatement(query3);
        pstmt3.setString(1, asin);
        pstmt3.setInt(2, catId);
        pstmt3.setInt(3, level);
        ResultSet rs = pstmt3.executeQuery();
        while (rs.next()) {
            exist = rs.getInt(1) > 0;
        }
        rs.close();
        pstmt3.close();

        if (!exist) {
            pstmt2 = conn.prepareStatement(query2);
            pstmt2.setString(1, asin);
            pstmt2.setInt(2, catId);
            pstmt2.setInt(3, level);
            pstmt2.execute();
            pstmt2.close();
        }
        if (level == 1) {
            pstmt4 = conn.prepareStatement(query4);
            pstmt4.setInt(1, catId);
            pstmt4.setString(2, asin);
            pstmt4.execute();
            pstmt4.close();
        }
    }

    private static List<String> trimCategory(List<String> lines) {
        List<String> newLines = new ArrayList<String>();
        for (String line : lines) {
//System.out.println("a, " + line.trim());
            line = line.replace("Amazon Best Sellers Rank:", "");
//System.out.println("b, " + line.trim());
            if (line.contains("(")) {
                line = line.substring(0, line.indexOf("("));
            }
//System.out.println("c, " + line.trim());
            if (line.contains("in ")) {
                int inIndex = line.indexOf("in ");
                line = line.substring(inIndex + 3, line.length());
            }
//System.out.println("d, " + line.trim());
            String newLine = line.trim();

            newLines.add(line.trim());

        }
        return newLines;
    }

    public static void populateMetros() throws Exception {
        String inputFilename = Utility.BASE_FOLDER + "GeoLiteCity_20120207/metrocodes.csv";
        Connection conn = getConnection();
        String query = " INSERT INTO HADOOP.METROS VALUES (?, ?, ?) ";
        PreparedStatement preparedStatement = conn.prepareStatement(query);
        List<String> lines = FileUtils.readLines(new File(inputFilename));
        int lineNumber = 0;
        for (String line : lines) {
            if (lineNumber >= 1) {
                String[] values = CSVUtils.parseLine(line);
                preparedStatement.setString(1, values[0]);
                preparedStatement.setString(2, values[1]);
                preparedStatement.setInt(3, Integer.parseInt(values[2]));
                preparedStatement.execute();
            }
            lineNumber++;
        }
        preparedStatement.close();
        conn.close();
    }

    public static void populateIpBlocks() throws Exception {
        String inputFilename = Utility.BASE_FOLDER + "GeoLiteCity_20120207/GeoLiteCity-Blocks.csv";
        Connection conn = getConnection();
        String query = " INSERT INTO HADOOP.IP_BLOCKS VALUES (?, ?, ?) ";
        PreparedStatement preparedStatement = conn.prepareStatement(query);
        List<String> lines = FileUtils.readLines(new File(inputFilename));
        int lineNumber = 0;
        boolean updated = false;
        for (String line : lines) {
            if (lineNumber >= 2) {
                String[] values = CSVUtils.parseLine(line);
                preparedStatement.setLong(1, Long.parseLong(values[0]));
                preparedStatement.setLong(2, Long.parseLong(values[1]));
                preparedStatement.setLong(3, Long.parseLong(values[2]));
                preparedStatement.addBatch();
                updated = false;
                if (lineNumber % 10000 == 0) {
                    System.out.println(lineNumber);
                    preparedStatement.executeBatch();
                    updated = true;
                }
            }
            lineNumber++;
        }
        if (!updated) {
            System.out.println("Last batch... " + lineNumber);
            preparedStatement.executeBatch();
        }
        preparedStatement.close();
        conn.close();
    }

    public static void populateLocationDimension() throws Exception {
        String inputFilename = Utility.BASE_FOLDER + "GeoLiteCity_20120207/GeoLiteCity-Location.csv";
        Connection conn = getConnection();
        String query = " INSERT INTO HADOOP.LOCATION_DIM VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ";
        PreparedStatement preparedStatement = conn.prepareStatement(query);

        List<String> lines = FileUtils.readLines(new File(inputFilename));
        int lineNumber = 0;
        for (String line : lines) {
            String[] values = CSVUtils.parseLine(line);
            if (lineNumber >= 2) {
                if (values[1].equals("US")) {
                    System.out.println(line);
                    preparedStatement.setInt(1, Integer.parseInt(values[0]));
                    preparedStatement.setString(2, values[1]);
                    preparedStatement.setString(3, values[2]);
                    preparedStatement.setString(4, values[3]);
                    preparedStatement.setString(5, values[4]);
                    preparedStatement.setFloat(6, Float.parseFloat(values[5]));
                    preparedStatement.setFloat(7, Float.parseFloat(values[6]));
                    if (!values[7].equals("")) {
                        preparedStatement.setInt(8, Integer.parseInt(values[7]));
                    } else {
                        preparedStatement.setObject(8, null);
                    }
                    if (!values[8].equals("")) {
                        preparedStatement.setInt(9, Integer.parseInt(values[8]));
                    } else {
                        preparedStatement.setObject(9, null);
                    }
                    preparedStatement.execute();
                }
            }
            lineNumber++;
        }
    }

    public static void populateDateDimension() throws Exception {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        String query = " INSERT INTO HADOOP.DATE_DIM (id, calendar_date, date_string, " +
                "yyyy, mm, dd, " +
                "day_of_week, day_of_month, day_of_year, week_of_year, month_of_year, quarter_of_year) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";

        PreparedStatement pstmt = conn.prepareStatement(query);
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, 2008);
        calendar.set(Calendar.MONTH, Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        for (int i = 1; i <= 2000; i++) {
            pstmt.setInt(1, i);
            pstmt.setTimestamp(2, new Timestamp(calendar.getTime().getTime()));
            pstmt.setString(3, SIMPLE_DATE_FORMAT.format(calendar.getTime()));
            pstmt.setInt(4, calendar.get(Calendar.YEAR));
            pstmt.setInt(5, calendar.get(Calendar.MONTH) + 1);
            pstmt.setInt(6, calendar.get(Calendar.DAY_OF_MONTH));

            pstmt.setInt(7, calendar.get(Calendar.DAY_OF_WEEK));
            pstmt.setInt(8, calendar.get(Calendar.DAY_OF_MONTH));
            pstmt.setInt(9, calendar.get(Calendar.DAY_OF_YEAR));
            pstmt.setInt(10, calendar.get(Calendar.WEEK_OF_YEAR));
            pstmt.setInt(11, calendar.get(Calendar.MONTH) + 1);
            if (calendar.get(Calendar.MONTH) <= 3) {
                pstmt.setInt(12, 1);
            } else if (calendar.get(Calendar.MONTH) > 3 && calendar.get(Calendar.MONTH) <= 6) {
                pstmt.setInt(12, 2);
            } else if (calendar.get(Calendar.MONTH) > 6 && calendar.get(Calendar.MONTH) <= 9) {
                pstmt.setInt(12, 3);
            } else {
                pstmt.setInt(12, 4);
            }
            calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR) + 1);
            pstmt.execute();
        }
        conn.commit();
        pstmt.close();
        conn.close();
    }

    public static void main(String[] arg) throws Exception {

        getAmazonProductMap();
    }
}
