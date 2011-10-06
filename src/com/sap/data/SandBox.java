package com.sap.data;



import org.apache.commons.csv.CSVUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 8/19/11
 * Time: 10:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class SandBox {
    public static void main(String[] arg) throws Exception {
        String file = "C:\\projects\\sadoopClient\\data\\GeoLiteCity-Location.csv";
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        //String url = "jdbc:sap://10.79.0.21:34015/I827779";
        String url = "jdbc:sap://COE-HE-28.WDF.SAP.CORP:30115/I827779";
        //Connection conn = DriverManager.getConnection(url, "SYSTEM", "manager");
        Connection conn = DriverManager.getConnection(url, "I827779", "Google6377");
        conn.setAutoCommit(false);
        String query =
                " INSERT INTO CITY_LOCATIONS (LOC_ID, COUNTRY, REGION, CITY, POSTAL_CODE, LATITUDE, LONGITUDE, METRO_CODE, AREA_CODE) " +
                        " VALUES (?,?,?,?,?,?,?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        List<String> lines = FileUtils.readLines(new File(file));
        int lineCount = 0;
        boolean doneBatch = false;
System.out.println("query=\n"+query);
        for (String line: lines) {
            if (lineCount > 0) {
                String[] columns = CSVUtils.parseLine(line);
                for (int i=0; i<columns.length; i++) {
//System.out.println((i+1) + " = " + columns[i]);
                    if (i < 4) {
                        stmt.setString(i+1, columns[i]);
                    } else {
                        int value = 0;
                        if (columns[i] == null || columns[i].equals("")) {
                            value = 0;
                        } else {
                            value = Integer.parseInt(columns[0]);
                        }
                        stmt.setInt(i+1, value);
                    }
                }
            }
//System.out.println("\n\n\n");
            if (lineCount > 0) {
                stmt.addBatch();
            }
            doneBatch = false;
            if (lineCount > 0 && lineCount % 1000 == 0) {
                System.out.println("lineCount=" + lineCount);
                stmt.executeBatch();
                doneBatch = true;
                conn.commit();
                System.out.println("Commit add batch...:" + lineCount);
            }
            lineCount++;
        }
        if (!doneBatch) {
            stmt.executeBatch();
            conn.commit();
            System.out.println("Commit last batch...:" + lineCount);
        }
        conn.commit();
        stmt.close();
        conn.close();
    }

    public static void main1(String[] arg) throws Exception {
        String file = "C:\\projects\\sadoopClient\\data\\GeoLiteCity-Location.csv";
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        //String url = "jdbc:sap://10.79.0.21:34015/I827779";
        String url = "jdbc:sap://COE-HE-28.WDF.SAP.CORP:30115/I827779";
        //Connection conn = DriverManager.getConnection(url, "SYSTEM", "manager");
        Connection conn = DriverManager.getConnection(url, "I827779", "Google6377");

        String query =
                " select * from CITY_LOCATIONS ";
        PreparedStatement stmt = conn.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();
        while(rs.next()) {
            System.out.println(rs.getInt(1));
        }
        rs.close();
        stmt.close();
        conn.close();
    }
}
