package com.sap.mains;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 5/8/12
 * Time: 12:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class DataCopier {
    public static Connection getHDBConnection() throws Exception {
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        String url = "jdbc:sap://10.79.1.13:30015?reconnect=true";
        return DriverManager.getConnection(url, "SYSTEM", "Hana2012");
    }

    public static Connection getCPRConnection() throws Exception {
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        String url = "jdbc:sap://10.165.27.75:31015?reconnect=true";
        return DriverManager.getConnection(url, "SYSTEM", "Admin123");
    }

    public static Connection getJH1Connection() throws Exception {
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        String url = "jdbc:sap://10.48.144.94:31015?reconnect=true";
        return DriverManager.getConnection(url, "SYSTEM", "Hana1234");
    }

    public static void main(String[] arg) throws Exception {
        Connection cprConn = getCPRConnection();
        Connection hdbConn = getJH1Connection();

        String getQuery = " SELECT DATE_ID, ITEM_ID,LOCATION_ID, CUSTOMER_ID,TRANSACTION_NUMBER, SALES_QUANTITY, SALES_DOLLARS ,COST_DOLLARS, PROFIT_DOLLARS, ITEM_ASIN, DATE_STRING" +
                " FROM SYSTEM.POS_FACT \n ";
        PreparedStatement getStmt = hdbConn.prepareStatement(getQuery);

        String putQuery = " INSERT INTO SYSTEM.POS_FACT VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?) ";
        PreparedStatement putStmt = cprConn.prepareStatement(putQuery);
        ResultSet rs = getStmt.executeQuery();
        int rowCount = 0;
        int batchRowCount = 0;
        while (rs.next()) {
            long txId = rs.getLong(5);
            //if (txId % 1000 < 15) {
            rowCount++;
            batchRowCount++;
            putStmt.setInt(1, rs.getInt(1));
            putStmt.setInt(2, rs.getInt(2));
            putStmt.setInt(3, rs.getInt(3));
            putStmt.setInt(4, rs.getInt(4));

            putStmt.setLong(5, rs.getLong(5));

            putStmt.setDouble(6, rs.getDouble(6));
            putStmt.setDouble(7, rs.getDouble(7));
            putStmt.setDouble(8, rs.getDouble(8));
            putStmt.setDouble(9, rs.getDouble(9));

            putStmt.setString(10, rs.getString(10));

            putStmt.setString(11, rs.getString(11));
            putStmt.addBatch();
            if (rowCount % 1000 == 0) {
                System.out.println("rowCount=" + rowCount);
                putStmt.executeBatch();
                batchRowCount = 0;
            }
            //}
        }
        if (batchRowCount > 0) {
            System.out.println("Last batch...rowCount=" + rowCount);
            putStmt.executeBatch();
        }
        putStmt.close();
        getStmt.close();
        rs.close();
        cprConn.close();
        hdbConn.close();
    }
}
