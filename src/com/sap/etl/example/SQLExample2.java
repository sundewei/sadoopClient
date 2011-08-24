package com.sap.etl.example;

import com.sap.hadoop.conf.ConfigurationManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 5/19/11
 * Time: 3:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class SQLExample2 {
    public static void main(String[] arg) throws SQLException {
        ConfigurationManager cm = new ConfigurationManager("I827779", "hadoopsap");
        // Get a JDBC connection to the Hive instance
        Connection conn = cm.getConnection();
        Statement stmt = conn.createStatement();
        long start = System.currentTimeMillis();
        // Get the ResultSet
        ResultSet rs = stmt.executeQuery(" SELECT sections.name, category.name " +
                " FROM sections JOIN category " +
                "      ON (sections.article_wpid = category.article_wpid)");
        int resultCount = 1;
        while (rs.next()) {
            System.out.println(resultCount + ", " + rs.getString(1) + ", " + rs.getString(2));
            resultCount++;
        }
        long end = System.currentTimeMillis();
        System.out.println("Took " + (end - start) / 1000 + " seconds");
    }
}
