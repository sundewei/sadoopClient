package com.sap.demo.data;

import com.sap.demo.Utility;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/27/11
 * Time: 12:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class GetDimDate {
    public static void main(String[] arg) throws Exception {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        String url = "jdbc:mysql://hadoop01:3306/test";
        Connection conn = DriverManager.getConnection(url, "root", "root");

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM DATE_DIM");
        String data = Utility.getCsvTable(rs);
        stmt.close();
        rs.close();
        conn.close();
        FileUtils.write(new File("C:\\projects\\data\\dimension\\date.csv"), data);
    }
}
