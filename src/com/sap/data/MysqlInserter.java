package com.sap.data;

import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 7/1/11
 * Time: 4:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class MysqlInserter {
    public static void main(String[] arg) throws Exception {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        String url = "jdbc:mysql://hadoop01:3306/default";
        //Connection conn = DriverManager.getConnection(url, "SYSTEM", "manager");
        Connection conn = DriverManager.getConnection(url, "visitor", "hadoopsap");
        PreparedStatement stmt = conn.prepareStatement(" INSERT INTO category_members VALUES (?, ?) ");

        FileInputStream in = new FileInputStream("C:\\projects\\freebase-wex-2011-04-30\\freebase-wex-2011-04-30-category_members.tsv");
        List<String> lines = IOUtils.readLines(in);
        int count = 0;
        boolean added = false;
        for (String line : lines) {
            String[] columns = line.split("\\t");
            if (columns.length == 2) {
                stmt.setString(1, columns[0]);
                stmt.setString(2, columns[1]);
                stmt.addBatch();
                added = false;
                count++;
                if (count % 1000000 == 0) {
                    stmt.executeBatch();
                    System.out.println("Adding " + count + "rows");
                    added = true;
                }
            }
        }
        if (!added) {
            System.out.println("Adding " + count + "rows");
            stmt.executeBatch();
        }
        conn.commit();
        stmt.close();
        conn.close();
    }
}
