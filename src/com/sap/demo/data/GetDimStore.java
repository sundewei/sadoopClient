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
 * Time: 12:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class GetDimStore {
    public static void main(String[] arg) throws Exception {
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        String url = "jdbc:sap://hphanar04.wdf.sap.corp:30015";
        Connection conn = DriverManager.getConnection(url, "SYSTEM", "manager");

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select STORE_NUM,\n" +
                "RETAILER_CD,\n" +
                "STORE_DESC,\n" +
                "STORE_ABBR,\n" +
                "STORE_TYPE,\n" +
                "STORE_NAME,\n" +
                "ADDRESS_1,\n" +
                "ADDRESS_2,\n" +
                "CITY,\n" +
                "STATE,\n" +
                "ZIP_CODE,\n" +
                "AREA_CODE,\n" +
                "PHONE_NUM,\n" +
                "ADD_DATE,\n" +
                "LAST_MOD_DATE,\n" +
                "CLOSED_DATE\n" +
                "from SYSTEM.DIM_STORE ");
        String data = Utility.getCsvTable(rs);
        stmt.close();
        rs.close();
        conn.close();
        FileUtils.write(new File("C:\\projects\\data\\dimension\\store.csv"), data);
    }

}
