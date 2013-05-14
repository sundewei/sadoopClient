package com.sap.etl.example;

import com.sap.hadoop.conf.ConfigurationManager;

import java.sql.Connection;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 7/5/11
 * Time: 3:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class Tester {
    public static void main(String[] arg) throws Exception {

        ConfigurationManager cm = new ConfigurationManager("ubuntu", "");
        Connection conn = cm.getConnection();
        System.out.println(conn.getMetaData().getDatabaseProductName());

    }
}
