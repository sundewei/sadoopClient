package com.sap.mapred.db;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/19/13
 * Time: 10:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class PosRow implements DBWritable, Writable {
    public long transactionId;
    public int categoryId1;
    public int categoryId2;
    public String categoryName1;
    public String categoryName2;
    public float cost;
    public float price;
    public int quantity;

    public static final String[] FIELDS = new String[]{
            "TRANSACTION_ID",
            "CATEGORY_ID1",
            "CATEGORY_ID2",
            "CATEGORY_NAME1",
            "CATEGORY_NAME2",
            "COST",
            "PRICE",
            "QUANTITY"
    };

    public void readFields(DataInput in) throws IOException {
    }

    public void readFields(ResultSet resultSet) throws SQLException {
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(transactionId);
        out.writeInt(categoryId1);
        out.writeInt(categoryId2);
        Text.writeString(out, categoryName1);
        Text.writeString(out, categoryName2);
        out.writeFloat(cost);
        out.writeFloat(price);
        out.writeInt(quantity);
    }

    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setLong(1, transactionId);
        stmt.setInt(2, categoryId1);
        stmt.setInt(3, categoryId2);
        stmt.setString(4, categoryName1);
        stmt.setString(5, categoryName2);
        stmt.setFloat(6, cost);
        stmt.setFloat(7, price);
        stmt.setInt(8, quantity);
    }
}
