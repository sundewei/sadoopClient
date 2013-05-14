package com.sap.demo.robject;

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
 * Date: 3/28/12
 * Time: 9:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class ItemSessionRow implements DBWritable, Writable {
    public static final String[] FIELDS = new String[]{"DATE_STRING", "ITEM_ASIN", "SESSION_COUNT"};

    public String dateString;
    public String itemAsin;
    public int sessionCount;

    public void readFields(DataInput in) throws IOException {
        throw new IOException("Not Implemented! DO NOT CALL.");
    }

    public void readFields(ResultSet resultSet)
            throws SQLException {
        throw new SQLException("Not Implemented! DO NOT CALL.");
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, dateString);
        Text.writeString(out, itemAsin);
        out.writeInt(sessionCount);
    }

    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, dateString);   // "2006-05-23"
        stmt.setString(2, itemAsin);
        stmt.setInt(3, sessionCount);
    }
}
