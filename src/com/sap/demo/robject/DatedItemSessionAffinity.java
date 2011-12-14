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
 * Date: 12/12/11
 * Time: 4:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class DatedItemSessionAffinity implements DBWritable, Writable {
    public String dateString;
    public String itemLookup;
    public String affinityItemLookup;
    public int affinityCount;
    public int sessionCount;

    public void readFields(DataInput in) throws IOException {
        dateString = Text.readString(in);
        itemLookup = Text.readString(in);
        affinityItemLookup = Text.readString(in);
        affinityCount = in.readInt();
        sessionCount = in.readInt();
    }

    public void readFields(ResultSet resultSet)
            throws SQLException {
        dateString = resultSet.getString(1);
        itemLookup = resultSet.getString(2);
        affinityItemLookup = resultSet.getString(3);
        affinityCount = resultSet.getInt(4);
        sessionCount = resultSet.getInt(5);
    }

    public void write(DataOutput out) throws IOException {
        throw new IOException("Not Implemented! DO NOT CALL.");
    }

    public void write(PreparedStatement stmt) throws SQLException {
        throw new SQLException("Not Implemented! DO NOT CALL.");
    }
}
