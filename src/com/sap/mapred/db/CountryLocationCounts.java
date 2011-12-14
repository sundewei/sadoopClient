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
 * Date: 10/14/11
 * Time: 12:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class CountryLocationCounts implements DBWritable, Writable {
    public String country;
    public int locationCount;

    public void readFields(DataInput in) throws IOException {
        this.country = Text.readString(in);
        this.locationCount = in.readInt();
    }

    public void readFields(ResultSet resultSet)
            throws SQLException {
        this.country = resultSet.getString(1);
        this.locationCount = resultSet.getInt(2);
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.country);
        out.writeInt(this.locationCount);
    }

    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, this.country);
        stmt.setInt(2, this.locationCount);
    }
}
