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
 * Date: 9/30/11
 * Time: 11:08 AM
 * To change this template use File | Settings | File Templates.
 */
public class DbRecord implements DBWritable, Writable {
    public int locId;
    public String country;
    public String region;
    public String city;
    public String postalCode;
    public int latitude;
    public int longitude;
    public int metroCode;
    public int areaCode;

    public void readFields(DataInput in) throws IOException {
        this.locId = in.readInt();
        this.country = Text.readString(in);
        this.region = Text.readString(in);
        this.city = Text.readString(in);
        this.postalCode = Text.readString(in);
        this.latitude = in.readInt();
        this.longitude = in.readInt();
        this.metroCode = in.readInt();
        this.areaCode = in.readInt();
    }

    public void readFields(ResultSet resultSet)
            throws SQLException {
        this.locId = resultSet.getInt(1);
        this.country = resultSet.getString(2);
        this.region = resultSet.getString(3);
        this.city = resultSet.getString(4);
        this.postalCode = resultSet.getString(5);
        this.latitude = resultSet.getInt(6);
        this.longitude = resultSet.getInt(7);
        this.metroCode = resultSet.getInt(8);
        this.areaCode = resultSet.getInt(9);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.locId);
        Text.writeString(out, this.country);
        Text.writeString(out, this.region);
        Text.writeString(out, this.city);
        Text.writeString(out, this.postalCode);
        out.writeInt(this.latitude);
        out.writeInt(this.longitude);
        out.writeInt(this.metroCode);
        out.writeInt(this.areaCode);
    }

    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setInt(1, this.locId);
        stmt.setString(2, this.country);
        stmt.setString(2, this.region);
        stmt.setString(2, this.city);
        stmt.setString(2, this.postalCode);
        stmt.setInt(1, this.latitude);
        stmt.setInt(1, this.longitude);
        stmt.setInt(1, this.metroCode);
        stmt.setInt(1, this.areaCode);
    }
}
