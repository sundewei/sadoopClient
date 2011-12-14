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
 * Date: 11/14/11
 * Time: 2:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class RequestRow implements DBWritable, Writable {

    public static final String[] FIELDS =
            {"SESSION_NUM", "IP", "REQUEST_MS", "START_MS", "END_MS", "REQUESTED_PAGE", "SESSION_LENGTH", "ITEM_LOOKUP", "IP_NUM"};

    public String sessionNum;
    public String ip;
    public long requestMs;
    public long startMs;
    public long endMs;
    public String requestedPage;
    public long sessionLengthMs;
    public String itemLookup;
    public long ipGeoNum;


    public void readFields(DataInput in) throws IOException {
        this.sessionNum = Text.readString(in);
        this.ip = Text.readString(in);
        this.requestMs = in.readLong();
        this.startMs = in.readLong();
        this.endMs = in.readLong();
        this.requestedPage = Text.readString(in);
        this.sessionLengthMs = in.readLong();
        this.itemLookup = Text.readString(in);
        this.ipGeoNum = in.readLong();
    }

    public void readFields(ResultSet resultSet)
            throws SQLException {
        this.sessionNum = resultSet.getString(1);
        this.ip = resultSet.getString(2);
        this.requestMs = resultSet.getLong(3);
        this.startMs = resultSet.getLong(4);
        this.endMs = resultSet.getLong(5);
        this.requestedPage = resultSet.getString(6);
        this.sessionLengthMs = resultSet.getLong(7);
        this.itemLookup = resultSet.getString(8);
        this.ipGeoNum = resultSet.getLong(9);
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.sessionNum);
        Text.writeString(out, this.ip);
        out.writeLong(this.requestMs);
        out.writeLong(this.startMs);
        out.writeLong(this.endMs);
        Text.writeString(out, this.requestedPage);
        out.writeLong(this.sessionLengthMs);
        Text.writeString(out, this.itemLookup);
        out.writeLong(this.ipGeoNum);
    }

    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, this.sessionNum);
        stmt.setString(2, this.ip);
        stmt.setLong(3, this.requestMs);
        stmt.setLong(4, this.startMs);
        stmt.setLong(5, this.endMs);
        stmt.setString(6, this.requestedPage);
        stmt.setLong(7, this.sessionLengthMs);
        stmt.setString(8, this.itemLookup);
        stmt.setLong(9, this.ipGeoNum);
    }

    @Override
    public String toString() {
        return "RequestRow{" +
                "sessionNum='" + sessionNum + '\'' +
                ", ip='" + ip + '\'' +
                ", requestMs=" + requestMs +
                ", startMs=" + startMs +
                ", endMs=" + endMs +
                ", requestedPage='" + requestedPage + '\'' +
                ", sessionLengthMs=" + sessionLengthMs +
                ", itemLookup='" + itemLookup + '\'' +
                ", ipGeoNum=" + ipGeoNum +
                '}';
    }
}
