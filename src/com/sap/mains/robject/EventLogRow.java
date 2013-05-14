package com.sap.mains.robject;

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
 * Date: 7/5/12
 * Time: 10:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class EventLogRow implements DBWritable, Writable {
    public static final String[] FIELDS =
            {"DATE_STRING", "USER_ID", "OTHER_ID", "EVENT_TYPE", "COMMENTS"};

    public String dateString;
    public String userId;
    public String otherId;
    public int eventType;
    public String comments;

    public void readFields(DataInput in) throws IOException {
    }

    public void readFields(ResultSet resultSet) throws SQLException {
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, dateString);
        Text.writeString(out, userId);
        Text.writeString(out, otherId);
        out.writeInt(eventType);
        Text.writeString(out, comments);
    }

    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, dateString);   // "2006-05-23"
        stmt.setString(2, userId);
        stmt.setString(3, otherId);
        stmt.setInt(4, eventType);
        stmt.setString(5, comments);
    }
}
