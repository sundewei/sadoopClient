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
 * Date: 12/3/11
 * Time: 11:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class AffinityRow implements DBWritable, Writable {

    public static final String[] FIELDS =
            {"DATE_STRING", "ITEM_ASIN", "AFFINITY_ITEM_ASIN", "AFFINITY_COUNT", "SESSION_COUNT"};

    public String dateString;
    public String itemAsin;
    public String affinityItemAsin;
    public int affinityCount;
    public int sessionCount;

    public void readFields(DataInput in) throws IOException {
    }

    public void readFields(ResultSet resultSet) throws SQLException {
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, dateString);
        Text.writeString(out, itemAsin);
        Text.writeString(out, affinityItemAsin);
        out.writeInt(affinityCount);
        out.writeInt(sessionCount);
    }

    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, dateString);   // "2006-05-23"
        stmt.setString(2, itemAsin);
        stmt.setString(3, affinityItemAsin);
        stmt.setInt(4, affinityCount);
        stmt.setInt(5, sessionCount);
    }
}
