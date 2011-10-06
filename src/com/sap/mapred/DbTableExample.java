package com.sap.mapred;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.mapred.db.DbRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.sql.Connection;


/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 9/27/11
 * Time: 2:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class DbTableExample {

    public static class DbRecordMapper extends Mapper<LongWritable, DbRecord, Text, IntWritable> {
        private static final Text OUTPUT_KEY = new Text();
        private static final IntWritable OUTPUT_VALUE = new IntWritable();

        public void map(LongWritable inKey, DbRecord record, Context context)
                throws IOException, InterruptedException {
            OUTPUT_KEY.set(record.country + "," + record.city);
            OUTPUT_VALUE.set(record.locId);
            context.write(OUTPUT_KEY, OUTPUT_VALUE);
        }
    }

    /**
     * The reduce class
     */
    public static class DbRecordReducer extends Reducer<Text, IntWritable, Text, Text> {
        private static final Text OUTPUT_VALUE = new Text();

        public void reduce(Text inKey, Iterable<IntWritable> inValues, Context context)
                throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (IntWritable value : inValues) {
                sb.append(value).append(",");
            }
            OUTPUT_VALUE.set(sb.toString());
            context.write(inKey, OUTPUT_VALUE);
        }
    }

    public static void main(String[] arg) throws Exception {
        ConfigurationManager configurationManager = new ConfigurationManager("I827779", "hadoopsap");
        Configuration conf = configurationManager.getConfiguration();
        Job job = new Job(conf, "DBTest");

        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        DBConfiguration.configureDB(job.getConfiguration(), "com.sap.db.jdbc.Driver", "jdbc:sap://COE-HE-28.WDF.SAP.CORP:30115/I827779", "I827779", "Google6377");
        conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.sap.db.jdbc.Driver");
//        DBConfiguration dbc = new DBConfiguration(conf);
//        Connection conn = dbc.getConnection();
//        System.out.println(conn);


        String[] fields = {"LOC_ID", "COUNTRY", "REGION", "CITY", "POSTAL_CODE", "LATITUDE", "LONGITUDE", "METRO_CODE", "AREA_CODE"};
        DBInputFormat.setInput(job, DbRecord.class, "CITY_LOCATIONS", "", "LOC_ID", fields);
        job.setJarByClass(DbTableExample.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(DbTableExample.DbRecordMapper.class);
        job.setReducerClass(DbTableExample.DbRecordReducer.class);
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/user/I827779/dbOutput/"));
        job.waitForCompletion(true);
    }
}

/*
create column table "I827779"."CITY_LOCATIONS"(
	"LOC_ID" INTEGER not null,
	"COUNTRY" VARCHAR (50) default '',
	"REGION" VARCHAR (50) default '',
	"CITY" VARCHAR (50) default '',
	"POSTAL_CODE" VARCHAR (10) default '',
	"LATITUDE" INTEGER,
	"LONGITUDE" INTEGER,
	"METRO_CODE" INTEGER,
	"AREA_CODE" INTEGER,
primary key ("LOC_ID"))
 */
