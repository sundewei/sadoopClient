package com.sap.mapred;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.mapred.db.CityLocations;
import com.sap.mapred.db.CountryLocationCounts;
import com.sap.mapred.db.HanaDBOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;

import java.io.IOException;


/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 9/27/11
 * Time: 2:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class DbTableWriteExample {

    public static class DbRecordMapper extends Mapper<LongWritable, CityLocations, Text, IntWritable> {
        private static final Text OUTPUT_KEY = new Text();
        private static final IntWritable OUTPUT_VALUE = new IntWritable();

        public void map(LongWritable inKey, CityLocations record, Context context)
                throws IOException, InterruptedException {
            OUTPUT_KEY.set(record.country);
            OUTPUT_VALUE.set(record.locId);
            context.write(OUTPUT_KEY, OUTPUT_VALUE);
        }
    }

    /**
     * The reduce class
     */
    public static class DbRecordReducer extends Reducer<Text, IntWritable, CountryLocationCounts, NullWritable> {
        private static final CountryLocationCounts OUTPUT_KEY = new CountryLocationCounts();
        private static final NullWritable NULL = NullWritable.get();

        public void reduce(Text inKey, Iterable<IntWritable> inValues, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : inValues) {
                count++;
            }
            OUTPUT_KEY.country = inKey.toString();
            OUTPUT_KEY.locationCount = count;
            context.write(OUTPUT_KEY, NULL);
        }
    }

    public static void main(String[] arg) throws Exception {
        ConfigurationManager configurationManager = new ConfigurationManager("I827779", "hadoopsap");
        Configuration conf = configurationManager.getConfiguration();
        Job job = new Job(conf, "DBTest");

        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        DBConfiguration.configureDB(job.getConfiguration(), "com.sap.db.jdbc.Driver", "jdbc:sap://COE-HE-28.WDF.SAP.CORP:30115/I827779", "I827779", "Google6377");
        conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.sap.db.jdbc.Driver");

        //Class.forName("com.mysql.jdbc.Driver").newInstance();
        //DBConfiguration.configureDB(job.getConfiguration(), "com.mysql.jdbc.Driver", "jdbc:mysql://hadoop01:3306/test", "root", "root");
        //conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.mysql.jdbc.Driver");


        /*
        DBConfiguration dbc = new DBConfiguration(conf);
        Connection conn = dbc.getConnection();
        System.out.println(conn);
        */

        String[] fields = {"LOC_ID", "COUNTRY", "REGION", "CITY", "POSTAL_CODE", "LATITUDE", "LONGITUDE", "METRO_CODE", "AREA_CODE"};
        DBInputFormat.setInput(job, CityLocations.class, "CITY_LOCATIONS", "", "LOC_ID", fields);
        job.setJarByClass(DbTableWriteExample.class);

        job.setMapperClass(DbTableWriteExample.DbRecordMapper.class);
        job.setReducerClass(DbTableWriteExample.DbRecordReducer.class);

        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(HanaDBOutputFormat.class);

        // Map's outputs
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reduce's outputs
        job.setOutputKeyClass(CountryLocationCounts.class);
        job.setOutputValueClass(NullWritable.class);

        HanaDBOutputFormat.setOutput(job, "COUNTRY_LOCATION_COUNTS", "COUNTRY", "LOCATION_COUNT");
        job.waitForCompletion(true);
        /*
        DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
        String tableName = dbConf.getOutputTableName();
        String[] fieldNames = dbConf.getOutputFieldNames();
        DBOutputFormat ooo = new DBOutputFormat();
        System.out.println(ooo.constructQuery(tableName, fieldNames));
        */
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
