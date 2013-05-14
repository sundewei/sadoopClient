package com.sap.mapred;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.mapred.db.PosRow;
import org.apache.commons.csv.CSVUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 3/26/13
 * Time: 10:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class HbaseMrBulkDataBenchmarker {
    private static String INPUT_PATH = "/data/posDataSmall";
    private static String OUTPUT_PATH;
    private static final String TABLE_NAME = "pos_rows";

    public static class HbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private static final byte[] FAMILY_BYTES = Bytes.toBytes("cf");
        private static final byte[] COLUMN_TRANSACTION_ID = Bytes.toBytes("TRANSACTION_ID");
        private static final byte[] COLUMN_CATEGORY_ID1 = Bytes.toBytes("CATEGORY_ID1");
        private static final byte[] COLUMN_CATEGORY_ID2 = Bytes.toBytes("CATEGORY_ID2");
        private static final byte[] COLUMN_CATEGORY_NAME1 = Bytes.toBytes("CATEGORY_NAME1");
        private static final byte[] COLUMN_CATEGORY_NAME2 = Bytes.toBytes("CATEGORY_NAME2");
        private static final byte[] COLUMN_COST = Bytes.toBytes("COST");
        private static final byte[] COLUMN_PRICE = Bytes.toBytes("PRICE");
        private static final byte[] COLUMN_QUANTITY = Bytes.toBytes("QUANTITY");

        public void map(LongWritable lineNum, Text posLine, Context context) throws IOException, InterruptedException {
            // Get the file name that is being processed now
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName().toString();

            String[] csvValues = CSVUtils.parseLine(posLine.toString());
            PosRow posRow = new PosRow();
            posRow.transactionId = Long.parseLong(csvValues[0]);
            posRow.categoryId1 = Integer.parseInt(csvValues[1]);
            posRow.categoryId2 = Integer.parseInt(csvValues[2]);
            posRow.categoryName1 = csvValues[3];
            posRow.categoryName2 = csvValues[4];
            posRow.cost = Float.parseFloat(csvValues[5]);
            posRow.price = Float.parseFloat(csvValues[6]);
            posRow.quantity = Integer.parseInt(csvValues[7]);

            String keyString = filename + "_" + posRow.transactionId;
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(keyString));

            context.write(rowKey, resultToPut(rowKey, posRow));
        }

        private static Put resultToPut(ImmutableBytesWritable key, PosRow posRow) throws IOException {
            Put put = new Put(key.get());
            put.add(FAMILY_BYTES, COLUMN_TRANSACTION_ID, Bytes.toBytes(posRow.transactionId));
            put.add(FAMILY_BYTES, COLUMN_CATEGORY_ID1, Bytes.toBytes(posRow.categoryId1));
            put.add(FAMILY_BYTES, COLUMN_CATEGORY_ID2, Bytes.toBytes(posRow.categoryId2));

            put.add(FAMILY_BYTES, COLUMN_CATEGORY_NAME1, Bytes.toBytes(posRow.categoryName1));
            put.add(FAMILY_BYTES, COLUMN_CATEGORY_NAME2, Bytes.toBytes(posRow.categoryName2));

            put.add(FAMILY_BYTES, COLUMN_COST, Bytes.toBytes(posRow.cost));
            put.add(FAMILY_BYTES, COLUMN_PRICE, Bytes.toBytes(posRow.price));
            put.add(FAMILY_BYTES, COLUMN_QUANTITY, Bytes.toBytes(posRow.quantity));
            return put;
        }
    }

    public static void main(String[] args) throws Exception {
        INPUT_PATH = INPUT_PATH + args[0] + Path.SEPARATOR;
        OUTPUT_PATH = INPUT_PATH + "hfileOutput" + Path.SEPARATOR;

        ConfigurationManager cm = new ConfigurationManager("hadoop", "abcd1234");
        Configuration conf = HBaseConfiguration.create(cm.getConfiguration());
        System.out.println("About to truncate table: " + TABLE_NAME);
        //truncateHbaseTable(conf);

        Job job = new Job(conf, "Import from file " + INPUT_PATH + " into table " + TABLE_NAME);
        //Configuration hBaseConfiguration = HBaseConfiguration.create(conf);
        // delete the hfile output folder
        System.out.println("About to delete: " + OUTPUT_PATH);
        deleteHdfsFolder(conf, OUTPUT_PATH);

        job.setJarByClass(HbaseMrBulkDataBenchmarker.class);
        job.setMapperClass(HbaseMapper.class);

        // The mapper outputs
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setInputFormatClass(TextInputFormat.class);

        HTable hTable = new HTable(conf, TABLE_NAME);

        // Auto configure partitioner and reducer
        HFileOutputFormat.configureIncrementalLoad(job, hTable);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileInputFormat.setInputPathFilter(job, CtlPathFilter.class);
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        //job.setNumReduceTasks(20);
        long start = System.currentTimeMillis();
        job.waitForCompletion(true);
        // Load generated HFiles into table
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path(OUTPUT_PATH), hTable);
        long end = System.currentTimeMillis();
        System.out.println(INPUT_PATH + " took " + (end - start) + " ms...");

    }

    private static boolean deleteHdfsFolder(Configuration conf, String folderName) throws IOException {
        Path hfileOutPath = new Path(folderName);
        FileSystem fileSystem = hfileOutPath.getFileSystem(conf);
        if (fileSystem.exists(hfileOutPath)) {
            return fileSystem.delete(hfileOutPath, true);
        }
        return true;
    }

    static class CtlPathFilter implements PathFilter {
        public boolean accept(Path path) {
            return !path.getName().endsWith(".ctl") && !path.getName().contains("hfileOutput");
        }
    }

    private static void truncateHbaseTable(Configuration conf) throws Exception {
        // Get a descriptor of the table first
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TABLE_NAME);

        List<String> columnFamilies = new ArrayList<String>();
        columnFamilies.add("cf");

        // Add a column family of wanted columns to the descriptor
        for (String columnFamilie : columnFamilies) {
            HColumnDescriptor colDesc = new HColumnDescriptor(Bytes.toBytes(columnFamilie));
            hTableDescriptor.addFamily(colDesc);
        }
        ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "abcd1234");
        Configuration hbaseConfiguration = HBaseConfiguration.create(conf);
        HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConfiguration);

        if (hbaseAdmin.tableExists(TABLE_NAME)) {
            hbaseAdmin.disableTable(TABLE_NAME);
            hbaseAdmin.deleteTable(TABLE_NAME);
        }
        hbaseAdmin.createTable(hTableDescriptor);
    }
}
