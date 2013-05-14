package com.sap.mains;

import com.sap.hadoop.conf.ConfigurationManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 7/31/12
 * Time: 11:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class MainUtility {
    public static class BenchmarkResult {
        public String batchId;
        public String hadoopPlatform;
        public String executionEnvironment;
        public String loadingMethod;
        public long numOfRows;
        public int commitAfter = -1;
        public int hanaThreadCount = 1;
        public long executionTimeMs;
        public int hadoopNumOfReducer = -1;
        public int hadoopNumOfMapper = -1;
        public long startTimeMs;
        public long endTimeMs;
        public int rowsPerStmt = -1;
        public int stmtsPerTx = -1;
        public String executionId;

        @Override
        public String toString() {
            return "BenchmarkResult{" +
                    "batchId='" + batchId + '\'' +
                    ", hadoopPlatform='" + hadoopPlatform + '\'' +
                    ", executionEnvironment='" + executionEnvironment + '\'' +
                    ", loadingMethod='" + loadingMethod + '\'' +
                    ", numOfRows=" + numOfRows +
                    ", commitAfter=" + commitAfter +
                    ", hanaThreadCount=" + hanaThreadCount +
                    ", executionTimeMs=" + executionTimeMs +
                    ", hadoopNumOfReducer=" + hadoopNumOfReducer +
                    ", hadoopNumOfMapper=" + hadoopNumOfMapper +
                    ", startTimeMs=" + startTimeMs +
                    ", endTimeMs=" + endTimeMs +
                    ", rowsPerStmt=" + rowsPerStmt +
                    ", stmtsPerTx=" + stmtsPerTx +
                    ", executionId=" + executionId +
                    '}';
        }

        public void setExecutionId() {
            if (executionId == null) {
                executionId = batchId + "|"
                        + hadoopPlatform + "|"
                        + executionEnvironment + "|"
                        + loadingMethod + "|"
                        + System.currentTimeMillis();
            }
        }
    }


    public static int getRowCount(Connection conn, String tableName) throws SQLException {
        String sql = " SELECT count(0) from " + tableName;
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        ResultSet rs = preparedStatement.executeQuery();
        int rowCount = 0;
        while (rs.next()) {
            rowCount = rs.getInt(1);
        }
        rs.close();
        preparedStatement.close();
        return rowCount;
    }

    public static void insertBenchmarkResult(Connection conn, BenchmarkResult result) throws SQLException {
        String sql = " INSERT INTO HADOOP.BENCHMARK (BATCH_ID,HADOOP_PLATFORM,EXECUTION_ENVIRONMENT,LOADING_METHOD,NUM_OF_ROWS,COMMIT_AFTER,HANA_THREAD_NUM,EXECUTION_TIME_MS,HADOOP_REDUCE_JOBS_NUM,HADOOP_MAP_JOBS_NUM,START_TIME,END_TIME,ROWS_PER_STMT,STMTS_PER_TX,EXECUTION_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.setString(1, result.batchId);
        preparedStatement.setString(2, result.hadoopPlatform);
        preparedStatement.setString(3, result.executionEnvironment);
        preparedStatement.setString(4, result.loadingMethod);

        preparedStatement.setLong(5, result.numOfRows);
        preparedStatement.setInt(6, result.commitAfter);
        preparedStatement.setInt(7, result.hanaThreadCount);
        preparedStatement.setLong(8, result.executionTimeMs);

        preparedStatement.setInt(9, result.hadoopNumOfReducer);
        preparedStatement.setInt(10, result.hadoopNumOfMapper);
        preparedStatement.setTimestamp(11, new Timestamp(result.startTimeMs));
        preparedStatement.setTimestamp(12, new Timestamp(result.endTimeMs));
        preparedStatement.setInt(13, result.rowsPerStmt);
        preparedStatement.setInt(14, result.stmtsPerTx);
        preparedStatement.setString(15, result.executionId);
        preparedStatement.execute();
        preparedStatement.close();
    }

    public static void main5(String[] arg) throws Exception {
        DefaultHttpClient client = new DefaultHttpClient();
        // http://llbpal40:50111/templeton/v1/hive?execute=%22select+*+from+pos_rows;%22&statusdir=%22/user/oozie/work/%22&user.name=hduser
        HttpPost httpPost = new HttpPost("http://llbpal40:50111/templeton/v1/hive");
        ArrayList<NameValuePair> postParameters;
        postParameters = new ArrayList<NameValuePair>();
        postParameters.add(new BasicNameValuePair("execute", "add jar /user/oozie/work/loaderWithSqoop/lib/sap_hadoop_example.jar; create temporary function comma_delimited as 'com.sap.hive.udf.CommaDelimited'; select comma_delimited(transaction_id, category1_name, category2_name) from pos_rows limit 1000; "));
        postParameters.add(new BasicNameValuePair("statusdir", "/user/oozie/work/"));
        postParameters.add(new BasicNameValuePair("user.name", "hduser"));

        httpPost.setEntity(new UrlEncodedFormEntity(postParameters));
        HttpResponse response = client.execute(httpPost);
        HttpEntity entity = response.getEntity();
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        System.out.println(writer.toString());
    }

    public static void main6(String[] arg) throws Exception {
        DefaultHttpClient client = new DefaultHttpClient();
        //String url = "http://llbpal40.pal.sap.corp:8080/oozie/v1/jobs?action=start";
        String url = "http://llbpal40.pal.sap.corp:8080/oozie/v1/jobs";
        HttpPost httpPost = new HttpPost(url);
        httpPost.addHeader("Content-Type", "application/xml;charset=UTF-8");
        httpPost.setEntity(new ByteArrayEntity(FileUtils.readFileToByteArray(new File("C:\\projects\\oozieWork\\llbpal40\\loaderWithSqoop\\rest.xml")),
                ContentType.APPLICATION_XML));

        HttpResponse response = client.execute(httpPost);
        HttpEntity entity = response.getEntity();
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        System.out.println(writer.toString());
    }


    public static void main7(String[] arg) throws Exception {
        DefaultHttpClient client = new DefaultHttpClient();
        String url = "http://llbpal40.pal.sap.corp:8080/oozie/v1/job/0000004-130503104107955-oozie-oozi-W?action=start";
        HttpPut httpPut = new HttpPut(url);
        HttpResponse response = client.execute(httpPut);
        HttpEntity entity = response.getEntity();
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity.getContent(), writer);
        System.out.println(writer.toString());
    }


    public static void main(String[] arg) throws Exception {
        main7(arg);
    }

    public static void main4(String[] arg) throws Exception {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:mysql://llbpal40/metastore","hiveuser", "password");
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(" select * from COLUMNS_V2 ");
        while(rs.next()) {
            System.out.println(rs.getString(1) + ", " + rs.getString(2) + ", " + rs.getString(3)+ ", " + rs.getString(4)+ ", " + rs.getString(5));
        }
        stmt.close();
        rs.close();
        con.close();
    }

    public static void main3(String[] arg) throws Exception {
        try {
            Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive://llbpal40:10000/default","", "");
        Statement stmt = con.createStatement();
        //stmt.executeQuery(" INSERT OVERWRITE DIRECTORY 'hdfs://llbpal40:8020/user/oozie/work/loaderWithPig/hiveOut' select transaction_id, category1_name, category2_id, cost, price, article_id, name, xml FROM pos_rows join sections on (pos_rows.transaction_id = sections.article_id) where transaction_id < 30000000 and transaction_id > 50000  ");
        ResultSet rs = stmt.executeQuery(" explain select transaction_id, category1_name, category2_id, cost, price, article_id, name, xml FROM pos_rows join sections on (pos_rows.transaction_id = sections.article_id) where transaction_id < 30000000 and transaction_id > 50000  ");

        System.out.println("rs.getMetaData().getColumnCount()=" + rs.getMetaData().getColumnCount());
        while(rs.next()) {
            System.out.println(rs.getString(1));
        }

        stmt.close();
        rs.close();
        con.close();
    }




    public static void main2(String[] arg) throws Exception {

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(arg[0])));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(arg[1])));
        String line = reader.readLine();
        // "905390","7088","268524","GOLF","Clubs","6.15","8.88","1",
        while (line != null) {
            writer.write(removeQuotes(line));
            line = reader.readLine();
        }
        reader.close();
        writer.close();

        //System.out.println(removeQuotes("\"905390\",\"7088\",\"268524\",\"GOLF\",\"Clubs\",\"6.15\",\"8.88\",\"1\","));
    }

    public static String removeQuotes(String line) {
        StringBuilder sb = new StringBuilder();
        int quoteCount = 0;
        char[] lineCharArr = line.toCharArray();
        for (int i = 0; i < lineCharArr.length; i++) {
            if (lineCharArr[i] == '\"') {
                quoteCount++;
                if (quoteCount > 6 && quoteCount <= 10) {
                    sb.append(lineCharArr[i]);
                }
            } else {
                sb.append(lineCharArr[i]);
            }
        }
        if (sb.charAt(sb.length() - 1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("\n");
        return sb.toString();
    }

    public static void main1(String[] arg) throws Exception {
        // Get a descriptor of the table first
        HTableDescriptor hTableDescriptor = new HTableDescriptor("pos_rows");

        List<String> columnFamilies = new ArrayList<String>();
        columnFamilies.add("cf");


        // Add a column family of wanted columns to the descriptor
        for (String columnFamilie : columnFamilies) {
            HColumnDescriptor colDesc = new HColumnDescriptor(Bytes.toBytes(columnFamilie));
            hTableDescriptor.addFamily(colDesc);
        }
        ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "abcd1234");
        Configuration hbaseConfiguration = HBaseConfiguration.create(configurationManager.getConfiguration());
        HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConfiguration);
        hbaseAdmin.createTable(hTableDescriptor);
    }

    public static void generateCtlFiles(String[] arg) throws Exception {
        String ctlTemplate = "IMPORT DATA\n" +
                "INTO TABLE SYSTEM.POS_ROWS_SS\n" +
                "FROM '/usr/local/mountedHdfs/data/posData/%s' \n" +
                "ERROR LOG '/usr/local/mountedHdfs/data/posData/%s.err'";
        ConfigurationManager cm = new ConfigurationManager("hadoop", "abcd1234");
        Path dataPath = new Path("/data/posData");
        FileSystem fileSystem = dataPath.getFileSystem(cm.getConfiguration());
        FileStatus[] fss = fileSystem.listStatus(dataPath);
        for (FileStatus fs : fss) {
            String ctlContent = String.format(ctlTemplate, fs.getPath().getName(), fs.getPath().getName());
            String ctlFilename = fs.getPath().toString() + ".ctl";
            fileSystem.delete(new Path(ctlFilename), true);
            OutputStream out = fileSystem.create(new Path(fs.getPath().toString() + ".ctl"));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
            writer.write(ctlContent);
            writer.flush();
            out.close();
            writer.close();
        }
        fileSystem.close();
    }

    public static String[] getArray(String prefix, int size) {
        String[] arr = new String[size];
        for (int i = 0; i < size; i++) {
            arr[i] = prefix + getPaddedString(String.valueOf((int) (Math.random() * 1000)), 5);
        }
        return arr;
    }

    public static String getPaddedString(String num, int length) {
        if (num.length() > length) {
            return num.substring(0, length);
        } else {
            while (num.length() < length) {
                num = "0" + num;
            }
            return num;
        }
    }


}
