package com.sap.mains;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 8/9/12
 * Time: 3:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class PigDbLoaderHelper {

    private static final String PROCES_NAME = "pigLatinDbLoader";
    private SigarInfoManager sigarInfoManager = new SigarInfoManager();

    private static String getPigControlFileContent(String filePath, String table, int commitPerRow) {
        StringBuilder sb = new StringBuilder();
        sb.append("register '/usr/local/pig/ngdbc.jar';\n");
        sb.append("register '/usr/local/pig/piggybank.jar';\n");
        sb.append("CSV_CONTENT = LOAD '").append(filePath).append("' USING PigStorage(',') AS (ONEK_HDFS::DATE_STRING:chararray,ONEK_HDFS::USER_ID:chararray,ONEK_HDFS::OTHER_ID:chararray,ONEK_HDFS::EVENT_TYPE:int,ONEK_HDFS::COMMENTS:chararray);\n");
        sb.append("STORE CSV_CONTENT INTO '/tmp/dbLoad' using org.apache.pig.piggybank.storage.DBStorage('com.sap.db.jdbc.Driver', 'jdbc:sap://10.48.144.94:31015?reconnect=true', 'SYSTEM', 'Hana1234', 'insert into ");
        sb.append(table);
        sb.append(" values (?,?,?,?,?)', '");
        sb.append(commitPerRow);
        sb.append("');\n");
        return sb.toString();
    }

    public static void writePigControlFile(String filename, String filePath, String table, int commitPerRow) throws IOException {
        String content = getPigControlFileContent(filePath, table, commitPerRow);
        FileUtils.writeStringToFile(new File(filename), content);
    }

    private static void truncateTable(Connection connection, String table) throws SQLException {
        String query = " truncate table " + table;
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    public static void main(String[] arg) throws Exception {
        PigDbLoaderHelper pigDbLoaderHelper = new PigDbLoaderHelper();

        Properties properties = new Properties();
        if (arg.length > 0) {
            properties.load(new FileInputStream(arg[0]));
        }

        String batchId = properties.getProperty("batchId");
        String hadoopPlatform = properties.getProperty("hadoopPlatform");
        String executionEnvironment = properties.getProperty("executionEnvironment");

        boolean enableStat = Boolean.parseBoolean(properties.getProperty("enableStat", "false"));
        boolean enableDetailedStat = Boolean.parseBoolean(properties.getProperty("enableDetailedStat", "false"));
        long statFreqMs = Long.parseLong(properties.getProperty("statFreqMs", "20000"));

        String dbDriver = properties.getProperty("dbDriver");
        Class.forName(dbDriver).newInstance();

        String url = properties.getProperty("connectionString");
        String dbUser = properties.getProperty("dbUser");
        String dbPassword = properties.getProperty("dbPassword");
        System.out.println("Trying to get a JDBC connection from " + url);
        Connection connection = DriverManager.getConnection(url, dbUser, dbPassword);

        int commitPerRow = Integer.parseInt(properties.getProperty("commitPerRow"));

        String table = properties.getProperty("table");

        String csvHdfsPath = properties.getProperty("csvHdfsPath");

        System.out.println("About to truncate table: " + table);
        truncateTable(connection, table);

        writePigControlFile("pig.ctl", csvHdfsPath, table, commitPerRow);

        MainUtility.BenchmarkResult result = new MainUtility.BenchmarkResult();
        result.batchId = batchId;
        result.hadoopPlatform = hadoopPlatform;
        result.executionEnvironment = executionEnvironment;
        result.loadingMethod = PROCES_NAME;
        result.setExecutionId();
        pigDbLoaderHelper.sigarInfoManager.setExecutionId(result.executionId);
        pigDbLoaderHelper.sigarInfoManager.setStat(enableStat);
        pigDbLoaderHelper.sigarInfoManager.setDetailedStat(enableDetailedStat);
        pigDbLoaderHelper.sigarInfoManager.setSleepMs(statFreqMs);

        pigDbLoaderHelper.sigarInfoManager.connection = connection;

        System.out.println("Done truncating table: " + table);

        System.out.println("Start to load " + table + " from HDFS location: " + csvHdfsPath);
        ProcessBuilder processBuilder = new ProcessBuilder();

        pigDbLoaderHelper.sigarInfoManager.start();
        long startMs = System.currentTimeMillis();
        Process process = processBuilder.command(new String[]{"pig", "-f", "pig.ctl"}).start();
        InputStream in = process.getInputStream();
        InputStream err = process.getErrorStream();
        BufferedReader errorReader = new BufferedReader(new InputStreamReader(err));
        String line = null;
        while ((line = errorReader.readLine()) != null) {
            System.out.println("Stderr: " + line);
        }
        BufferedReader outReader = new BufferedReader(new InputStreamReader(in));
        line = null;
        while ((line = outReader.readLine()) != null) {
            System.out.println("Stdout: " + line);
        }
        in.close();
        err.close();
        long endMs = System.currentTimeMillis();

        pigDbLoaderHelper.sigarInfoManager.stopNow = true;

        result.executionTimeMs = (endMs - startMs);
        result.startTimeMs = startMs;
        result.endTimeMs = endMs;
        result.commitAfter = commitPerRow;
        result.numOfRows = MainUtility.getRowCount(connection, table);
//System.out.println(result);
        MainUtility.insertBenchmarkResult(connection, result);
        connection.close();
        System.exit(0);
    }
}
