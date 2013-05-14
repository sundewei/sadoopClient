package com.sap.mains;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 6/28/12
 * Time: 3:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class SqoopDbLoader {
    private static final String PROCES_NAME = "sqoopJavaLoader";
    private SigarInfoManager sigarInfoManager = new SigarInfoManager();

    private static String TABLE;
    private static String DB_DRIVER;
    private static String CONNECTION_STRING;
    private static String DB_USER;
    private static String DB_PASSWORD;
    private static String CSV_HDFS_PATH;

    public static Connection getConnection() throws Exception {
        Class.forName(DB_DRIVER).newInstance();
        return DriverManager.getConnection(CONNECTION_STRING, DB_USER, DB_PASSWORD);
    }

    private static void truncateTable(Connection connection) throws Exception {
        String query = "truncate table " + TABLE;
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    public static void main(String[] arg) throws Exception {
        SqoopDbLoader sqoopDbLoader = new SqoopDbLoader();
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

        int numOfMapper = Integer.parseInt(properties.getProperty("numOfMapper"));

        DB_DRIVER = properties.getProperty("dbDriver");
        CONNECTION_STRING = properties.getProperty("connectionString");
        TABLE = properties.getProperty("table");
        DB_USER = properties.getProperty("dbUser");
        DB_PASSWORD = properties.getProperty("dbPassword");
        CSV_HDFS_PATH = properties.getProperty("csvHdfsPath");

        String rowsPerStmt = properties.getProperty("rowsPerStmt");
        String stmtsPerTx = properties.getProperty("stmtsPerTx");

        System.out.println("About to truncate table: " + TABLE);
        Connection connection = getConnection();
        truncateTable(connection);
        connection.close();
        System.out.println("Done truncating table: " + TABLE);

        List<String> arguments = new ArrayList<String>();
        arguments.add("sqoop");
        arguments.add("export");

        if (rowsPerStmt != null) {
            arguments.add("-D sqoop.export.records.per.statement=" + rowsPerStmt);
        }

        if (stmtsPerTx != null) {
            arguments.add("-D sqoop.export.statements.per.transaction=" + stmtsPerTx);
        }

        arguments.add("--connect");
        arguments.add(CONNECTION_STRING);
        arguments.add("--driver");
        arguments.add(DB_DRIVER);
        arguments.add("--table");
        arguments.add(TABLE);
        arguments.add("-username");
        arguments.add(DB_USER);
        arguments.add("-password");
        arguments.add(DB_PASSWORD);
        arguments.add("--export-dir");
        arguments.add(CSV_HDFS_PATH);
        arguments.add("--batch");

        ProcessBuilder processBuilder = new ProcessBuilder();
        System.out.println("Command: \n " + getCommand(arguments));
        MainUtility.BenchmarkResult result = new MainUtility.BenchmarkResult();
        result.batchId = batchId;
        result.hadoopPlatform = hadoopPlatform;
        result.executionEnvironment = executionEnvironment;
        result.loadingMethod = PROCES_NAME;
        result.setExecutionId();
        sqoopDbLoader.sigarInfoManager.setExecutionId(result.executionId);
        sqoopDbLoader.sigarInfoManager.setStat(enableStat);
        sqoopDbLoader.sigarInfoManager.setDetailedStat(enableDetailedStat);
        sqoopDbLoader.sigarInfoManager.connection = connection;
        sqoopDbLoader.sigarInfoManager.setSleepMs(statFreqMs);

        sqoopDbLoader.sigarInfoManager.start();
        long startMs = System.currentTimeMillis();
        Process process = processBuilder.command(arguments).start();
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
        sqoopDbLoader.sigarInfoManager.stopNow = true;

        connection = getConnection();

        result.hadoopNumOfMapper = numOfMapper;

        result.executionTimeMs = (endMs - startMs);
        result.startTimeMs = startMs;
        result.endTimeMs = endMs;
        result.numOfRows = MainUtility.getRowCount(connection, TABLE);
        result.rowsPerStmt = Integer.parseInt(rowsPerStmt);
        result.stmtsPerTx = Integer.parseInt(stmtsPerTx);
        result.commitAfter = result.rowsPerStmt * result.stmtsPerTx;
        MainUtility.insertBenchmarkResult(connection, result);
        connection.close();

        System.out.println("Loading " + CSV_HDFS_PATH + " to " + TABLE + " took " + (endMs - startMs) + " milliseconds");
    }

    private static String getCommand(List<String> args) {
        StringBuilder sb = new StringBuilder();
        for (String arg : args) {
            sb.append(arg).append(" ");
        }
        return sb.toString();
    }
}
