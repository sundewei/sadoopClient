package com.sap.mains;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 7/11/12
 * Time: 11:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class SqlScriptCsvLoader {
    private static final String PROCES_NAME = "sqlScriptJavaLoader";
    private SigarInfoManager sigarInfoManager = new SigarInfoManager();

    private String csvLocalPath;
    private String csvPath;
    private String localFolderPath;
    private Connection connection;
    private String table;
    private int threadCount = 1;
    private int commitPerRow = 1;
    private boolean remoteCtlFile = false;

    public SqlScriptCsvLoader() {
    }

    public void setCsvLocalPath(String csvLocalPath) {
        this.csvLocalPath = csvLocalPath;
        if (this.csvLocalPath != null && !this.csvLocalPath.equals("")) {
            csvPath = csvLocalPath;
        }
        File csvFile = new File(csvPath);
        localFolderPath = csvFile.getParentFile().getAbsolutePath();
    }

    public void setLocalFolderPath(String localFolderPath) {
        this.localFolderPath = localFolderPath;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    private void deleteTable() throws SQLException {
        String query = " truncate table " + table;
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    public void loadTable() throws Exception {
        String query = getQuery();
        System.out.println("Query = \n " + query);
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.execute();
        preparedStatement.close();
    }

    private String getQuery() throws IOException {
        StringBuilder sb = new StringBuilder();
        String filename = localFolderPath + File.separator + table + ".CTL";
        if (remoteCtlFile) {
            String errorFilename = localFolderPath + File.separator + "import.err";
            filename = remoteGenerateControlFile(filename, table, csvLocalPath, errorFilename);
        } else {
            filename = generateControlFile();
        }
        sb.append("IMPORT FROM '").append(filename).append("'\n");
        sb.append("WITH THREADS ").append(threadCount).append("  ");
        if (commitPerRow > 0) {
            sb.append("BATCH ").append(commitPerRow);
        }
        return sb.toString();
    }

    private String generateControlFile() throws IOException {
        String filename = localFolderPath + File.separator + table + ".CTL";
        String errorFilename = localFolderPath + File.separator + "import.err";
        StringBuilder sb = new StringBuilder();
        sb.append("IMPORT DATA\n");
        sb.append("INTO TABLE ").append(table).append("\n");
        sb.append("FROM '").append(csvLocalPath).append("' \n");
        sb.append("ERROR LOG '").append(errorFilename).append("'");
        File controlFile = new File(filename);
        if (controlFile.exists()) {
            System.out.println("About to delete " + filename + ", because it exists...");
            controlFile.delete();
        }
        System.out.println("Now about to write new content to " + filename + "...");
        FileUtils.write(controlFile, sb.toString());
        return filename;
    }

    private String remoteGenerateControlFile(String controlFilename, String table, String csvFilename, String errorFilename)
            throws IOException {
        String url = "http://lspal134:8080/FileUtilApp/w?";
        url += "controlFilename=" + controlFilename;
        url += "&table=" + table;
        url += "&csvFilename=" + csvFilename;
        url += "&errorFilename=" + errorFilename;
        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpGet httpget = new HttpGet(url);
        HttpResponse response = httpclient.execute(httpget);
        HttpEntity entity = response.getEntity();
        List<String> lines = IOUtils.readLines(entity.getContent());
        return lines.get(0);
    }

    public static void main1(String[] arg) throws Exception {
        SqlScriptCsvLoader loader = new SqlScriptCsvLoader();
        String text =
                loader.remoteGenerateControlFile(
                        "/usr/sap/JH1/home/benchmark/sqlScriptJava/temp/SYSTEM.EVENT_LOGS.CTL",
                        "SYSTEM.EVENT_LOGS",
                        "/usr/local/mountedHdfs/data/eventLog1K.csv",
                        "/usr/sap/JH1/home/benchmark/sqlScriptJava/temp/import.err"
                );
        System.out.println("text=" + text);
    }

    public static void main(String[] arg) throws Exception {
        Properties properties = new Properties();
        if (arg.length > 0) {
            properties.load(new FileInputStream(arg[0]));
        }

        String batchId = properties.getProperty("batchId");
        String hadoopPlatform = properties.getProperty("hadoopPlatform");
        String executionEnvironment = properties.getProperty("executionEnvironment");
        int numOfMapper = Integer.parseInt(properties.getProperty("numOfMapper"));
        boolean enableStat = Boolean.parseBoolean(properties.getProperty("enableStat", "false"));
        boolean enableDetailedStat = Boolean.parseBoolean(properties.getProperty("enableDetailedStat", "false"));

        String dbDriver = properties.getProperty("dbDriver");
        Class.forName(dbDriver).newInstance();

        String url = properties.getProperty("connectionString");
        String dbUser = properties.getProperty("dbUser");
        String dbPassword = properties.getProperty("dbPassword");
        System.out.println("Trying to get a JDBC connection from " + url);

        Connection connection = DriverManager.getConnection(url, dbUser, dbPassword);

        SqlScriptCsvLoader sqlScriptCsvLoader = new SqlScriptCsvLoader();
        sqlScriptCsvLoader.remoteCtlFile = Boolean.parseBoolean(properties.getProperty("enableRemoteCtlFileGeneration", "false"));
        sqlScriptCsvLoader.commitPerRow = Integer.parseInt(properties.getProperty("commitPerRow", "20000"));
        long statFreqMs = Long.parseLong(properties.getProperty("statFreqMs", "20000"));

        sqlScriptCsvLoader.setConnection(connection);

        String table = properties.getProperty("table");
        sqlScriptCsvLoader.setTable(table);

        String csvLocalPath = properties.getProperty("csvLocalPath");
        sqlScriptCsvLoader.setCsvLocalPath(csvLocalPath);
        String localFolderPath = properties.getProperty("localFolderPath");
        if (localFolderPath != null) {
            sqlScriptCsvLoader.setLocalFolderPath(localFolderPath);
        }

        int threadCount = Integer.parseInt(properties.getProperty("threadCount"));
        sqlScriptCsvLoader.setThreadCount(threadCount);
        System.out.println("About to truncate table: " + table + "...");
        sqlScriptCsvLoader.deleteTable();
        System.out.println("Start to load " + table + " from " + sqlScriptCsvLoader.csvPath);

        MainUtility.BenchmarkResult result = new MainUtility.BenchmarkResult();
        result.batchId = batchId;
        result.hadoopPlatform = hadoopPlatform;
        result.executionEnvironment = executionEnvironment;
        result.loadingMethod = PROCES_NAME;
        result.setExecutionId();
        sqlScriptCsvLoader.sigarInfoManager.setExecutionId(result.executionId);
        sqlScriptCsvLoader.sigarInfoManager.setStat(enableStat);
        sqlScriptCsvLoader.sigarInfoManager.setDetailedStat(enableDetailedStat);
        sqlScriptCsvLoader.sigarInfoManager.setSleepMs(statFreqMs);

        sqlScriptCsvLoader.sigarInfoManager.connection = connection;

        sqlScriptCsvLoader.sigarInfoManager.start();
        long startMs = System.currentTimeMillis();
        sqlScriptCsvLoader.loadTable();
        long endMs = System.currentTimeMillis();
        System.out.println("Loading took " + (endMs - startMs) + " milliseconds");

        sqlScriptCsvLoader.sigarInfoManager.stopNow = true;
        result.hadoopNumOfMapper = numOfMapper;

        result.executionTimeMs = (endMs - startMs);
        result.commitAfter = sqlScriptCsvLoader.commitPerRow;
        result.startTimeMs = startMs;
        result.endTimeMs = endMs;
        result.numOfRows = MainUtility.getRowCount(connection, table);
        result.hanaThreadCount = threadCount;
        MainUtility.insertBenchmarkResult(connection, result);
        connection.close();
        System.exit(0);
    }
}
