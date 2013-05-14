package com.sap.mains;

import com.sap.hadoop.conf.ConfigurationManager;
import org.apache.commons.csv.CSVUtils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 7/2/12
 * Time: 11:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class CsvDbLoader {
    private static final String PROCES_NAME = "standaloneJavaLoader";

    private String csvHdfsPath;
    private String csvLocalPath;
    private String csvPath;
    private boolean isHdfs;
    private String hdfsUser;
    private String hdfsPassword;
    private Connection connection;
    private String table;
    private int commitPerRow = 1000;
    private List<Class> columnTypes = new ArrayList<Class>();
    private ConfigurationManager configurationManager;
    private long rowCount = 0;
    private SigarInfoManager sigarInfoManager = new SigarInfoManager();

    private InputStream getInputStream() throws Exception {
        if (isHdfs) {
            configurationManager = new ConfigurationManager(hdfsUser, hdfsPassword);
            return configurationManager.getFileSystem().getInputStream(csvPath);
        } else {
            return new FileInputStream(csvLocalPath);
        }
    }

    public CsvDbLoader(String uname, String pwd) {
        this.hdfsUser = uname;
        this.hdfsPassword = pwd;
    }

    public void setCommitPerRow(int commitPerRow) {
        this.commitPerRow = commitPerRow;
    }

    public void setCsvHdfsPath(String csvHdfsPath) {
        this.csvHdfsPath = csvHdfsPath;
        if (this.csvHdfsPath != null && !this.csvHdfsPath.equals("")) {
            this.csvPath = csvHdfsPath;
            this.isHdfs = true;
        }
    }

    public void setCsvLocalPath(String csvLocalPath) {
        this.csvLocalPath = csvLocalPath;
        if (this.csvLocalPath != null && !this.csvLocalPath.equals("")) {
            csvPath = csvLocalPath;
            isHdfs = false;
        }
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void setTable(String table) {
        this.table = table;
    }

    private static void setValue(PreparedStatement preparedStatement, String value, Class ccc, int index) throws SQLException {
        if (ccc == Integer.class) {
            preparedStatement.setInt(index, Integer.parseInt(value));
        } else if (ccc == String.class) {
            preparedStatement.setString(index, value);
        }
    }

    private void truncateTable() throws SQLException {
        String query = " truncate table " + table;
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    public void loadTable() throws Exception {
        String query = getQuery();
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        InputStream csvInputStream = getInputStream();
//System.out.println("csvInputStream="+csvInputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(csvInputStream));
        String line = reader.readLine();
        int batchCount = 0;
        while (line != null) {
            String[] values = CSVUtils.parseLine(line);
            int index = 0;
//System.out.println(line);
            for (Class ccc : columnTypes) {
                setValue(preparedStatement, values[index], ccc, index + 1);
                index++;
            }
            preparedStatement.addBatch();
            rowCount++;
            batchCount++;
            if (batchCount % commitPerRow == 0) {
                batchCount = 0;
                preparedStatement.executeBatch();
//System.out.println("rowCount = " + rowCount);
            }
            line = reader.readLine();
        }
        reader.close();
        csvInputStream.close();

        if (batchCount > 0) {
            preparedStatement.executeBatch();
//System.out.println("Last batch, rowCount = " + rowCount);
        }
        preparedStatement.close();
    }

    private String getQuery() {
        String queryStart = " insert into " + table + " values ( ";
        String queryEnd = " ) ";
        StringBuilder sb = new StringBuilder();
        sb.append(queryStart);

        for (int i = 0; i < columnTypes.size(); i++) {
            sb.append("?");
            if (i != columnTypes.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(queryEnd);
        return sb.toString();
    }

    public static void main(String[] arg) throws Exception {
        Properties properties = new Properties();
        if (arg.length > 0) {
            properties.load(new FileInputStream(arg[0]));
        }

        String batchId = properties.getProperty("batchId");
        String hadoopPlatform = properties.getProperty("hadoopPlatform");
        String executionEnvironment = properties.getProperty("executionEnvironment");
        boolean enableStat = Boolean.parseBoolean(properties.getProperty("enableStat", "false"));
        boolean enableDetailedStat = Boolean.parseBoolean(properties.getProperty("enableDetailedStat", "false"));

        System.out.println("enableStat=" + enableStat);
        System.out.println("enableDetailedStat=" + enableDetailedStat);

        long statFreqMs = Long.parseLong(properties.getProperty("statFreqMs", "20000"));

        int numOfMapper = Integer.parseInt(properties.getProperty("numOfMapper"));

        String dbDriver = properties.getProperty("dbDriver");
        Class.forName(dbDriver).newInstance();

        String url = properties.getProperty("connectionString");
        String dbUser = properties.getProperty("dbUser");
        String dbPassword = properties.getProperty("dbPassword");
        System.out.println("Trying to get a JDBC connection from " + url);
        Connection connection = DriverManager.getConnection(url, dbUser, dbPassword);

        String hdfsUser = properties.getProperty("hdfsUser");
        String hdfsPassword = properties.getProperty("hdfsPassword");

        CsvDbLoader csvDbLoader = new CsvDbLoader(hdfsUser, hdfsPassword);

        csvDbLoader.setConnection(connection);

        csvDbLoader.columnTypes.add(String.class);
        csvDbLoader.columnTypes.add(String.class);
        csvDbLoader.columnTypes.add(String.class);
        csvDbLoader.columnTypes.add(Integer.class);
        csvDbLoader.columnTypes.add(String.class);

        int commitPerRow = Integer.parseInt(properties.getProperty("commitPerRow"));
        csvDbLoader.setCommitPerRow(commitPerRow);

        String table = properties.getProperty("table");
        csvDbLoader.setTable(table);

        String csvHdfsPath = properties.getProperty("csvHdfsPath");
        if (csvHdfsPath != null) {
            csvDbLoader.setCsvHdfsPath(csvHdfsPath);
        }

        String csvLocalPath = properties.getProperty("csvLocalPath");
        if (csvLocalPath != null) {
            csvDbLoader.setCsvLocalPath(csvLocalPath);
        }

        System.out.println("About to truncate table: " + table);
        csvDbLoader.truncateTable();
        System.out.println("Done truncating table: " + table);

        System.out.println("Start to load " + table + " from " + csvDbLoader.csvPath);
        MainUtility.BenchmarkResult result = new MainUtility.BenchmarkResult();
        result.batchId = batchId;
        result.hadoopPlatform = hadoopPlatform;
        result.executionEnvironment = executionEnvironment;
        result.loadingMethod = PROCES_NAME + (csvDbLoader.isHdfs ? "_Hdfs" : "_Local");
        result.setExecutionId();
        csvDbLoader.sigarInfoManager.setExecutionId(result.executionId);
        csvDbLoader.sigarInfoManager.setStat(enableStat);
        csvDbLoader.sigarInfoManager.setDetailedStat(enableDetailedStat);
        csvDbLoader.sigarInfoManager.connection = connection;
        csvDbLoader.sigarInfoManager.setSleepMs(statFreqMs);

        csvDbLoader.sigarInfoManager.start();
        long startMs = System.currentTimeMillis();
        csvDbLoader.loadTable();
        long endMs = System.currentTimeMillis();
        csvDbLoader.sigarInfoManager.stopNow = true;

        System.out.println("Loading " + csvDbLoader.rowCount + " rows took " + (endMs - startMs) + " milliseconds");
        if (csvDbLoader.configurationManager != null && csvDbLoader.configurationManager.getFileSystem() != null) {
            csvDbLoader.configurationManager.getFileSystem().close();
        }

        result.hadoopNumOfMapper = numOfMapper;

        result.executionTimeMs = (endMs - startMs);
        result.startTimeMs = startMs;
        result.endTimeMs = endMs;
        result.commitAfter = commitPerRow;
        result.numOfRows = MainUtility.getRowCount(connection, table);

        MainUtility.insertBenchmarkResult(connection, result);
        connection.close();
        System.exit(0);

    }
}
