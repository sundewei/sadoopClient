package com.sap.mains;

import org.hyperic.sigar.*;

import java.io.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 8/13/12
 * Time: 2:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class SigarInfoManager extends Thread {
    public Connection connection;
    private String executionId;
    public boolean stopNow = false;
    public boolean detailedStat = false;
    public boolean stat = false;
    public long sleepMs = 5000;
    public SigarInfo selfSigarInfo = new SigarInfo();
    public int minutesToRun = 1440;
    public List<ProcessInfo> processInfoList = new ArrayList<ProcessInfo>();
    public static DateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    String dbPassword;
    String dbUser;
    String connectionString;
    String dbDriver;

    private static String[] LIST_COMMAND = new String[]{
            "ps",
            "-ef"
    };

    public void setSleepMs(long sleepMs) {
        this.sleepMs = sleepMs;
    }

    public void setDetailedStat(boolean detailedStat) {
        this.detailedStat = detailedStat;
    }

    public void setStat(boolean stat) {
        this.stat = stat;
    }

    public void setMinutesToRun(int minutesToRun) {
        this.minutesToRun = minutesToRun;
    }

    public SigarInfoManager() {
        ProcessBuilder processBuilder = new ProcessBuilder();
        try {
            Process process = processBuilder.command(LIST_COMMAND).start();
            InputStream stdOut = process.getInputStream();
            BufferedReader outReader = new BufferedReader(new InputStreamReader(stdOut));
            String line = null;
            while ((line = outReader.readLine()) != null) {
                if (line.contains("jh1adm") && !line.contains("root") && !line.contains("pts/1") && !line.contains("sshd:")
                        && !line.contains("ps -ef") && !line.contains("-sh")) {
                    while (line.contains("  ")) {
                        line = line.replace("  ", " ");
                    }
                    ProcessInfo processInfo = new ProcessInfo(line);
                    processInfoList.add(processInfo);
                }
            }
            stdOut.close();
            outReader.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }


    }

    public static class ProcessInfo {
        public String user;
        public String pid;
        public String cmd;
        public SigarInfo sigarInfo;

        public ProcessInfo(String line) {
            String[] values = line.split(" ");
            user = values[0];
            pid = values[1];
            cmd = "";
            for (int i = 7; i < values.length; i++) {
                cmd += " " + values[i];
            }
            sigarInfo = new SigarInfo(Long.parseLong(pid));
        }

        @Override
        public String toString() {
            return "ProcessInfo{" +
                    "user='" + user + '\'' +
                    ", pid='" + pid + '\'' +
                    ", cmd='" + cmd + '\'' +
                    '}';
        }
    }

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public void run() {
        long startMs = System.currentTimeMillis();
        long msToRun = minutesToRun * 60 * 1000;
        if (stat) {
            int sequence = 0;
            while (!stopNow) {
                StatResults statResults = new StatResults();
                statResults.millisecond = System.currentTimeMillis();
                statResults.executionId = getExecutionId();
                statResults.sequence = sequence;
                try {
                    Mem mem = selfSigarInfo.sigar.getMem();
                    System.out.println("\n\n");
                    long[] pids = selfSigarInfo.sigar.getProcList();
                    double totalPercentage = 0d;
                    for (long pid : pids) {
                        try {
                            ProcCpu procCpu = selfSigarInfo.sigar.getProcCpu(pid);
                            totalPercentage += procCpu.getPercent() * 100;
                        } catch (SigarException se) {
                            System.out.println("Got an exception on this process: " + pid);
                            se.printStackTrace();

                        }
                    }
                    System.out.println("totalCpuUsedPercentage=" + totalPercentage);

//System.out.println(multiProcCpu.getPercent() + " ---> " + CpuPerc.format(multiProcCpu.getPercent()));
//                    Map<String, String> map = multiProcCpu.toMap();
//
//                    for (Map.Entry<String, String> entry: map.entrySet()) {
//                        System.out.println(entry.getKey() + ": " + entry.getValue());
//                    }


                    statResults.cpuPercentage = totalPercentage;
                    statResults.memActualFreeBytes = mem.getActualFree();
                    statResults.memFreeBytes = mem.getFree();
                    statResults.memActualUsedBytes = mem.getActualUsed();
                    statResults.memUsedBytes = mem.getUsed();
                    NetInterfaceStat eth0 = selfSigarInfo.sigar.getNetInterfaceStat("eth0");
                    NetInterfaceStat lo = selfSigarInfo.sigar.getNetInterfaceStat("lo");
                    statResults.eth0ReceivedBytes = eth0.getRxBytes();
                    statResults.loReceivedBytes = lo.getRxBytes();
                    statResults.eth0TxBytes = eth0.getTxBytes();
                    statResults.loTxBytes = lo.getTxBytes();

                    for (ProcessInfo processInfo : processInfoList) {
                        try {
                            StatResult statResult = new StatResult();
                            statResult.command = processInfo.cmd;
                            statResult.processId = processInfo.pid;
                            statResult.cpuPercentage = processInfo.sigarInfo.sigar.getProcCpu(processInfo.pid).getPercent() * 100;
                            ProcMem procMem = processInfo.sigarInfo.sigar.getProcMem(processInfo.pid);
                            statResult.residentMemBytes = procMem.getResident();
                            statResult.virtualMemBytes = procMem.getSize();
                            statResult.sharedMemBytes = procMem.getShare();
                            statResult.user = processInfo.user;
                            statResult.millisecond = statResults.millisecond;
                            statResult.executionId = statResults.executionId;
                            statResult.sequence = statResults.sequence;
                            statResults.addStateResult(statResult);
                        } catch (SigarException se) {
                            se.printStackTrace();
                        }
                    }
                    if (stat) {
                        save(statResults);
                    }
                    Thread.sleep(sleepMs);
                    long nowMs = System.currentTimeMillis();
//System.out.println(nowMs + " - " + startMs + " > " + msToRun + " = " + (nowMs - startMs) + " ? " + ((nowMs - startMs) > msToRun));
                    if ((nowMs - startMs) > msToRun) {
                        stopNow = true;
                        connection.close();
                    }
                    sequence++;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    public void save(StatResults statResults) {
        String insertSrsSql = " INSERT INTO HADOOP.BENCHMARK_STATS VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?)";
        String insertSrSql = " INSERT INTO HADOOP.BENCHMARK_STAT VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";
        try {
            connectToDb();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Got an exception and cannot continue...exiting...");
            System.exit(1);
        }

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(insertSrsSql);
            preparedStatement.setString(1, statResults.executionId);
            preparedStatement.setDouble(2, statResults.cpuPercentage);
            preparedStatement.setLong(3, statResults.memActualFreeBytes);
            preparedStatement.setLong(4, statResults.memFreeBytes);
            preparedStatement.setLong(5, statResults.memActualUsedBytes);
            preparedStatement.setLong(6, statResults.memUsedBytes);
            preparedStatement.setLong(7, statResults.eth0ReceivedBytes);
            preparedStatement.setLong(8, statResults.eth0TxBytes);
            preparedStatement.setLong(9, statResults.loReceivedBytes);
            preparedStatement.setLong(10, statResults.loTxBytes);
            preparedStatement.setLong(11, statResults.sequence);
            preparedStatement.setTimestamp(12, new Timestamp(statResults.millisecond));
            preparedStatement.execute();
            preparedStatement.close();
            if (detailedStat) {
                PreparedStatement preparedStatement2 = connection.prepareStatement(insertSrSql);
                for (StatResult statResult : statResults.statResultList) {
                    preparedStatement2.setString(1, statResult.executionId);
                    preparedStatement2.setDouble(2, statResult.cpuPercentage);
                    preparedStatement2.setLong(3, statResult.residentMemBytes);
                    preparedStatement2.setLong(4, statResult.virtualMemBytes);
                    preparedStatement2.setLong(5, statResult.sharedMemBytes);
                    preparedStatement2.setString(6, statResult.command);
                    preparedStatement2.setLong(7, statResult.sequence);
                    preparedStatement2.setTimestamp(8, new Timestamp(statResult.millisecond));
                    preparedStatement2.addBatch();
                }
                preparedStatement2.executeBatch();
                preparedStatement2.close();
            }
        } catch (SQLException sqle) {
            sqle.printStackTrace();
            System.exit(1);
        }
    }

    public static class StatResults {
        public String executionId;
        public long sequence;
        public long millisecond;
        public double cpuPercentage;
        public long memFreeBytes;
        public long memActualFreeBytes;
        public long memUsedBytes;
        public long memActualUsedBytes;
        public long eth0ReceivedBytes;
        public long loReceivedBytes;

        public long eth0TxBytes;
        public long loTxBytes;

        List<StatResult> statResultList = new ArrayList<StatResult>();

        public void addStateResult(StatResult statResult) {
            statResultList.add(statResult);
        }
    }

    public static class StatResult {
        public long millisecond;
        public long sequence;
        public String executionId;
        public String command;
        public String processId;
        public String user;
        public double cpuPercentage;
        public long residentMemBytes;
        public long virtualMemBytes;
        public long sharedMemBytes;
    }

    public void connectToDb() throws SQLException {
        if (connection == null) {
            connection = DriverManager.getConnection(connectionString, dbUser, dbPassword);
        } else {
            try {
                PreparedStatement statement = connection.prepareStatement("SELECT count(0) FROM HADOOP.BENCHMARK_STATS WHERE EXECUTION_ID = ?");
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();
                rs.next();
                int count = rs.getInt(1);
//System.out.println("Found " + count +" rows with execution_id = '" + executionId + "'");
                rs.close();
                statement.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Found an exception when checking the row count, renewing the db connection now...");
                connection = DriverManager.getConnection(connectionString, dbUser, dbPassword);
            }
        }
    }

    public static void main(String[] arg) throws Exception {
        Properties properties = new Properties();
        if (arg.length > 0) {
            properties.load(new FileInputStream(arg[0]));
        }
        boolean enableStat = Boolean.parseBoolean(properties.getProperty("enableStat", "false"));
        boolean enableDetailedStat = Boolean.parseBoolean(properties.getProperty("enableDetailedStat", "false"));
        long statFreqMs = Long.parseLong(properties.getProperty("statFreqMs", "20000"));
        int minutesToRun = Integer.parseInt(properties.getProperty("minutesToRun", "5"));

        String dbDriver = properties.getProperty("dbDriver");
        Class.forName(dbDriver).newInstance();
        String connectionString = properties.getProperty("connectionString");
        String dbUser = properties.getProperty("dbUser");
        String dbPassword = properties.getProperty("dbPassword");
        System.out.println("Trying to get a JDBC connection from " + connectionString);

        SigarInfoManager sigarInfoManager = new SigarInfoManager();

        sigarInfoManager.dbDriver = dbDriver;
        sigarInfoManager.connectionString = connectionString;
        sigarInfoManager.dbUser = dbUser;
        sigarInfoManager.dbPassword = dbPassword;

        sigarInfoManager.setSleepMs(statFreqMs);
        sigarInfoManager.setStat(enableStat);
        sigarInfoManager.setDetailedStat(enableDetailedStat);
        sigarInfoManager.connectToDb();
        sigarInfoManager.setMinutesToRun(minutesToRun);
        sigarInfoManager.setExecutionId("Daemon Thread Started at: " + FORMATTER.format(new Date(System.currentTimeMillis())) + ", set to run " + minutesToRun + " minutes");
        sigarInfoManager.start();
    }
}
