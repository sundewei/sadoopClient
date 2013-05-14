package com.sap.mains;

import com.sap.demo.AccessEntry;
import com.sap.demo.Utility;
import com.sap.hadoop.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;


/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/9/12
 * Time: 4:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class LogSender {
    private static final DefaultHttpClient client = new DefaultHttpClient();
    private static final Calendar START_CALENDAR = Calendar.getInstance();
    private static final Calendar END_CALENDAR = Calendar.getInstance();
    private static final DateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final DateFormat YYYY_FORMAT = new SimpleDateFormat("yyyy");
    private static final DateFormat MM_FORMAT = new SimpleDateFormat("MM");
    private static final DateFormat DD_FORMAT = new SimpleDateFormat("dd");

    public static final Random RANDOM = new Random();
    private static List<String> HOSTS = new ArrayList<String>();
    private static String HOST_NAME;
    private static String REST_URL;

    private static void initDates() throws UnknownHostException {
        START_CALENDAR.set(Calendar.YEAR, 2012);
        START_CALENDAR.set(Calendar.MONTH, Calendar.JANUARY);
        START_CALENDAR.set(Calendar.DAY_OF_MONTH, 1);
        START_CALENDAR.set(Calendar.HOUR_OF_DAY, 0);
        START_CALENDAR.set(Calendar.MINUTE, 0);
        START_CALENDAR.set(Calendar.SECOND, 0);
        START_CALENDAR.set(Calendar.MILLISECOND, 0);

        END_CALENDAR.set(Calendar.YEAR, 2012);
        END_CALENDAR.set(Calendar.MONTH, Calendar.DECEMBER);
        END_CALENDAR.set(Calendar.DAY_OF_MONTH, 31);
        END_CALENDAR.set(Calendar.HOUR_OF_DAY, 23);
        END_CALENDAR.set(Calendar.MINUTE, 59);
        END_CALENDAR.set(Calendar.SECOND, 59);
        END_CALENDAR.set(Calendar.MILLISECOND, 999);
        HOSTS.add("LLBPAL36");
        HOSTS.add("LLBPAL35");
        HOSTS.add("LLBPAL34");
        HOSTS.add("LLBPAL33");
        HOSTS.add("LLBPAL32");
        HOST_NAME = InetAddress.getLocalHost().getHostName().toUpperCase();
        if (!HOSTS.contains(HOST_NAME)) {
            REST_URL = "http://" + HOSTS.get(RANDOM.nextInt(HOSTS.size())) + ":8182/table/22";
        } else {
            REST_URL = "http://" + HOST_NAME + ":8182/table/22";
        }
    }

    public static void main(String[] arg) throws Exception {
        initDates();
        ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "abcd1234");
        Configuration configuration = configurationManager.getConfiguration();
        Calendar indexCalendar = (Calendar) START_CALENDAR.clone();
        while (indexCalendar.before(END_CALENDAR)) {
            String dateString = SIMPLE_DATE_FORMAT.format(indexCalendar.getTime());
            String flagLocation = "/data/shield/data/flags/" + dateString + ".flag";
            String errorFlagLocation = "/data/shield/data/flags/" + dateString + ".error";
            String location = "/data/shield/data/" + dateString + ".log";
            Path path = new Path(flagLocation);
            FileSystem fs = path.getFileSystem(configuration);
            if (!fs.exists(path)) {
                fs.create(path).close();
                System.out.println("Working on " + flagLocation + ", using " + REST_URL);
                InputStream inputStream = configurationManager.getFileSystem().getInputStream(location);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                int logCount = (int) (Math.random() * 100000) + 1;
                String line = reader.readLine();
                List<String> lines = new ArrayList<String>();
                while (line != null) {
                    AccessEntry accessData = Utility.getAccessEntry(line.toString());
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTimeInMillis(accessData.timestamp.getTime());
                    line = YYYY_FORMAT.format(calendar.getTime()) + "," +
                            MM_FORMAT.format(calendar.getTime()) + "," +
                            DD_FORMAT.format(calendar.getTime()) + "," + line;
                    if (lines.size() >= logCount) {
                        sendLogs(fs, errorFlagLocation, REST_URL, lines, 10);
                        lines = new ArrayList<String>();
                    }
                    line = reader.readLine();
                }

                if (lines.size() > 0) {
                    sendLogs(fs, errorFlagLocation, REST_URL, lines, 10);
                }
            } else {
                System.out.println("Skipping " + flagLocation + ", it's already being worked on");
            }
            indexCalendar.add(Calendar.DATE, 1);
        }
    }

    private static void sendLogs(FileSystem fs, String errorFlagLocation, String restUrl, List<String> lines, int retry) throws Exception {
        int failCount = 0;
        boolean ok = false;
        while (failCount < retry) {
            try {
                sendLogs(restUrl, lines);
                ok = true;
                break;
            } catch (Exception e) {
                System.out.println("Retry: " + failCount + "/" + retry + ", line size: " + lines.size() + ",  because of this error: " + e.getMessage());
            }
            failCount++;
        }

        if (!ok) {
            fs.create(new Path(errorFlagLocation)).close();
        }
    }

    private static void sendLogs(String restUrl, List<String> lines) throws Exception {
        HttpPost httpPost = new HttpPost(restUrl);
        httpPost.addHeader("Content-Type", "text/csv");
        StringEntity stringEntity = new StringEntity(getLine(lines));
        httpPost.setEntity(stringEntity);
        HttpResponse response = client.execute(httpPost);
        response.getEntity().getContent().close();
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException(response.getStatusLine().getReasonPhrase());
        }

    }

    private static String getLine(List<String> lines) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String line : lines) {
            stringBuilder.append(line).append("\n");
        }
        return stringBuilder.toString();
    }
}
