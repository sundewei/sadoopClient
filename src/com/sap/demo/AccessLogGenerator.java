package com.sap.demo;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/24/11
 * Time: 11:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccessLogGenerator {
    private static Calendar CALENDAR;
    private static Map<Integer, Integer> HOUR_LOG_VOLUME = new HashMap<Integer, Integer>();
    private static Map<Integer, Integer> WEEKDAY_LOG_VOLUME = new HashMap<Integer, Integer>();
    private static Random RANDOM = new Random();
    private static int PAGE_HIT_RATE = 90;

    static {
        CALENDAR = Calendar.getInstance();
        // 2011-10-01
        CALENDAR.set(Calendar.YEAR, 2008);
        CALENDAR.set(Calendar.MONTH, Calendar.JANUARY);
        CALENDAR.set(Calendar.DAY_OF_MONTH, 1);
        CALENDAR.set(Calendar.HOUR_OF_DAY, 0);
        CALENDAR.set(Calendar.MINUTE, 0);
        CALENDAR.set(Calendar.SECOND, 0);
        CALENDAR.set(Calendar.MILLISECOND, 0);

        HOUR_LOG_VOLUME.put(0, 10);
        HOUR_LOG_VOLUME.put(1, 5);
        HOUR_LOG_VOLUME.put(2, 6);
        HOUR_LOG_VOLUME.put(3, 5);
        HOUR_LOG_VOLUME.put(4, 4);
        HOUR_LOG_VOLUME.put(5, 3);
        HOUR_LOG_VOLUME.put(6, 10);
        HOUR_LOG_VOLUME.put(7, 15);
        HOUR_LOG_VOLUME.put(8, 30);
        HOUR_LOG_VOLUME.put(9, 70);
        HOUR_LOG_VOLUME.put(10, 80);
        HOUR_LOG_VOLUME.put(11, 75);
        HOUR_LOG_VOLUME.put(12, 35);
        HOUR_LOG_VOLUME.put(13, 70);
        HOUR_LOG_VOLUME.put(14, 80);
        HOUR_LOG_VOLUME.put(15, 60);
        HOUR_LOG_VOLUME.put(16, 50);
        HOUR_LOG_VOLUME.put(17, 40);
        HOUR_LOG_VOLUME.put(18, 10);
        HOUR_LOG_VOLUME.put(19, 15);
        HOUR_LOG_VOLUME.put(20, 25);
        HOUR_LOG_VOLUME.put(21, 30);
        HOUR_LOG_VOLUME.put(22, 20);
        HOUR_LOG_VOLUME.put(23, 10);

        WEEKDAY_LOG_VOLUME.put(Calendar.SUNDAY, 75);
        WEEKDAY_LOG_VOLUME.put(Calendar.MONDAY, 80);
        WEEKDAY_LOG_VOLUME.put(Calendar.TUESDAY, 85);
        WEEKDAY_LOG_VOLUME.put(Calendar.WEDNESDAY, 89);
        WEEKDAY_LOG_VOLUME.put(Calendar.THURSDAY, 95);
        WEEKDAY_LOG_VOLUME.put(Calendar.FRIDAY, 85);
        WEEKDAY_LOG_VOLUME.put(Calendar.SATURDAY, 75);
    }

    /*
    public static void main(String[] arg) throws Exception {
        int dayIndex = 1;
        int dayCount = 365 * 3;
        //long intervalMs = 5 * 60 * 1000;
        long intervalMs = 5 * 60 * 1000;

        Calendar cloneCalendar = (Calendar) CALENDAR.clone();
        while (dayIndex <= dayCount) {
            Set<TimedString> sortedLines = new TreeSet<TimedString>();
            String nowLogFilename = getFilename();
System.out.println("Working on " + nowLogFilename);
            long nowMsStart = CALENDAR.getTime().getTime();
            long nowMsRunning = CALENDAR.getTime().getTime();
//int count = 0;
//Map<Integer, Integer> hourCount = new TreeMap<Integer, Integer>();
//System.out.println(CALENDAR.getTime());
            while (nowMsRunning - nowMsStart < (24 * 3600000 - intervalMs)) {
//count++;
                nowMsRunning = nowMsRunning + getMsToAdd(intervalMs, cloneCalendar);
                cloneCalendar.setTimeInMillis(nowMsRunning);
//if(count % 10 == 0) {
//    System.out.println(cloneCalendar.getTime());
//}
                ViewedPageGenerator.ViewedPage[] viewedPages = ViewedPageGenerator.getViewedPages(RANDOM.nextInt(ViewedPageGenerator.PAGE_LIST_SIZE));
                for (ViewedPageGenerator.ViewedPage viewPage: viewedPages) {
                    String ip = IpGenerator.getIp(RANDOM.nextInt(IpGenerator.FIXED_IP_SIZE));

                    TimedString[] tss = viewPage.getTimedStrings(ip, nowMsRunning);
                    // Volume control
                    if (RANDOM.nextInt(100) < WEEKDAY_LOG_VOLUME.get(CALENDAR.get(Calendar.DAY_OF_WEEK))) {
                        for (TimedString ts: tss) {
                            // Hit rate
                            if (RANDOM.nextInt(100) < PAGE_HIT_RATE) {
                                sortedLines.add(ts);
                            }
                        }
                    }
                }
//if (!hourCount.containsKey(cloneCalendar.get(Calendar.HOUR_OF_DAY))) {
//hourCount.put(cloneCalendar.get(Calendar.HOUR_OF_DAY), 1);
//System.out.println(cloneCalendar.get(Calendar.HOUR_OF_DAY));
//} else {
//    hourCount.put(cloneCalendar.get(Calendar.HOUR_OF_DAY), hourCount.get(cloneCalendar.get(Calendar.HOUR_OF_DAY)) + 1);
//}
            }
            FileUtils.writeLines(new File(nowLogFilename), sortedLines);
//System.out.println(nowLogFilename);
            CALENDAR.set(Calendar.DAY_OF_MONTH, CALENDAR.get(Calendar.DAY_OF_MONTH) + 1);
            dayIndex++;

//for (Map.Entry<Integer, Integer> entry: hourCount.entrySet()) {
//System.out.println(entry.getKey() + " : " + entry.getValue());
//}
        }
    }
    */
    private static String getFilename() {
        return "C:\\projects\\data\\newLogsHome\\localhost_access_log." +
                CALENDAR.get(Calendar.YEAR) +
                "-" + Utility.getPaddedNumberString(CALENDAR.get(Calendar.MONTH) + 1, 2, "0") +
                "-" + Utility.getPaddedNumberString(CALENDAR.get(Calendar.DAY_OF_MONTH), 2, "0") +
                ".txt";
    }

    private static long getMsToAdd(long intervalMs, Calendar cloneCalendar) {
        long msToAdd = intervalMs / HOUR_LOG_VOLUME.get(cloneCalendar.get(Calendar.HOUR_OF_DAY));
        long variableMs = intervalMs / HOUR_LOG_VOLUME.get(cloneCalendar.get(Calendar.HOUR_OF_DAY)) / (RANDOM.nextInt(10) + 1);
        msToAdd += variableMs;
        return msToAdd;
    }

}
