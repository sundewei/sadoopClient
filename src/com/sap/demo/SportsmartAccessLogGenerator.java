package com.sap.demo;

import com.sap.demo.dao.*;
import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.conf.IFileSystem;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/18/11
 * Time: 12:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class SportsmartAccessLogGenerator {

    private static Map<Integer, Double> MONTHLY_SALES_ADJ_FACTORS = new HashMap<Integer, Double>();
    private static Map<Integer, Double> HOUR_LOG_VOLUME = new HashMap<Integer, Double>();
    private static Map<Integer, Integer> WEEKDAY_LOG_VOLUME = new HashMap<Integer, Integer>();
    private static Calendar CALENDAR;
    private static Map<String, Double> REGION_ADJ_FACTORS = new HashMap<String, Double>();

    public static void main(String[] arg) throws Exception {
        List<CssUrl> cssUrls = Utility.getCssUrls("C:\\projects\\data\\sportmart\\urls\\cssUrls.csv");
        List<ImageUrl> imageUrls = Utility.getImageUrls("C:\\projects\\data\\sportmart\\urls\\imageUrls.csv");
        List<JsUrl> jsUrls = Utility.getJsUrls("C:\\projects\\data\\sportmart\\urls\\jsUrls.csv");
        ConfigurationManager cm = new ConfigurationManager("hadoop", "hadoop");
        IFileSystem fileSystem = cm.getFileSystem();
        System.out.println("Getting browser map");
        Map<String, List<String>> browserMap = Utility.getBrowserMap();
        System.out.println("Getting stateItemSoldMap");
        Map<String, Map<Integer, Double>> stateItemSoldMap = Utility.getStateItemSoldMap();
        System.out.println("Getting dateItemSoldMap");
        Map<String, Map<Integer, Double>> dateItemSoldMap = Utility.getDateItemSoldMap();
        System.out.println("Getting getItems");
        Collection<Item> items = Utility.getItems();
        System.out.println("Getting categoryItems");
        Map<String, List<Item>> categoryItems = Utility.getCategoryItems(items);
        //Map<String, List<Item>> subCategoryItems = Utility.getCategoryItems(items);
        System.out.println("Getting getItemMap");
        Map<String, Item> itemMap = Utility.getItemMap(items);
        System.out.println("Getting stateIpRangeMap");
        Map<String, TreeMap<Long, Long>> stateIpRangeMap = IpGenerator.getAllStateRangeMap();

        // 80K
        long dailyBaseReqCount2006 = 179000 * 3;
        long dailyBaseReqCount2007 = 205000 * 3;
        long dailyBaseReqCount2008 = 214000 * 3;
        long dailyBaseReqCount2009 = 235000 * 3;
        long dailyBaseReqCount = dailyBaseReqCount2006;

        Map<String, State> stateMap = Utility.getStateMap();
        Bag todayStateBag = null;
        Bag todayItemBag = null;
        Bag browserBag = getBrowserBag();

        Calendar startCalendar = Calendar.getInstance();
        startCalendar.set(Calendar.YEAR, 2006);
        startCalendar.set(Calendar.MONTH, Calendar.JANUARY);
        startCalendar.set(Calendar.DAY_OF_MONTH, 1);
        startCalendar.set(Calendar.HOUR_OF_DAY, 0);
        startCalendar.set(Calendar.MINUTE, 0);
        startCalendar.set(Calendar.SECOND, 0);
        startCalendar.set(Calendar.MILLISECOND, 0);

        Calendar endCalendar = Calendar.getInstance();
        endCalendar.set(Calendar.YEAR, 2010);
        endCalendar.set(Calendar.MONTH, Calendar.JANUARY);
        endCalendar.set(Calendar.DAY_OF_MONTH, 1);
        endCalendar.set(Calendar.HOUR_OF_DAY, 0);
        endCalendar.set(Calendar.MINUTE, 0);
        endCalendar.set(Calendar.SECOND, 0);
        endCalendar.set(Calendar.MILLISECOND, 0);

        Calendar indexCalendar = (Calendar) startCalendar.clone();

        while (indexCalendar.before(endCalendar)) {
            TreeSet<TimedString> timedStrings = new TreeSet<TimedString>();
            String nowDateString = Utility.SIMPLE_DATE_FORMAT.format(indexCalendar.getTime());
            File logFile = new File("C:\\projects\\data\\sportmart\\logs\\large_access_" + nowDateString + ".log");
            String yyyy = nowDateString.substring(0, 4);
            if (yyyy.equals("2007")) {
                dailyBaseReqCount = dailyBaseReqCount2007;
            } else if (yyyy.equals("2008")) {
                dailyBaseReqCount = dailyBaseReqCount2008;
            } else if (yyyy.equals("2009")) {
                dailyBaseReqCount = dailyBaseReqCount2009;
            }

            if (!logFile.exists()) {
                System.out.println(" Working on " + nowDateString);
                logFile.createNewFile();
            } else {
                System.out.println(" Skipping....... " + nowDateString);
                indexCalendar.add(Calendar.DATE, 1);
                continue;
            }
            double dateItemTotal = getItemSoldTotal(dateItemSoldMap, nowDateString);
            double allDateItemTotal = getAllItemSoldTotalByKeyPrefix(dateItemSoldMap, yyyy);
            double dateRatio = (365 * dateItemTotal) / allDateItemTotal;
            if (todayStateBag == null) {
                todayStateBag = getStateBag(stateMap, stateItemSoldMap, dateRatio);
            }

            if (todayItemBag == null) {
                todayItemBag = getItemBag(dateItemSoldMap.get(nowDateString));
            }

            for (int hour = 0; hour < 24; hour++) {
                System.out.println(nowDateString + ", Hour = " + hour);
                long hourlyReqCount = (long) (HOUR_LOG_VOLUME.get(hour) * Utility.getFuzzyNumber(dailyBaseReqCount / 24));
                int reqIndex = 0;

                while (reqIndex < hourlyReqCount) {
                    String stateAbb = todayStateBag.drawBall();
                    TreeMap<Long, Long> ipRangeMap = stateIpRangeMap.get(stateAbb);

                    // Need to get several pages
                    int viewedPageCount = Utility.RANDOM.nextInt(10) + 5;

                    String ip = IpGenerator.getIp(ipRangeMap);
                    List<String> userAgents = browserMap.get(browserBag.drawBall());
                    String userAgent = userAgents.get(Utility.RANDOM.nextInt(userAgents.size()));
                    long viewingStartMs = Utility.getViewingStartMs(indexCalendar, hour);

                    for (int i = 0; i < viewedPageCount; i++) {
                        int itemLookup = Integer.parseInt(todayItemBag.drawBall());
                        Item item = itemMap.get(String.valueOf(itemLookup));
                        //SOCCER
                        //BICYCLE
                        //SKATEBOARD
                        //FOOTBALL
                        //GOLF
                        //CAMPING

                        // Add SOCCER trend items
                        Set<Item> trendItems = getTrendItems(categoryItems, item, yyyy, "SOCCER", new int[]{300, 612, 972});
                        for (Item trendItem : trendItems) {
                            TimedString[] tss = getTimedStrings(nowDateString, trendItem.getItemLookup(), ip, viewingStartMs, userAgent, cssUrls, imageUrls, jsUrls);
                            timedStrings.addAll(Arrays.asList(tss));
                        }

                        // Add BICYCLE trend items
                        trendItems = getTrendItems(categoryItems, item, yyyy, "BICYCLE", new int[]{200, 430, 590});
                        for (Item trendItem : trendItems) {
                            TimedString[] tss = getTimedStrings(nowDateString, trendItem.getItemLookup(), ip, viewingStartMs, userAgent, cssUrls, imageUrls, jsUrls);
                            timedStrings.addAll(Arrays.asList(tss));
                        }

                        // Add SKATEBOARD trend items
                        trendItems = getTrendItems(categoryItems, item, yyyy, "SKATEBOARD", new int[]{180, 320, 420});
                        for (Item trendItem : trendItems) {
                            TimedString[] tss = getTimedStrings(nowDateString, trendItem.getItemLookup(), ip, viewingStartMs, userAgent, cssUrls, imageUrls, jsUrls);
                            timedStrings.addAll(Arrays.asList(tss));
                        }

                        // Add FOOTBALL trend items
                        trendItems = getTrendItems(categoryItems, item, yyyy, "FOOTBALL", new int[]{175, 250, 375});
                        for (Item trendItem : trendItems) {
                            TimedString[] tss = getTimedStrings(nowDateString, trendItem.getItemLookup(), ip, viewingStartMs, userAgent, cssUrls, imageUrls, jsUrls);
                            timedStrings.addAll(Arrays.asList(tss));
                        }

                        // Add GOLF trend items
                        trendItems = getTrendItems(categoryItems, item, yyyy, "GOLF", new int[]{120, 195, 230});
                        for (Item trendItem : trendItems) {
                            TimedString[] tss = getTimedStrings(nowDateString, trendItem.getItemLookup(), ip, viewingStartMs, userAgent, cssUrls, imageUrls, jsUrls);
                            timedStrings.addAll(Arrays.asList(tss));
                        }

                        // Add CAMPING trend items
                        trendItems = getTrendItems(categoryItems, item, yyyy, "CAMPING", new int[]{400, 720, 875});
                        for (Item trendItem : trendItems) {
                            TimedString[] tss = getTimedStrings(nowDateString, trendItem.getItemLookup(), ip, viewingStartMs, userAgent, cssUrls, imageUrls, jsUrls);
                            timedStrings.addAll(Arrays.asList(tss));
                        }

                        TimedString[] tss = getTimedStrings(nowDateString, itemLookup, ip, viewingStartMs, userAgent, cssUrls, imageUrls, jsUrls);
                        timedStrings.addAll(Arrays.asList(tss));

                        viewingStartMs = viewingStartMs + Utility.nextLong(6 * 60 * 1000);
                        while (!Utility.SIMPLE_DATE_FORMAT.format(new Timestamp(viewingStartMs)).equals(nowDateString)) {
                            viewingStartMs = Utility.getViewingStartMs(indexCalendar, hour);
                        }
                    }
                    reqIndex++;
                }
            }

            BufferedWriter out = new BufferedWriter(new FileWriter(logFile));
            for (TimedString ts : timedStrings) {
                out.write(ts.line);
                out.write("\n");
            }
            out.close();
            String hdfsFolder = cm.getRemoteFolder() + "sportmart/accessLogs/" + Utility.YYYY_MM_DATE_FORMAT.format(indexCalendar) + "/";
            if (!fileSystem.exists(hdfsFolder)) {
                System.out.println("Creating HDFS folder: " + hdfsFolder);
                fileSystem.mkdirs(hdfsFolder);
            }
            String hdfsFilename = hdfsFolder + logFile.getName();
            System.out.println("Begin uploading to HDFS: " + hdfsFilename);
            fileSystem.uploadFromLocalFile(logFile.getAbsolutePath(), hdfsFilename);
            System.out.println("Done uploading to HDFS: " + hdfsFilename);
            indexCalendar.add(Calendar.DATE, 1);
            todayStateBag = null;
            todayItemBag = null;
        }
        //*/
    }

    private static Set<Item> getTrendItems(Map<String, List<Item>> categoryItems, Item item, String yyyy, String catName, int[] nums) {
        Set<Item> addedItems = new HashSet<Item>();
        if (item.getCategory().equals(catName)) {

            List<Item> possibleSourceItems = categoryItems.get(item.getCategory());
            int random = Utility.RANDOM.nextInt(10000);
//System.out.println("catName="+catName+",yyyy="+yyyy+",random="+random+",nums[0]="+nums[0]);
            if (yyyy.equals("2007")) {
                if (random < nums[0]) {
//System.out.println("++++++++++catName="+catName+",yyyy="+yyyy+",random="+random+",nums[0]="+nums[0]);
                    addedItems.add(getPossibleItem(possibleSourceItems, new Item[]{item}));
                }
            } else if (yyyy.equals("2008")) {
                if (random < nums[1]) {
                    addedItems.add(getPossibleItem(possibleSourceItems, new Item[]{item}));
                }
            } else if (yyyy.equals("2009")) {
                if (random < nums[2]) {
                    addedItems.add(getPossibleItem(possibleSourceItems, new Item[]{item}));
                }
            }
        }
        return addedItems;
    }

    private static Item getPossibleItem(List<Item> source, Item[] exclude) {
        Item picked = source.get(Utility.RANDOM.nextInt(source.size()));
        while (true) {
            boolean found = true;
            for (Item excludeItem : exclude) {
                if (picked.getItemLookup() == excludeItem.getItemLookup()) {
                    found = false;
                }
            }
            if (found) {
                return picked;
            } else {
                picked = source.get(Utility.RANDOM.nextInt(source.size()));
            }
        }
    }

    private static TimedString[] getTimedStrings(String dateString, int itemLookup, String ip, long nowMs,
                                                 String userAgent, List<CssUrl> cssUrls, List<ImageUrl> imageUrls,
                                                 List<JsUrl> jsUrls) {
        long requestMsRange = 2200l;
        TimedString ts = new TimedString();
        Header h = getHeader(itemLookup);
        h.reqMap.put("User-Agent", userAgent);
        ts.line = Utility.getAccessLogLine(ip, getHeader(itemLookup), nowMs);
        ts.ms = nowMs;

        int count = Utility.RANDOM.nextInt(30) + 15;

        TimedString[] tss = new TimedString[count];
        tss[0] = ts;

        for (int i = 1; i < count; i++) {
            int random = Utility.RANDOM.nextInt(3);
            TimedString nowTs = new TimedString();
            long pageMs = nowMs + Utility.nextLong(requestMsRange);
            int tries = 1;
            while (!Utility.SIMPLE_DATE_FORMAT.format(new Timestamp(pageMs)).equals(dateString)) {
                pageMs = nowMs + Utility.nextLong(requestMsRange) - 100 * tries;
            }

            Header header = null;
            if (random == 0) {
                header = getHeader(itemLookup, cssUrls.get(Utility.RANDOM.nextInt(cssUrls.size())).getUrl(), userAgent);
                nowTs.line = Utility.getAccessLogLine(ip, header, pageMs);
            } else if (random == 1) {
                header = getHeader(itemLookup, imageUrls.get(Utility.RANDOM.nextInt(imageUrls.size())).getUrl(), userAgent);
                nowTs.line = Utility.getAccessLogLine(ip, header, pageMs);
            } else {
                header = getHeader(itemLookup, jsUrls.get(Utility.RANDOM.nextInt(jsUrls.size())).getUrl(), userAgent);
                nowTs.line = Utility.getAccessLogLine(ip, header, pageMs);
            }
            nowTs.ms = pageMs;
            tss[i] = nowTs;
        }
        return tss;
    }

    private static Bag getItemBag(Map<Integer, Double> todayItemSoldMap) {
        Bag bag = new Bag();
        for (Map.Entry<Integer, Double> entry : todayItemSoldMap.entrySet()) {
            String itemLookup = String.valueOf(entry.getKey());
            double volume = entry.getValue() * 10000;
            // Give some low value for items that has no volume
            if (volume <= 0) {
                volume = 100;
            }
            bag.addBalls(itemLookup, (long) volume);
        }
        return bag;
    }

    private static Bag getBrowserBag() throws Exception {
        Bag bag = new Bag();
        bag.addBalls("MSIE", 343l);
        bag.addBalls("Firefox", 262l);
        bag.addBalls("Chrome", 222);
        bag.addBalls("Safari", 64);
        bag.addBalls("Opera", 24);
        bag.addBalls("Others", 70);
        bag.addBalls("Mobile", 15);
        return bag;
    }

    private static Bag getStateBag(Map<String, State> stateMap,
                                   Map<String, Map<Integer, Double>> stateItemSoldMap,
                                   double dateRatio) {
        Bag todayStateBag = new Bag();
        for (Map.Entry<String, State> entry : stateMap.entrySet()) {
            State state = entry.getValue();
            // Skip DC
            if (!state.getAbbreviation().equalsIgnoreCase("DC")) {
                double stateItemTotal = getItemSoldTotal(stateItemSoldMap, state.getAbbreviation());
                double allStateItemTotal = getAllItemSoldTotal(stateItemSoldMap);
                double stateRatio = stateItemTotal / allStateItemTotal;
                long stateReqCount = (long) (dateRatio * stateRatio * 100000 * state.getPopulationPercentage() * getRegionFactor(state.getRegion()));
                todayStateBag.addBalls(state.getAbbreviation(), stateReqCount);
            }
        }
        return todayStateBag;
    }

    private static Header getHeader(int itemLookup, String otherUrl, String userAgent) {
        Header header = new Header();
        header.url = otherUrl;
        int lastSlash = otherUrl.lastIndexOf("/");
        header.resource = otherUrl.substring(lastSlash);
        header.method = "GET";
        header.httpStatusCode = 200;
        header.http = "HTTP/1.1";
        header.reqMap.put("Referer", "http://www.sportsauthority.com/product/index.jsp?productId=" + itemLookup + "&parentPage=family");
        header.reqMap.put("User-Agent", userAgent);
        return header;
    }

    private static double getItemSoldTotal(Map<Integer, Double> map) {
        double total = 0d;
        for (Map.Entry<Integer, Double> entry : map.entrySet()) {
            total += entry.getValue();
        }
        return total;
    }

    private static double getAllItemSoldTotal(Map<String, Map<Integer, Double>> map) {
        double total = 0d;
        for (Map.Entry<String, Map<Integer, Double>> entry : map.entrySet()) {
            total += getItemSoldTotal(entry.getValue());
        }
        return total;
    }

    private static double getAllItemSoldTotalByKeyPrefix(Map<String, Map<Integer, Double>> map, String keyPrefix) {
        double total = 0d;
        for (Map.Entry<String, Map<Integer, Double>> entry : map.entrySet()) {
            if (entry.getKey().startsWith(keyPrefix)) {
                total += getItemSoldTotal(entry.getValue());
            }
        }
        return total;
    }


    private static double getItemSoldTotal(Map<String, Map<Integer, Double>> map, String key) {
        Map<Integer, Double> target = map.get(key);
        return getItemSoldTotal(target);
    }

    private static double getRegionFactor(String region) {
        if (region == null) {
            return 0.4d;
        } else {
            return REGION_ADJ_FACTORS.get(region);
        }
    }

    static {
        CALENDAR = Calendar.getInstance();
        // 2011-01-01
        CALENDAR.set(Calendar.YEAR, 2006);
        CALENDAR.set(Calendar.MONTH, Calendar.JANUARY);
        CALENDAR.set(Calendar.DAY_OF_MONTH, 1);
        CALENDAR.set(Calendar.HOUR_OF_DAY, 0);
        CALENDAR.set(Calendar.MINUTE, 0);
        CALENDAR.set(Calendar.SECOND, 0);
        CALENDAR.set(Calendar.MILLISECOND, 0);

        HOUR_LOG_VOLUME.put(0, 0.035d);
        HOUR_LOG_VOLUME.put(1, 0.0175d);
        HOUR_LOG_VOLUME.put(2, 0.0175d);
        HOUR_LOG_VOLUME.put(3, 0.0175d);
        HOUR_LOG_VOLUME.put(4, 0.0175d);
        HOUR_LOG_VOLUME.put(5, 0.0175d);
        HOUR_LOG_VOLUME.put(6, 0.023d);
        HOUR_LOG_VOLUME.put(7, 0.0175d);
        HOUR_LOG_VOLUME.put(8, 0.0116d);
        HOUR_LOG_VOLUME.put(9, 0.07d);
        HOUR_LOG_VOLUME.put(10, 0.093d);
        HOUR_LOG_VOLUME.put(11, 0.085d);
        HOUR_LOG_VOLUME.put(12, 0.04d);
        HOUR_LOG_VOLUME.put(13, 0.0816d);
        HOUR_LOG_VOLUME.put(14, 0.093d);
        HOUR_LOG_VOLUME.put(15, 0.07d);
        HOUR_LOG_VOLUME.put(16, 0.0582d);
        HOUR_LOG_VOLUME.put(17, 0.0466d);
        HOUR_LOG_VOLUME.put(18, 0.023d);
        HOUR_LOG_VOLUME.put(19, 0.0757d);
        HOUR_LOG_VOLUME.put(20, 0.04d);
        HOUR_LOG_VOLUME.put(21, 0.029d);
        HOUR_LOG_VOLUME.put(22, 0.023d);
        HOUR_LOG_VOLUME.put(23, 0.035d);

        WEEKDAY_LOG_VOLUME.put(Calendar.SUNDAY, 75);
        WEEKDAY_LOG_VOLUME.put(Calendar.MONDAY, 80);
        WEEKDAY_LOG_VOLUME.put(Calendar.TUESDAY, 85);
        WEEKDAY_LOG_VOLUME.put(Calendar.WEDNESDAY, 89);
        WEEKDAY_LOG_VOLUME.put(Calendar.THURSDAY, 95);
        WEEKDAY_LOG_VOLUME.put(Calendar.FRIDAY, 85);
        WEEKDAY_LOG_VOLUME.put(Calendar.SATURDAY, 75);

        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.JANUARY, 0.871D);
        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.FEBRUARY, 0.892D);
        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.MARCH, 0.962D);
        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.APRIL, 0.972D);

        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.MAY, 0.997D);
        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.JUNE, 0.978D);
        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.JULY, 0.968D);
        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.AUGUST, 0.981D);

        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.SEPTEMBER, 0.912D);
        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.OCTOBER, 0.962D);
        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.NOVEMBER, 1.099D);
        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.DECEMBER, 1.415D);

        REGION_ADJ_FACTORS.put("Northeast", 1.1D);
        REGION_ADJ_FACTORS.put("Midwest", 1.08D);
        REGION_ADJ_FACTORS.put("South", 1.07D);
        REGION_ADJ_FACTORS.put("West", 1.17D);
    }

    public static Header getHeader(int itemLookup) {
        Header header = new Header();
        header.url = "http://www.sportsauthority.com/product/index.jsp?productId=" + itemLookup + "&parentPage=family";
        header.resource = "/product/index.jsp?productId=" + itemLookup + "&parentPage=family";
        header.method = "GET";
        header.httpStatusCode = 200;
        header.http = "HTTP/1.1";
        return header;
    }
}
