package com.sap.demo;

import com.sap.demo.dao.Item;
import com.sap.demo.data.HtmlPageParser;
import org.apache.commons.csv.CSVUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/1/11
 * Time: 11:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class PosRowGenerator {
    private static Map<Integer, Double> MONTHLY_SALES_ADJ_FACTORS = new HashMap<Integer, Double>();
    private static Map<String, Double> STORE_TYPE_ADJ_FACTORS = new HashMap<String, Double>();
    private static Map<String, Double> REGION_ADJ_FACTORS = new HashMap<String, Double>();
    private static Map<Integer, Double> DAY_OF_WEEK_FACTORS = new HashMap<Integer, Double>();
    private static Map<String, StateIncome> STATE_INCOMES = new HashMap<String, StateIncome>();

    private static final long MAX_TX_DOLLAR = 32 * 1000 * 1000 * 12;

    private static final int MAX_BASKET_DOLLAR = 100;

    private static final int AVG_ITEM_COUNT = 5;

    private static final Calendar START_CALENDAR = Calendar.getInstance();

    private static final Calendar END_CALENDAR = Calendar.getInstance();

    //private static final Map<String, List<Store>> REGION_STORES = new HashMap<String, List<Store>>();

    //private static final Map<String, List<Store>> TYPE_STORES = new HashMap<String, List<Store>>();
    private static List<Store> STORES = new ArrayList<Store>();

    private static int STORE_INDEX = 0;

    static {
        // 0.871   0.892   0.962   0.972   0.997   0.978   0.968   0.981   0.912   0.962   1.099   1.415

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

        // Supercenter
        // Wal-Mart
        // CLUB
        // DC
        // Neighborhood Market
        // Marketside

        STORE_TYPE_ADJ_FACTORS.put("Supercenter", 1D);
        STORE_TYPE_ADJ_FACTORS.put("Wal-Mart", 1D);
        STORE_TYPE_ADJ_FACTORS.put("CLUB", 0.45D);
        STORE_TYPE_ADJ_FACTORS.put("DC", 0.85D);
        STORE_TYPE_ADJ_FACTORS.put("Neighborhood Market", 0.6D);
        STORE_TYPE_ADJ_FACTORS.put("Marketside", 0.35D);
        /*
        START_CALENDAR.set(Calendar.YEAR, 2010);
        START_CALENDAR.set(Calendar.MONTH, Calendar.JANUARY);
        START_CALENDAR.set(Calendar.DAY_OF_MONTH, 1);
        START_CALENDAR.set(Calendar.HOUR_OF_DAY, 0);
        START_CALENDAR.set(Calendar.MINUTE, 0);
        START_CALENDAR.set(Calendar.SECOND, 0);
        START_CALENDAR.set(Calendar.MILLISECOND, 0);
        */

        START_CALENDAR.set(Calendar.YEAR, 2009);
        START_CALENDAR.set(Calendar.MONTH, Calendar.JANUARY);
        START_CALENDAR.set(Calendar.DAY_OF_MONTH, 1);
        START_CALENDAR.set(Calendar.HOUR_OF_DAY, 0);
        START_CALENDAR.set(Calendar.MINUTE, 0);
        START_CALENDAR.set(Calendar.SECOND, 0);
        START_CALENDAR.set(Calendar.MILLISECOND, 0);

        END_CALENDAR.set(Calendar.YEAR, 2010);
        END_CALENDAR.set(Calendar.MONTH, Calendar.DECEMBER);
        END_CALENDAR.set(Calendar.DAY_OF_MONTH, 31);
        END_CALENDAR.set(Calendar.HOUR_OF_DAY, 23);
        END_CALENDAR.set(Calendar.MINUTE, 59);
        END_CALENDAR.set(Calendar.SECOND, 59);
        END_CALENDAR.set(Calendar.MILLISECOND, 999);

        /*
        END_CALENDAR.set(Calendar.YEAR, 2010);
        END_CALENDAR.set(Calendar.MONTH, Calendar.DECEMBER);
        END_CALENDAR.set(Calendar.DAY_OF_MONTH, 31);
        END_CALENDAR.set(Calendar.HOUR_OF_DAY, 23);
        END_CALENDAR.set(Calendar.MINUTE, 59);
        END_CALENDAR.set(Calendar.SECOND, 59);
        END_CALENDAR.set(Calendar.MILLISECOND, 999);
         */

        REGION_ADJ_FACTORS.put("Northeast", 1.1D);
        REGION_ADJ_FACTORS.put("Midwest", 1.08D);
        REGION_ADJ_FACTORS.put("South", 1.07D);
        REGION_ADJ_FACTORS.put("West", 0.9D);

        DAY_OF_WEEK_FACTORS.put(Calendar.MONDAY, 0.92d);
        DAY_OF_WEEK_FACTORS.put(Calendar.TUESDAY, 0.93d);
        DAY_OF_WEEK_FACTORS.put(Calendar.WEDNESDAY, 0.91d);
        DAY_OF_WEEK_FACTORS.put(Calendar.THURSDAY, 0.98d);
        DAY_OF_WEEK_FACTORS.put(Calendar.FRIDAY, 1.10d);
        DAY_OF_WEEK_FACTORS.put(Calendar.SATURDAY, 1.14d);
        DAY_OF_WEEK_FACTORS.put(Calendar.SUNDAY, 1.12d);


        try {
            List<StateIncome> list = getStateIncome();
            for (StateIncome stateIncome : list) {
                STATE_INCOMES.put(stateIncome.fullName, stateIncome);
            }

            STORES = getStores();
            //for (Store store: stores) {
            /*
            List<Store> regStore = REGION_STORES.get(store.region);
            if (regStore != null) {
                regStore.add(store);
            } else {
                regStore = new ArrayList<Store>();
                regStore.add(store);
                REGION_STORES.put(store.region, regStore);
            }

            List<Store> typeStore = TYPE_STORES.get(store.storeType);
            if (typeStore != null) {
                typeStore.add(store);
            } else {
                typeStore = new ArrayList<Store>();
                typeStore.add(store);
                TYPE_STORES.put(store.storeType, typeStore);
            }
            */
            //STORES.add(store);
            //}
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] arg) throws Exception {

        Map<String, Item> itemMap = Utility.getItemMap(null);
        Map<String, String> headerIdItemIdMap = HtmlPageParser.getHeaderIdItemIdMap();
        ViewedPageGenerator.ViewedPage[] pages = ViewedPageGenerator.getViewedPages();
        Map<String, Integer> dateMap = Utility.getDateMap();
        int count = 0;
        Connection conn = Utility.getConnection();
        while (START_CALENDAR.before(END_CALENDAR)) {
            long start = System.currentTimeMillis();
            long txMax = Utility.getFuzzyNumber(MAX_TX_DOLLAR, 1785, 9215);
            System.out.println("Working on " + Utility.SIMPLE_DATE_FORMAT.format(START_CALENDAR.getTime()));
            Collection<Transaction> transactions = getTransactions(itemMap, headerIdItemIdMap, pages, dateMap, txMax, START_CALENDAR);
            System.out.println("Got " + transactions.size() + " transactions.");
            Utility.insertTransactions(conn, transactions);
            long end = System.currentTimeMillis();
            System.out.println("Took " + ((end - start) / 1000) + " seconds...\n\n");
            START_CALENDAR.add(Calendar.DATE, 1);
        }
    }

    public static List<StateIncome> getStateIncome() throws Exception {
        List<String> stores = FileUtils.readLines(new File("C:\\projects\\data\\state\\stateIncome.csv"));
        List<StateIncome> stateIncomes = new ArrayList<StateIncome>();
        for (String store : stores) {
            String[] storeValues = CSVUtils.parseLine(store);
            StateIncome si = new StateIncome();
            if (storeValues[0] != null && !storeValues[0].equals("")) {
                si.fullName = storeValues[0];
                si.region = Utility.STATE_REGION.get(si.fullName);
                si.income = Integer.parseInt(storeValues[1]);
                stateIncomes.add(si);
            } else {
                StateIncome.avg = Integer.parseInt(storeValues[1]);
            }
        }
        return stateIncomes;
    }

    public static List<Store> getStores() throws Exception {
        Map<String, String> stateAbbrFullMap = Utility.getStateAbbrFullMap();
        List<String> stores = FileUtils.readLines(new File("C:\\projects\\data\\store\\store_dim.csv"));
        List<Store> found = new ArrayList<Store>();
        boolean skipFirst = false;
        for (String store : stores) {
            if (skipFirst) {
                String[] storeValues = CSVUtils.parseLine(store);
                Store st = new Store();
                st.id = storeValues[0];
                st.region = storeValues[13];
                st.storeType = storeValues[4];
                st.stateAbbr = storeValues[9];
                st.stateFullname = stateAbbrFullMap.get(st.stateAbbr);
                found.add(st);
            } else {
                skipFirst = true;
            }
        }
        return found;
    }

    public static Store getStore() {
        Store store = STORES.get(STORE_INDEX);
//System.out.println(STORE_INDEX + ", store="+store.sessionNum+", "+store.stateAbbr+", "+store.storeType);
        STORE_INDEX++;
        if (STORE_INDEX == STORES.size()) {
            STORE_INDEX = 0;
        }
        return store;
    }

    public static void readFromStore() throws Exception {
        Connection conn = Utility.getConnection();
        PreparedStatement stmt1 =
                conn.prepareStatement("SELECT * from STORE_DIM ");
        ResultSet rs = stmt1.executeQuery();
        FileUtils.write(new File("C:\\projects\\data\\store\\store_dim.csv"), Utility.getCsvTable(rs));
        stmt1.close();
        conn.close();
    }

    public static Collection<Transaction> getTransactions(Map<String, Item> itemMap,
                                                          Map<String, String> headerIdItemIdMap,
                                                          ViewedPageGenerator.ViewedPage[] pages,
                                                          Map<String, Integer> dateMap,
                                                          long txMax,
                                                          Calendar nowCalendar)
            throws Exception {
        long start = System.currentTimeMillis();
        //System.out.println("max_daily_tx_dollar: " + txMax);
        txMax = (long) (txMax * MONTHLY_SALES_ADJ_FACTORS.get(nowCalendar.get(Calendar.MONTH)));
        txMax = (long) (txMax * DAY_OF_WEEK_FACTORS.get(START_CALENDAR.get(Calendar.DAY_OF_WEEK)));
        System.out.println("max_daily_tx_dollar: " + txMax);
        double txDollars = 0;
        //int highPriced = 0;
        List<Transaction> transactions = new ArrayList<Transaction>();
        while (txDollars < txMax) {
            double keepChance = 1d;
            Store store = getStore();
            keepChance = keepChance * STORE_TYPE_ADJ_FACTORS.get(store.storeType);
            keepChance = keepChance * REGION_ADJ_FACTORS.get(store.region);
            keepChance = keepChance * STATE_INCOMES.get(store.stateFullname).getAdjFactor();

            if (Utility.RANDOM.nextInt(1000) < keepChance * 1000) {
                Transaction tx = getTransaction(itemMap, headerIdItemIdMap, pages);
                tx.store = store;
                tx.dateId = dateMap.get(Utility.SIMPLE_DATE_FORMAT.format(nowCalendar.getTime()));
                transactions.add(tx);
                txDollars += tx.total;
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Transactions took " + (end - start) / 1000 + " seconds to generate...");
        //System.out.println(transactions.size());
        //System.out.println("highPriced="+highPriced);
        return transactions;
    }

    public static Transaction getTransaction(Map<String, Item> itemMap,
                                             Map<String, String> headerIdItemIdMap,
                                             ViewedPageGenerator.ViewedPage[] pages)
            throws Exception {

        int itemCountMax = Utility.getFuzzyNumber(AVG_ITEM_COUNT);
        int totalMax = Utility.getFuzzyNumber(MAX_BASKET_DOLLAR);

        //System.out.println("max_total_basket_dollar: " + totalMax);
        //System.out.println("max_item_count: " + itemCountMax);

        List<Item> items = new ArrayList<Item>();
        for (ViewedPageGenerator.ViewedPage page : pages) {
            Item item = itemMap.get(headerIdItemIdMap.get(page.pageId));
            if (item != null) {
                items.add(item);
            }
        }

        Map<Item, Integer> posMap = new HashMap<Item, Integer>();
        double total = 0;
        int itemCount = 0;
        while (true) {
            Item item = items.get(Utility.RANDOM.nextInt(items.size()));
            String priceStr = item.getColumn("UNIT_PRICE");
            boolean keepAdding = false;
            if (total < totalMax) {
                keepAdding = true;
                //System.out.println("KeepAdding 1");
            }

            if (!keepAdding) {
                if (total < totalMax * 1.3 && itemCount < itemCountMax) {
                    keepAdding = true;
                    //System.out.println("KeepAdding 2");
                } else {
                    if (Utility.RANDOM.nextInt(100) > 90) {
                        keepAdding = true;
                        //System.out.println("KeepAdding 3");
                    }
                }
            }


            if (keepAdding) {
                total += Double.parseDouble(priceStr);
                itemCount++;
                if (posMap.containsKey(item)) {
                    posMap.put(item, posMap.get(item) + 1);
                } else {
                    posMap.put(item, 1);
                }
            } else {
                break;
            }
        }
        return new Transaction(posMap);
    }


    public static void listItem() throws Exception {

        List<Item> items = Utility.getItems(null);
        for (Item item : items) {
            System.out.println(item.getColumn("DESCRIPTION"));
            System.out.println("pstmt.setString(1, \"\");");
            System.out.println("pstmt.setInt(" + item.getColumn("UPC") + ");\n");
        }

    }


    public static void updatePriceCost() throws Exception {
        Connection conn = Utility.getConnection();
        PreparedStatement pstmt = conn.prepareStatement("UPDATE ITEM_DIM set UNIT_COST = ?, UNIT_PRICE = ? WHERE ID = ?");
        List<Item> items = Utility.getItems(null);
        for (Item item : items) {
            String priceStr = item.getColumn("UNIT_COST");
            double price = Double.parseDouble(priceStr);
            double cost = 0;
            if (price < 10) {
                cost = ((100 - (Utility.RANDOM.nextInt(80) + 5)) * price) / 100;
            } else if (price < 50 && price > 10) {
                cost = ((100 - (Utility.RANDOM.nextInt(25) + 10)) * price) / 100;
            } else if (price < 100 && price > 50) {
                cost = ((100 - (Utility.RANDOM.nextInt(10) + 10)) * price) / 100;
            } else {
                cost = ((100 - (Utility.RANDOM.nextInt(10) + 6)) * price) / 100;
            }
            String costStr = Utility.MONEY_FORMATTER.format(cost);
//System.out.println(item.getColumn("ID") + ", price = "+priceStr+", cost="+costStr);

            pstmt.setString(1, costStr);
            pstmt.setString(2, priceStr);
            pstmt.setString(3, item.getColumn("ID"));
            pstmt.executeUpdate();
        }
        pstmt.close();
        conn.close();
    }

    public static void updateSubCategory() throws Exception {
        Connection conn = Utility.getConnection();
        PreparedStatement pstmt = conn.prepareStatement("UPDATE ITEM_DIM set SUB_CATEGORY = ? WHERE UPC = ?");
        pstmt.setString(1, "20");
        pstmt.setInt(2, 140541278);
        pstmt.executeUpdate();
        pstmt.close();
        conn.close();
    }

    public static class Transaction {
        Map<Item, Integer> itemCountMap;
        public Store store;
        public double total = 0;
        public double cost = 0;
        public double revenue = 0;
        public int dateId;

        public Transaction(Map<Item, Integer> map) throws Exception {
            this.itemCountMap = map;
            for (Map.Entry<Item, Integer> entry : itemCountMap.entrySet()) {
                total += Utility.MONEY_FORMATTER.parse(entry.getKey().getColumn("UNIT_PRICE")).doubleValue() * entry.getValue();
                cost += Utility.MONEY_FORMATTER.parse(entry.getKey().getColumn("UNIT_COST")).doubleValue() * entry.getValue();
            }
            revenue = total - cost;
        }

        @Override
        public String toString() {
            return "Transaction{" +
                    "  total=" + total +
                    ", cost=" + cost +
                    ", revenue=" + revenue +
                    '}';
        }
    }

    public static class Store {
        public String region;
        public String storeType;
        public String id;
        public String stateAbbr;
        public String stateFullname;
    }

    public static class StateIncome {
        public String fullName;
        public int income;
        public String region;
        public static int avg;

        public double getAdjFactor() {
            return ((double) avg) / ((double) income);
        }

    }
}
