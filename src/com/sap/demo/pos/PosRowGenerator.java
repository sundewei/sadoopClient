package com.sap.demo.pos;

import com.sap.demo.Bag;
import com.sap.demo.Utility;
import com.sap.demo.dao.State;
import com.sap.demo.pos.beans.AmazonProduct;

import java.io.File;
import java.sql.Connection;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 2/13/12
 * Time: 4:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class PosRowGenerator {
    private static Random RANDOM = new Random();
    private static Map<Integer, Double> MONTHLY_SALES_ADJ_FACTORS = new HashMap<Integer, Double>();
    private static Map<Integer, Double> DAY_OF_WEEK_FACTORS = new HashMap<Integer, Double>();
    private static Map<String, State> STATE_MAP;
    private static Bag STATE_POPULATION_BAG = new Bag();
    private static Bag CATEGORY_ITEM_COUNT_BAG = new Bag();
    private static Bag QUANTITY_BAG = new Bag();
    private static Bag SALES_TOTAL_BUCKET_BAG = new Bag();
    private static Map<Integer, Bag> CATEGORY_ITEM_BAG_MAP = new HashMap<Integer, Bag>();
    private static final Calendar START_CALENDAR = Calendar.getInstance();
    private static final Calendar END_CALENDAR = Calendar.getInstance();
    public static Map<Integer, AmazonProduct> AMAZON_PRODUCT_MAP;
    private static Map<String, List<Integer>> STATE_LOCATION_ID_MAP;
    private static Map<String, Integer> DATE_STRING_ID_MAP;
    public static Map<Integer, String> ID_DATE_STRING_MAP;
    private static Map<Integer, Double> YEAR_ADJ_FACTOR = new HashMap<Integer, Double>();

    private static final long MAX_TX_DOLLAR = 1000000 * 145;

    static {
        /*
        START_CALENDAR.set(Calendar.YEAR, 2010);
        START_CALENDAR.set(Calendar.MONTH, Calendar.OCTOBER);
        START_CALENDAR.set(Calendar.DAY_OF_MONTH, 18);
        START_CALENDAR.set(Calendar.HOUR_OF_DAY, 0);
        START_CALENDAR.set(Calendar.MINUTE, 0);
        START_CALENDAR.set(Calendar.SECOND, 0);
        START_CALENDAR.set(Calendar.MILLISECOND, 0);

        END_CALENDAR.set(Calendar.YEAR, 2011);
        END_CALENDAR.set(Calendar.MONTH, Calendar.DECEMBER);
        END_CALENDAR.set(Calendar.DAY_OF_MONTH, 31);
        END_CALENDAR.set(Calendar.HOUR_OF_DAY, 23);
        END_CALENDAR.set(Calendar.MINUTE, 59);
        END_CALENDAR.set(Calendar.SECOND, 59);
        END_CALENDAR.set(Calendar.MILLISECOND, 999);
        */

        START_CALENDAR.set(Calendar.YEAR, 2008);
        START_CALENDAR.set(Calendar.MONTH, Calendar.JANUARY);
        START_CALENDAR.set(Calendar.DAY_OF_MONTH, 1);
        START_CALENDAR.set(Calendar.HOUR_OF_DAY, 0);
        START_CALENDAR.set(Calendar.MINUTE, 0);
        START_CALENDAR.set(Calendar.SECOND, 0);
        START_CALENDAR.set(Calendar.MILLISECOND, 0);

        END_CALENDAR.set(Calendar.YEAR, 2012);
        END_CALENDAR.set(Calendar.MONTH, Calendar.MAY);
        END_CALENDAR.set(Calendar.DAY_OF_MONTH, 16);
        END_CALENDAR.set(Calendar.HOUR_OF_DAY, 23);
        END_CALENDAR.set(Calendar.MINUTE, 59);
        END_CALENDAR.set(Calendar.SECOND, 59);
        END_CALENDAR.set(Calendar.MILLISECOND, 999);


        DAY_OF_WEEK_FACTORS.put(Calendar.MONDAY, 0.92d);
        DAY_OF_WEEK_FACTORS.put(Calendar.TUESDAY, 0.93d);
        DAY_OF_WEEK_FACTORS.put(Calendar.WEDNESDAY, 0.91d);
        DAY_OF_WEEK_FACTORS.put(Calendar.THURSDAY, 0.97d);
        DAY_OF_WEEK_FACTORS.put(Calendar.FRIDAY, 1.10d);
        DAY_OF_WEEK_FACTORS.put(Calendar.SATURDAY, 1.14d);
        DAY_OF_WEEK_FACTORS.put(Calendar.SUNDAY, 1.12d);

        // Month adj factors
        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.JANUARY, 0.871D);
        MONTHLY_SALES_ADJ_FACTORS.put(Calendar.FEBRUARY, 0.882D);
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

        YEAR_ADJ_FACTOR.put(2008, 1d);
        YEAR_ADJ_FACTOR.put(2009, 1d * 1.12d);
        YEAR_ADJ_FACTOR.put(2010, 1d * 1.12d * 1.15d);
        YEAR_ADJ_FACTOR.put(2011, 1d * 1.12d * 1.15d * 1.17d);
        YEAR_ADJ_FACTOR.put(2012, 1.98d);

        try {
            STATE_MAP = com.sap.demo.pos.Utility.getStateMap();
            AMAZON_PRODUCT_MAP = DatabaseUtility.getAmazonProductMap();
            STATE_LOCATION_ID_MAP = DatabaseUtility.getStateLocationIdMap();
            DATE_STRING_ID_MAP = DatabaseUtility.getDateStringIdMap();
            ID_DATE_STRING_MAP = new HashMap<Integer, String>(DATE_STRING_ID_MAP.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (Map.Entry<String, Integer> entry : DATE_STRING_ID_MAP.entrySet()) {
            ID_DATE_STRING_MAP.put(entry.getValue(), entry.getKey());
        }

        Set<Integer> distTopCategory = new HashSet<Integer>();

        for (Map.Entry<Integer, AmazonProduct> entry : AMAZON_PRODUCT_MAP.entrySet()) {
            distTopCategory.add(entry.getValue().getTopCategoryId());
        }

        Collection<AmazonProduct> amazonProductCollection = AMAZON_PRODUCT_MAP.values();

        for (Integer topCatId : distTopCategory) {
            Bag bag = CATEGORY_ITEM_BAG_MAP.get(topCatId) != null ? CATEGORY_ITEM_BAG_MAP.get(topCatId) : new Bag();
            for (AmazonProduct amazonProduct : amazonProductCollection) {
                if (amazonProduct.getTopCategoryId() == topCatId) {
                    if (topCatId == 11 && amazonProduct.getAvgRating() < 3.5f) {
                        // Make the high rated DC num of review lower rate
                        bag.addBalls(String.valueOf(amazonProduct.getId()), (long) ((amazonProduct.getNumOfReviews() + 50) * 2.2));
                    } else if (topCatId == 11 && amazonProduct.getAvgRating() < 5f && amazonProduct.getAvgRating() >= 3.5f) {
                        // Make the high rated DC num of good review lower rate
                        bag.addBalls(String.valueOf(amazonProduct.getId()), (amazonProduct.getNumOfReviews() + 50) * 2);
                    } else {
                        // normal item count based on num of reviews
                        bag.addBalls(String.valueOf(amazonProduct.getId()), amazonProduct.getNumOfReviews() + 30);
                    }
                }
            }
            CATEGORY_ITEM_BAG_MAP.put(topCatId, bag);
        }

        for (Map.Entry<String, State> entry : STATE_MAP.entrySet()) {
            STATE_POPULATION_BAG.addBalls(entry.getValue().getAbbreviation(), entry.getValue().getPopulation());
        }
        /*
        CATEGORY_ITEM_COUNT_BAG.addBalls("1", 358477);
        CATEGORY_ITEM_COUNT_BAG.addBalls("4", 92338);
        CATEGORY_ITEM_COUNT_BAG.addBalls("11", 88747);
        CATEGORY_ITEM_COUNT_BAG.addBalls("78", 26529);
        CATEGORY_ITEM_COUNT_BAG.addBalls("2", 23995);
        CATEGORY_ITEM_COUNT_BAG.addBalls("46", 1272);
        CATEGORY_ITEM_COUNT_BAG.addBalls("5", 969);
        CATEGORY_ITEM_COUNT_BAG.addBalls("41", 596);
        CATEGORY_ITEM_COUNT_BAG.addBalls("17", 590);
        CATEGORY_ITEM_COUNT_BAG.addBalls("95", 246);
        CATEGORY_ITEM_COUNT_BAG.addBalls("64", 178);
        CATEGORY_ITEM_COUNT_BAG.addBalls("24", 146);
        CATEGORY_ITEM_COUNT_BAG.addBalls("171", 134);
        CATEGORY_ITEM_COUNT_BAG.addBalls("117", 93);
        CATEGORY_ITEM_COUNT_BAG.addBalls("74", 80);
        CATEGORY_ITEM_COUNT_BAG.addBalls("3", 79);
        CATEGORY_ITEM_COUNT_BAG.addBalls("90", 78);
        CATEGORY_ITEM_COUNT_BAG.addBalls("75", 58);
        CATEGORY_ITEM_COUNT_BAG.addBalls("56", 28);
        CATEGORY_ITEM_COUNT_BAG.addBalls("69", 10);
        CATEGORY_ITEM_COUNT_BAG.addBalls("87", 9);
        CATEGORY_ITEM_COUNT_BAG.addBalls("60", 4);
        CATEGORY_ITEM_COUNT_BAG.addBalls("135", 3);
        CATEGORY_ITEM_COUNT_BAG.addBalls("149", 2);
        */
        CATEGORY_ITEM_COUNT_BAG.addBalls("1", 358477);
        CATEGORY_ITEM_COUNT_BAG.addBalls("4", 92338);
        CATEGORY_ITEM_COUNT_BAG.addBalls("11", 88747);
        CATEGORY_ITEM_COUNT_BAG.addBalls("78", 26529);
        CATEGORY_ITEM_COUNT_BAG.addBalls("2", 23995);
        CATEGORY_ITEM_COUNT_BAG.addBalls("46", 1272);
        CATEGORY_ITEM_COUNT_BAG.addBalls("5", 969);
        CATEGORY_ITEM_COUNT_BAG.addBalls("41", 596);
        CATEGORY_ITEM_COUNT_BAG.addBalls("17", 590);
        CATEGORY_ITEM_COUNT_BAG.addBalls("95", 246);
        CATEGORY_ITEM_COUNT_BAG.addBalls("64", 178);
        CATEGORY_ITEM_COUNT_BAG.addBalls("24", 146);
        CATEGORY_ITEM_COUNT_BAG.addBalls("171", 134);
        CATEGORY_ITEM_COUNT_BAG.addBalls("117", 93);
        CATEGORY_ITEM_COUNT_BAG.addBalls("74", 80);
        CATEGORY_ITEM_COUNT_BAG.addBalls("3", 79);
        CATEGORY_ITEM_COUNT_BAG.addBalls("90", 78);
        CATEGORY_ITEM_COUNT_BAG.addBalls("75", 58);
        CATEGORY_ITEM_COUNT_BAG.addBalls("56", 28);
        CATEGORY_ITEM_COUNT_BAG.addBalls("69", 10);
        CATEGORY_ITEM_COUNT_BAG.addBalls("87", 9);
        CATEGORY_ITEM_COUNT_BAG.addBalls("60", 4);
        CATEGORY_ITEM_COUNT_BAG.addBalls("135", 3);
        CATEGORY_ITEM_COUNT_BAG.addBalls("149", 2);

        QUANTITY_BAG.addBalls("1", 100);
        QUANTITY_BAG.addBalls("2", 500);
        QUANTITY_BAG.addBalls("3", 60);
        QUANTITY_BAG.addBalls("4", 70);
        QUANTITY_BAG.addBalls("5", 100);
        QUANTITY_BAG.addBalls("6", 100);
        QUANTITY_BAG.addBalls("7", 100);
        QUANTITY_BAG.addBalls("14", 2);
        QUANTITY_BAG.addBalls("15", 1);
        QUANTITY_BAG.addBalls("16", 1);

        SALES_TOTAL_BUCKET_BAG.addBalls("100", 2000);
        SALES_TOTAL_BUCKET_BAG.addBalls("200", 100);
        SALES_TOTAL_BUCKET_BAG.addBalls("300", 100);
        SALES_TOTAL_BUCKET_BAG.addBalls("400", 30);
        SALES_TOTAL_BUCKET_BAG.addBalls("500", 30);
        SALES_TOTAL_BUCKET_BAG.addBalls("600", 20);
        SALES_TOTAL_BUCKET_BAG.addBalls("700", 20);
        SALES_TOTAL_BUCKET_BAG.addBalls("800", 15);
        SALES_TOTAL_BUCKET_BAG.addBalls("900", 10);
        SALES_TOTAL_BUCKET_BAG.addBalls("1000", 4);
        SALES_TOTAL_BUCKET_BAG.addBalls("2000", 3);
        SALES_TOTAL_BUCKET_BAG.addBalls("3000", 2);
    }

    public static void main(String[] arg) throws Exception {
        String flagFoldername = com.sap.demo.pos.Utility.BASE_FOLDER + "data/posRows/";
        File flagFolderFile = new File(flagFoldername);
        if (!flagFolderFile.exists()) {
            flagFolderFile.mkdir();
        }

        Connection conn = DatabaseUtility.getConnection();
        while (START_CALENDAR.before(END_CALENDAR)) {
            String dateString = Utility.SIMPLE_DATE_FORMAT.format(START_CALENDAR.getTime());
            File flagFile = new File(flagFoldername + dateString + ".posRowFlag");
            if (!flagFile.exists()) {
                flagFile.createNewFile();
                System.out.println("Creating flag file: " + flagFile.getAbsolutePath());
                int dateId = DATE_STRING_ID_MAP.get(dateString);
                long start = System.currentTimeMillis();
                long txMax = com.sap.demo.Utility.getFuzzyNumber(MAX_TX_DOLLAR);
                System.out.println("1. txMax=" + txMax);
                txMax = (long) (txMax * MONTHLY_SALES_ADJ_FACTORS.get(START_CALENDAR.get(Calendar.MONTH)));
                System.out.println("2. txMax=" + txMax);
                txMax = (long) (txMax * DAY_OF_WEEK_FACTORS.get(START_CALENDAR.get(Calendar.DAY_OF_WEEK)));
                System.out.println("3. txMax=" + txMax);
                txMax = (long) (txMax * YEAR_ADJ_FACTOR.get(START_CALENDAR.get(Calendar.YEAR)));
                System.out.println("Working on " + dateString + ", txMax = " + txMax);
                Collection<PosRow> dailyPosRows = getDailyPosRow(dateId, txMax);
                //File outFile = new File(folder + dateString + ".csv");
                //FileOutputStream out = new FileOutputStream(outFile);
                //for (PosRow posRow: dailyPosRows) {
                //    IOUtils.write(posRow.toString(), out);
                //}
                System.out.println("Got " + dailyPosRows.size() + " posRows.");
                DatabaseUtility.insertPosRows(conn, dailyPosRows);
                System.out.println("Inserted into DB " + dailyPosRows.size() + " posRows.");
                long end = System.currentTimeMillis();
                System.out.println("Took " + ((end - start) / 1000) + " seconds...\n\n");
            }
            START_CALENDAR.add(Calendar.DATE, 1);
            //out.close();
            //break;
        }
        conn.close();
    }

    private static Collection<PosRow> getDailyPosRow(int dateId, long dailySalesAmount) {
        Collection<PosRow> posRows = new HashSet<PosRow>();
        int salesAmount = Integer.parseInt(SALES_TOTAL_BUCKET_BAG.drawBall());
        int fuzzySalesAmount = (int) (salesAmount * Utility.getFuzzyNumber((long) salesAmount));
        int currentSalesAmount = 0;
        while (currentSalesAmount < dailySalesAmount) {

            Collection<PosRow> transactionPosRows =
                    getTransactionPosRow(dateId,
                            Integer.parseInt(CATEGORY_ITEM_COUNT_BAG.drawBall()),
                            fuzzySalesAmount,
                            Integer.parseInt(QUANTITY_BAG.drawBall()));
            posRows.addAll(transactionPosRows);
            currentSalesAmount += getSalesAmount(transactionPosRows);
        }
        return posRows;
    }

    private static boolean hasErrorInPos(Collection<PosRow> posRows) {
        boolean hasError = false;
        Set<Integer> itemIds = new HashSet<Integer>();
        for (PosRow posRow : posRows) {
            if (!itemIds.contains(posRow.itemId)) {
                itemIds.add(posRow.itemId);
            } else {
                System.out.println("Found conflict pos rows: " + posRow.itemId);
                hasError = true;
                break;
            }
        }
        if (hasError) {
            int i = 0;
            for (PosRow posRow : posRows) {
                System.out.println(i + " -> " + posRow);
            }
            System.out.println("\n\n");
        }
        return hasError;
    }

    private static double getSalesAmount(Collection<PosRow> posRows) {
        double total = 0;
        for (PosRow posRow : posRows) {
            total += posRow.salesDollars;
        }
        return total;
    }

    private static Collection<PosRow> getTransactionPosRow(int dateId, int categoryId, int salesTotal, int itemCount) {
        String state = STATE_POPULATION_BAG.drawBall();
        List<Integer> locIds = STATE_LOCATION_ID_MAP.get(state);
        int locationId = locIds.get(RANDOM.nextInt(locIds.size()));
        long transactionNumber = getTransactionNumber();
        Map<Integer, PosRow> itemPosRow = new HashMap<Integer, PosRow>();
        int currentTotal = 0;
        int currentCount = 0;
        int sameCategoryCount = 0;
        while (currentTotal < salesTotal && currentCount < itemCount) {
            Bag itemBag = null;
            if (sameCategoryCount >= 2) {
                // choose random category since we have already selected the passed in category N times
                categoryId = Integer.parseInt(CATEGORY_ITEM_COUNT_BAG.drawBall());
            } else {
                sameCategoryCount++;
            }
            itemBag = CATEGORY_ITEM_BAG_MAP.get(categoryId);
            if (itemBag == null) {
                for (Map.Entry<Integer, Bag> entry : CATEGORY_ITEM_BAG_MAP.entrySet()) {
                    System.out.println(entry.getKey() + " = " + entry.getValue());
                }


                System.out.println("itemBag is null for....categoryId=" + categoryId);
            }
            int itemId = Integer.parseInt(itemBag.drawBall());
            AmazonProduct amazonProduct = AMAZON_PRODUCT_MAP.get(itemId);
            int qty = 1;
            if (amazonProduct.getPrice() <= 100 || RANDOM.nextInt(100) < 4) {
                qty = Integer.parseInt(QUANTITY_BAG.drawBall());
            }
            currentTotal += amazonProduct.getPrice() * (double) qty;
            currentCount += qty;
            PosRow nowPosRow = getPosRow(amazonProduct, qty, dateId, transactionNumber, locationId);
            if (!itemPosRow.containsKey(amazonProduct.getId())) {
                itemPosRow.put(nowPosRow.itemId, nowPosRow);
            } else {
                itemPosRow.put(nowPosRow.itemId, mergePosRow(itemPosRow.get(amazonProduct.getId()), nowPosRow));
            }
        }


        return itemPosRow.values();
    }

    private static PosRow mergePosRow(PosRow r1, PosRow r2) {
        r1.salesDollars += r2.salesDollars;
        r1.salesDollars += r2.salesDollars;
        r1.costDollars += r2.costDollars;
        r1.profitDollars += r2.profitDollars;
        return r1;
    }

    private static PosRow getPosRow(AmazonProduct amazonProduct, int qty, int dateId, long transactionNumber, int locId) {
        PosRow posRow = new PosRow();
        posRow.dateId = dateId;
        posRow.itemId = amazonProduct.getId();
        posRow.locationId = locId;
        posRow.transactionNumber = transactionNumber;
        posRow.salesQuantity = qty;
        posRow.salesDollars = (double) qty * amazonProduct.getPrice();
        posRow.costDollars = (double) qty * amazonProduct.getCost();
        posRow.profitDollars = posRow.salesDollars - posRow.costDollars;
        return posRow;
    }

    private static long getTransactionNumber() {
        return System.currentTimeMillis();
    }

    public static class PosRow {
        int dateId;
        int itemId;
        int locationId;
        int customerId;
        long transactionNumber;
        int salesQuantity;
        double salesDollars;
        double costDollars;
        double profitDollars;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(dateId).append(",");
            sb.append(itemId).append(",");
            sb.append(locationId).append(",");
            sb.append(customerId).append(",");
            sb.append(transactionNumber).append(",");
            sb.append(salesQuantity).append(",");
            sb.append(salesDollars).append(",");
            sb.append(costDollars).append(",");
            sb.append(profitDollars).append("\n");
            return sb.toString();
        }
    }


}
