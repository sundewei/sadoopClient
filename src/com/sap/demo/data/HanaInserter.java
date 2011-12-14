package com.sap.demo.data;

import com.sap.demo.Utility;
import org.apache.commons.csv.CSVUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/27/11
 * Time: 12:    46 PM
 * To change this template use File | Settings | File Templates.
 */
public class HanaInserter {


    public static Map<String, Integer> getBrandMap() {
        Map<String, Integer> brandMap = new HashMap<String, Integer>();

        brandMap.put("", 0);
        brandMap.put("Susan Wise Bauer", 1);
        brandMap.put("Unknown", 2);
        brandMap.put("Seventh Generation", 3);
        brandMap.put("Syd Logan", 4);
        brandMap.put("Culligan", 5);
        brandMap.put("Blu-ray ~ Johnny Depp", 6);
        brandMap.put("Danze", 7);
        brandMap.put("Safco", 8);
        brandMap.put("Wiha Tools", 9);
        brandMap.put("WeeRide", 10);
        brandMap.put("UBI Soft", 11);
        brandMap.put("eforcity", 12);
        brandMap.put("Bling Jewelry", 13);
        brandMap.put("FineDiamonds9", 14);
        brandMap.put("JVC", 15);
        brandMap.put("Coaster", 16);
        brandMap.put("Blu-ray ~ Samuel L. Jackson", 17);
        brandMap.put("Fiskars", 18);
        brandMap.put("Apple", 19);
        brandMap.put("Waterpik", 20);
        brandMap.put("Mongoose", 21);
        brandMap.put("Gigabyte", 22);
        brandMap.put("Furinno", 23);
        brandMap.put("Rasta Imposta", 24);
        brandMap.put("Adaptec", 25);
        brandMap.put("Newgy", 26);
        brandMap.put("SuperJeweler", 27);
        brandMap.put("Dan Gookin", 28);
        brandMap.put("Schwinn", 29);
        brandMap.put("Spalding", 30);
        brandMap.put("Philips", 31);
        brandMap.put("Amazon.com", 32);
        brandMap.put("DVD ~ Gabriel Byrne", 33);
        brandMap.put("Bazic", 34);
        brandMap.put("Doc Lew Childre", 35);
        brandMap.put("AmazonBasics", 36);
        brandMap.put("Michael Kerrisk", 37);
        brandMap.put("Bethesda", 38);
        brandMap.put("ToiletTree Products", 39);
        brandMap.put("SquareTrade Inc.", 40);
        brandMap.put("Electronic Arts", 41);
        brandMap.put("Tamiya America, Inc", 42);
        brandMap.put("Harvil", 43);
        brandMap.put("Arthur Griffith", 44);
        brandMap.put("Joola", 45);
        brandMap.put("Deluxe", 46);
        brandMap.put("Nancy Hall", 47);
        brandMap.put("Tamiya", 48);
        brandMap.put("Panasonic", 49);
        brandMap.put("Fat Brain Toys", 50);
        brandMap.put("Trail-A-Bike", 51);
        brandMap.put("Marware", 52);
        brandMap.put("Sony", 53);
        brandMap.put("Maxell", 54);
        brandMap.put("Pacific Cycle", 55);
        brandMap.put("PearlsOnly", 56);
        brandMap.put("BooginHead", 57);
        brandMap.put("Finejewelers", 58);
        brandMap.put("DVD ~ Larry David", 59);
        brandMap.put("Philips Avent", 60);
        brandMap.put("Brother", 61);
        brandMap.put("GMC", 62);
        brandMap.put("Mediabridge", 63);
        brandMap.put("Stiga", 64);
        brandMap.put("Transcend", 65);
        brandMap.put("California Costumes", 66);
        brandMap.put("GOGO", 67);
        brandMap.put("Lifetime", 68);
        brandMap.put("Melissa & Doug", 69);
        brandMap.put("SUCKUK", 70);
        brandMap.put("Namco", 71);
        brandMap.put("Canon", 72);
        brandMap.put("Diggin", 73);
        brandMap.put("Synology", 74);
        brandMap.put("Carrom", 75);
        brandMap.put("Coast Innovations", 76);
        brandMap.put("Ipong", 77);
        brandMap.put("HP", 78);
        brandMap.put("Morphsuits", 79);
        brandMap.put("Nikon", 80);
        brandMap.put("JLab Audio", 81);
        brandMap.put("Amazon.com Collection", 82);
        brandMap.put("Donna Martin", 83);
        brandMap.put("Huffy", 84);
        brandMap.put("Haribo", 85);
        brandMap.put("Mediasonic", 86);
        brandMap.put("Huggies", 87);
        brandMap.put("Butterfly", 88);
        brandMap.put("Asus", 89);
        brandMap.put("Hasbro", 90);
        brandMap.put("Rolodex", 91);
        brandMap.put("by Forum Novelties Inc.", 92);
        brandMap.put("Voit", 93);
        brandMap.put("Taymor Industries", 94);
        brandMap.put("Andrew Krause", 95);
        brandMap.put("Mattel", 96);
        brandMap.put("PrimaCare Medical Supplies", 97);
        brandMap.put("AMD", 98);
        brandMap.put("Harvard", 99);
        brandMap.put("Pampers", 100);
        brandMap.put("Grohe", 101);
        brandMap.put("Core Knowledge Foundation", 102);
        brandMap.put("Warner Bros", 103);
        brandMap.put("Neil Matthew", 104);
        brandMap.put("Greg Tang", 105);
        brandMap.put("Lil Characters", 106);
        brandMap.put("InterDesign", 107);
        brandMap.put("Syba", 108);
        brandMap.put("Dzherelo: Wikipedia", 109);
        brandMap.put("Capcom", 110);
        brandMap.put("Ally Condie", 111);
        brandMap.put("Playcraft", 112);
        brandMap.put("Samsung", 113);
        brandMap.put("Baden", 114);
        brandMap.put("Kingston", 115);
        brandMap.put("Corsair", 116);
        brandMap.put("Amazon Digital Services", 117);
        brandMap.put("Hipstreet", 118);
        brandMap.put("Ace For Men", 119);
        brandMap.put("Franklin", 120);
        brandMap.put("MPI", 121);
        brandMap.put("Western Digital", 122);
        brandMap.put("Generic", 123);
        brandMap.put("Netaya", 124);
        brandMap.put("Amazon", 125);
        brandMap.put("LG", 126);
        brandMap.put("Importer520", 127);
        brandMap.put("Killerspin", 128);
        brandMap.put("Chef Works", 129);
        return brandMap;
    }

    public static void main(String[] arg) throws Exception {
        //Connection conn = getConnection();
        //List<String> dateLines = FileUtils.readLines(new File("C:\\projects\\data\\dimension\\date.csv"));
        //List<String> storeLines = FileUtils.readLines(new File("C:\\projects\\data\\dimension\\store.csv"));
        //Utility.insert(conn, "STORE_DIM", storeLines);
        //Utility.insert(conn, "DATE_DIM", dateLines);

        //insertStores();

        //insertLocations();

        insertIpLocations();
    }

    public static void insertItems() throws Exception {
        Connection conn = Utility.getConnection();
        PreparedStatement stmt = conn.prepareStatement("insert into I827779.ITEM_DIM values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        Map<String, Integer> catMap = HtmlPageParser.getReverseCategoryMap();
        List<HtmlPageParser.HtmlPageInfo> pages = HtmlPageParser.getHtmlPageInfoList();

        /*
        CREATE TABLE ITEM_DIM (
            ID VARCHAR(20) PRIMARY KEY,
            CATEGORY INTEGER,
            SUB_CATEGORY INTEGER,
            DESCRIPTION VARCHAR(300),
            UPC CHAR(10),
            BRAND INTEGER,
            UNIT_COST NUMBER NOT NULL,
            UNIT_PRICE NUMBER NOT NULL,
            STATUS INTEGER NOT NULL,
            LAST_MODE_DATE DATE,
            ADDED_DATE DATE)
         */


        for (HtmlPageParser.HtmlPageInfo page : pages) {
            Map<String, String> metaMap = HtmlPageParser.getInformationMap(page.content);
            int categoryId = catMap.get(HtmlPageParser.getCategory(metaMap.get("description")));
            String category = HtmlPageParser.getCategory(metaMap.get("description"));
            System.out.println("Working on " + page.productId);
            stmt.setString(1, page.productId);
            stmt.setInt(2, categoryId);
            stmt.setObject(3, null);
            stmt.setString(4, metaMap.get("description"));
            System.out.println("Utility.getPaddedNumberString(page.productId.hashCode(), 10, \"0\")=" + Utility.getPaddedNumberString(Math.abs(page.productId.hashCode()), 10, "0"));
            stmt.setString(5, Utility.getPaddedNumberString(Math.abs(page.productId.hashCode()), 10, "0"));
            stmt.setInt(6, getBrandMap().get(metaMap.get("brand")));
            System.out.println("price:" + Double.parseDouble(metaMap.get("price")));
            stmt.setString(7, metaMap.get("price"));
            stmt.setDouble(8, 0d);
            stmt.setInt(9, 0);
            stmt.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
            stmt.setTimestamp(11, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
        }

        stmt.close();
        conn.close();

    }

    public static void insertBrands() throws Exception {
        Connection conn = Utility.getConnection();
        PreparedStatement stmt = conn.prepareStatement("insert into I827779.BRAND values(?, ?, ?)");
        Map<String, Integer> catMap = HtmlPageParser.getReverseCategoryMap();
        List<HtmlPageParser.HtmlPageInfo> pages = HtmlPageParser.getHtmlPageInfoList();

        /*
        CREATE TABLE ITEM_DIM (
            ID VARCHAR(20) PRIMARY KEY,
            CATEGORY INTEGER,
            SUB_CATEGORY INTEGER,
            DESCRIPTION VARCHAR(300),
            UPC CHAR(10),
            BRAND INTEGER,
            UNIT_COST NUMBER NOT NULL,
            UNIT_PRICE NUMBER NOT NULL,
            STATUS INTEGER NOT NULL,
            LAST_MODE_DATE DATE,
            ADDED_DATE DATE)
         */

        /*
        for (HtmlPageParser.HtmlPageInfo page: pages) {
            Map<String, String> metaMap = HtmlPageParser.getInformationMap(page.content);
            int categoryId = catMap.get(HtmlPageParser.getCategory(metaMap.get("description")));
            String category = HtmlPageParser.getCategory(metaMap.get("description"));
            stmt.setString(1, page.itemLookup);
            stmt.setInt(2, categoryId);
            stmt.setObject(3, null);
            stmt.setString(4, metaMap.get("description"));
            stmt.setString(5, Utility.getPaddedNumberString(page.itemLookup.hashCode(), 10, "0"));
            stmt.setString(6, metaMap.get());

        }
        */
        Set<String> brands = new HashSet<String>();
        Map<String, String> brandTypes = new HashMap<String, String>();
        int brandId = 0;
        for (HtmlPageParser.HtmlPageInfo page : pages) {
            Map<String, String> metaMap = HtmlPageParser.getInformationMap(page.content);
            if (!brands.contains(metaMap.get("brand"))) {
                brands.add(metaMap.get("brand"));
                int categoryId = catMap.get(HtmlPageParser.getCategory(metaMap.get("description")));
                if (categoryId == 5 || categoryId == 14 || categoryId == 16 || categoryId == 22) {
                    brandTypes.put(metaMap.get("brand"), "WAITING");
                } else {
                    brandTypes.put(metaMap.get("brand"), "Manufacturer");
                }
            }
            //System.out.println(page.itemLookup + " : " + metaMap.get("merchant"));
        }

        for (String brand : brands) {
            stmt.setInt(1, brandId);
            stmt.setString(2, brand);
            stmt.setString(3, brandTypes.get(brand));
            stmt.execute();
            brandId++;
        }
        stmt.close();
        conn.close();

    }

    public static void insertStores() throws Exception {
        List<String> stores = FileUtils.readLines(new File("C:\\projects\\data\\store\\store_dim.csv"));

        Map<String, String> abbState = Utility.getStateAbbrFullMap();

        List<String> newStores = new ArrayList<String>();
        boolean skipFirst = false;
        for (String store : stores) {
            if (skipFirst) {
                String[] storeValues = CSVUtils.parseLine(store);
                String stateAbb = storeValues[9];
                String fullStateName = abbState.get(stateAbb);
                String region = Utility.STATE_REGION.get(fullStateName);
                store = store + ", \"" + region + "\"";
            } else {
                store = store + ", \"REGION\"";
                skipFirst = true;
            }
            newStores.add(store);
        }

        Connection conn = Utility.getConnection();
        Utility.insert(conn, "STORE_DIM", newStores);
        conn.close();
    }

    public static void insertLocations() throws Exception {
        Map<Integer, LocationParser.Location> locationMap = LocationParser.getLocations();
        Connection conn = Utility.getConnection();
        conn.setAutoCommit(false);
        String query = "insert into SPORTMART.DIM_LOCATIONS values(?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        int count = 0;
        for (LocationParser.Location location : locationMap.values()) {
            stmt.setInt(1, location.locId);
            stmt.setString(2, location.country);
            stmt.setString(3, location.city);
            if (location.postalCode != null) {
                stmt.setString(4, location.postalCode);
            } else {
                stmt.setObject(4, null);
            }
            stmt.setDouble(5, location.latitude);
            stmt.setDouble(6, location.longitude);

            if (location.metroCode != LocationParser.DOUBLE_NULL) {
                stmt.setInt(7, location.metroCode);
            } else {
                stmt.setObject(7, null);
            }

            if (location.areaCode != LocationParser.INT_NULL) {
                stmt.setInt(8, location.areaCode);
            } else {
                stmt.setObject(8, null);
            }

            if (location.provinceName != null) {
                stmt.setString(9, location.provinceName);
            } else {
                stmt.setObject(9, null);
            }

            if (location.metroName != null) {
                stmt.setString(10, location.metroName);
            } else {
                stmt.setObject(10, null);
            }

            stmt.addBatch();
            count++;
            if (count % 800 == 0) {
                stmt.executeBatch();
                conn.commit();
                System.out.println("Inserting " + count);
            }
        }
        if (count % 800 != 0) {
            stmt.executeBatch();
            System.out.println("Inserting final " + count);
        }
        conn.commit();
        stmt.close();
        conn.close();
    }

    public static void insertIpLocations() throws Exception {
        List<String> ipBlocks = FileUtils.readLines(new File("C:\\projects\\data\\GeoLiteCity_20111004\\GeoLiteCity-Blocks.csv"));
        Connection conn = Utility.getConnection();
        conn.setAutoCommit(false);
        String query = "insert into SPORTMART.IP_LOCATIONS values(?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        int count = 0;
        for (int i = 2; i < ipBlocks.size(); i++) {
            String[] ipLocValues = CSVUtils.parseLine(ipBlocks.get(i));
            stmt.setString(1, ipLocValues[0]);
            stmt.setString(2, ipLocValues[1]);
            stmt.setString(3, ipLocValues[2]);
            stmt.addBatch();
            count++;
            if (count % 10000 == 0) {
                stmt.executeBatch();
                System.out.println(count + " rows inserted...");
            }
        }
        if (count % 10000 != 0) {
            stmt.executeBatch();
            System.out.println("Last Batch");
        }
        conn.commit();
        stmt.close();
        conn.close();
    }


}
