package com.sap.demo;

import com.sap.demo.dao.*;
import com.sap.hadoop.conf.IFileSystem;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/18/11
 * Time: 4:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class Utility {
    public static Random RANDOM = new Random();
    private static final Logger LOG = Logger.getLogger(Utility.class.getName());
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss Z]");
    public static final DateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    public static final DateFormat SIMPLE_MONTH_FORMAT = new SimpleDateFormat("yyyy-MM");
    public static final DateFormat YYYY_MM_DATE_FORMAT = new SimpleDateFormat("yyyy-MM");
    public static final DateFormat MEDIUM_DATE_FORMAT = new SimpleDateFormat("MMMMM d, yyyy");
    public static final DecimalFormat MONEY_FORMATTER = new DecimalFormat("###.##");
    public static Map<String, String> STATE_REGION = new HashMap<String, String>();
    static String[] STATES;
    public static Map<String, String> STATE_ABBREVIATION = new HashMap<String, String>();
    public static Map<String, String> ABBREVIATION_STATE = new HashMap<String, String>();

    public static void writeTransactions(Collection<PosRowGenerator.Transaction> txs, String filename) throws IOException {
        StringBuilder sb = new StringBuilder();
        int txid = 0;
        BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true));
        for (PosRowGenerator.Transaction tx : txs) {
//System.out.println(tx);
            String storeId = tx.store.id;
            for (Map.Entry<Item, Integer> entry : tx.itemCountMap.entrySet()) {
                sb.append("\"").append(String.valueOf(txid)).append("\",");
                sb.append("\"").append(storeId).append("\",");
                sb.append(entry.getKey().toCsvString()).append(",");
                sb.append("\"").append(entry.getValue()).append("\",");
                sb.append("\n");
            }
            txid++;
            if (txid % 100 == 0) {
                IOUtils.write(sb.toString(), writer);
                sb = new StringBuilder();
            }
//System.out.println(sb.toString() + "\n-----------------\n");
        }
        if (sb.length() > 0) {
            // The remaining lines
            IOUtils.write(sb.toString(), writer);
        }
        writer.flush();
        writer.close();
    }

    public static void close(PreparedStatement pstmt) throws SQLException {
        if (pstmt != null) {
            pstmt.close();
        }
    }

    public static void close(ResultSet rs) throws SQLException {
        if (rs != null) {
            rs.close();
        }
    }

    public static void close(Connection conn) throws Exception {
        if (conn != null) {
            conn.clearWarnings();
            conn.close();
        }
    }

    public static int getInt(PreparedStatement pstmt) throws Exception {
        ResultSet rs = pstmt.executeQuery();
        int count = 0;
        while (rs.next()) {
            count = rs.getInt(1);
        }
        close(rs);
        return count;
    }

    public static String getCsvString(String[] values, char delimiter) {
        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            sb.append(value).append(delimiter);
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }


    public static Map<String, String> getStateAbbrFullMap() throws Exception {
        List<String> states = FileUtils.readLines(new File(com.sap.demo.pos.Utility.BASE_FOLDER + "data/state/states.csv"));
        Map<String, String> abbState = new HashMap<String, String>();
        for (String state : states) {
            String[] ss = CSVUtils.parseLine(state);
            abbState.put(ss[1], ss[0]);
        }
        return abbState;
    }

    public static void dumpFactPos(String[] arg) throws Exception {
        if (arg[0].equals("1")) {
            List<Item> items = getItems(arg[1]);
            Connection conn = getConnection();
            int itemCount = 0;
            for (Item item : items) {
                String filename = "F:\\sportmart\\tableCsv\\fact_pos\\fewer_col_item_" + Utility.getPaddedNumberString(item.getItemLookup(), 10, "0") + ".csv";
                try {
//filename = "F:\\sportmart\\tableCsv\\fact_pos\\fewer_col_item_"+Utility.getPaddedNumberString(240407, 10, "0")+".csv";
//generateFactPosCsv(conn, 240407, filename);
                    generateFactPosCsv(conn, item.getItemLookup(), filename);
//break;
                } catch (Exception e) {
                    e.printStackTrace();
                    FileUtils.write(new File(filename + ".exception"), e.getMessage());
                }
                itemCount++;
                System.out.println(itemCount + "/" + items.size());
            }
            conn.close();
        } else if (arg[0].equals("2")) {
            generateItemQty();
        } else {
            String folderName = "F:\\sportmart\\tableCsv\\fact_pos\\";
            //String folderName = "c:\\test\\";
            File folder = new File(folderName);
            File[] files = folder.listFiles();
            File newFile = new File(folderName + "data.csv");
            BufferedWriter out = new BufferedWriter(new FileWriter(newFile, true));
            int count = 0;
            for (File file : files) {
                System.out.println(count + "/" + files.length + ", Working on " + file.getName());
                if (file.length() > 0) {
                    long start = System.currentTimeMillis();
                    IOUtils.copy(new FileReader(file), out);
                    out.write("\n");
                    out.flush();
                    long end = System.currentTimeMillis();
                    System.out.println("Appending took: " + ((end - start) / 1000) + " seconds to finish...\n");
                } else {
                    System.out.println(file.getName() + " is " + file.length() + "bytes \n");
                }
                count++;
            }
            out.close();
        }
    }


    public static void generateFactPosCsv(Connection conn, int itemLookup, String filename) throws Exception {

        String query = "SELECT CALENDAR_DATE,\n" +
                "       ITEM_LOOKUP,\n" +
                "       STORE_NUM,\n" +
                "       RETAILER_CD,\n" +
                "       UNIT_RETAIL,\n" +
                "       UNIT_COST,\n" +
                "       SOLD_QTY,\n" +
                "       SOLD_DOLLARS,\n" +
                "       ONHAND_QTY,\n" +
                "       ONORDER_QTY,\n" +
                "       TOTAL_QTY,\n" +
                "       ONHAND_DOLLARS,\n" +
                "       ONORDER_DOLLARS,\n" +
                "       INVENTORY_TOTAL_DOLLARS,\n" +
                "       TRAITED,\n" +
                "       VALID\n" +
                "FROM   SPORTMART.FACT_POS \n" +
                "WHERE ITEM_LOOKUP = ?";

        /*
        String query =  "SELECT CALENDAR_DATE,\n"+
                        "       INVENTORY_TOTAL_DOLLARS,\n"+
                        "       VALID\n" +
                        "FROM   SPORTMART.FACT_POS \n" +
                        "WHERE ITEM_LOOKUP = ?";
        */

        File csvFile = new File(filename);
        if (!csvFile.exists()) {
            PreparedStatement pstmt = conn.prepareStatement(query);
            pstmt.setInt(1, itemLookup);
            System.out.println("About to run query for item ( " + itemLookup + " ): \n" + query);
            long start = System.currentTimeMillis();
            ResultSet rs = pstmt.executeQuery();
            ResultSetMetaData rsMeta = rs.getMetaData();
//System.out.println("rsMeta.getColumnCount() = " + rsMeta.getColumnCount());
//for (int i=1; i<=rsMeta.getColumnCount(); i++) {
//    System.out.println("i=" + i + ", " + rsMeta.getColumnName(i));
//}
            long end = System.currentTimeMillis();
            System.out.println("Query took: " + ((end - start) / 1000) + " seconds to finish...");

            System.out.println("Writing to " + filename);
            BufferedWriter out = new BufferedWriter(new FileWriter(filename));
            StringBuilder sb = new StringBuilder();
            int rowCount = 0;
            while (rs.next()) {
                for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                    sb.append("\"");
                    if (i == 1) {
                        sb.append(DATE_FORMAT.format(rs.getDate(i)));
                    } else {
                        sb.append(rs.getString(i));
                    }
                    sb.append("\"");
                    if (i == rsMeta.getColumnCount()) {
                        sb.append("\n");
                    } else {
                        sb.append(",");
                    }
                }
                rowCount++;
                if (rowCount % 100000 == 0) {
                    out.write(sb.toString());
                    System.out.println("rowCount = " + rowCount);
                    sb = new StringBuilder();
                }
            }
            if (sb.length() > 0) {
                out.write(sb.toString());
                System.out.println("Last batch...rowCount = " + rowCount);
            }
            sb = new StringBuilder();
            rs.close();
            pstmt.close();
            out.flush();
            out.close();
            conn.clearWarnings();
        }
    }

    public static void generateItemQty() throws Exception {
        /*
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        String url = "jdbc:sap://llnpal056:35015";
        Connection conn = DriverManager.getConnection(url, "SYSTEM", "Hadoophana123");
        System.out.println(conn.getMetaData().getDatabaseProductName());
        System.out.println(conn.getMetaData().getDatabaseProductVersion());
        */
        //System.out.println(getIpGeoNum("173.164.241.97"));
        /*
        BufferedReader br =
                new BufferedReader(new InputStreamReader(new FileInputStream("C:\\projects\\data\\newLogsHome\\localhost_access_log.2008-01-01.txt")));
        String line = br.readLine();
        Set<String> set = new HashSet<String>();
        while (line != null) {
            AccessEntry ae = getAccessEntry(line);
            String pid = getItemAsin(ae.resource);
            if(pid != null) {
                set.add(ae.resource.substring(0, ae.resource.indexOf("/", 2)));
                if (ae.resource.substring(0, ae.resource.indexOf("/", 2)).equals("/dp")) {
                    System.out.println(ae.resource);
                }
                //System.out.println("pid=" + pid + ", ae.resource=" + ae.resource);
            }
            line = br.readLine();
        }
        br.close();

        for (String re: set) {
            System.out.println(re);
        }
        */
        Set<Integer> storeNums = getStoreNums();
        Connection conn = Utility.getConnection();
        int count = 0;
        for (Integer storeNum : storeNums) {
            String filename = "F:\\sportmart\\storeItemQty\\" + storeNum + ".csv";
            File file = new File(filename);
            System.out.println(count + "/" + storeNums.size());
            if (file.exists()) {
                System.out.println("Skipping " + filename);
            } else {
                System.out.println("Working on " + filename);
                dumpItemQty(conn, storeNum, filename);
            }
            count++;
        }
        conn.close();
    }

    public static void insertTransactions(Connection conn, Collection<PosRowGenerator.Transaction> transactions) throws Exception {
        /*
        CREATE TABLE POS_FACT (
          date_id INT NOT NULL,
          item_id INT NOT NULL,
          store_id INT NOT NULL,
          promotion_id INT,
          customer_id INT,
          transaction_number BIGINT NOT NULL,
          sales_quantity INT NOT NULL,
          sales_dollars double NOT NULL,
          cost_dollars double NOT NULL,
          profit_dollars double NOT NULL
        )
         */
        String query = " INSERT INTO HADOOP.POS_FACT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
        PreparedStatement stmt = conn.prepareStatement(query);
        int rowCount = 0;
        boolean started = false;
        for (PosRowGenerator.Transaction tx : transactions) {
            for (Map.Entry<Item, Integer> entry : tx.itemCountMap.entrySet()) {
                Item item = entry.getKey();
                int itemQuantity = entry.getValue();
                stmt.setInt(1, tx.dateId);
                stmt.setString(2, item.getColumn("ID"));
                stmt.setString(3, tx.store.id);
                stmt.setObject(4, null);
                stmt.setObject(5, null);
                stmt.setLong(6, System.currentTimeMillis());
                stmt.setInt(7, itemQuantity);
                double price = Double.parseDouble(item.getColumn("UNIT_PRICE"));
                double cost = Double.parseDouble(item.getColumn("UNIT_COST"));
                double revenue = (price - cost) * itemQuantity;
                stmt.setString(8, Utility.MONEY_FORMATTER.format(price));
                stmt.setString(9, Utility.MONEY_FORMATTER.format(cost));
                stmt.setString(10, Utility.MONEY_FORMATTER.format(revenue));
                rowCount++;
            }
            if (rowCount > 10000) {
                if (!started) {
                    System.out.println("Start inserting...");
                    started = true;
                }
                stmt.executeBatch();
                rowCount = 0;
            }
            stmt.addBatch();
        }

        if (rowCount > 0) {
            System.out.println("Last batch...");
            stmt.executeBatch();
        }
        stmt.close();
    }

    public static int getFuzzyNumber(int base) {
        return getFuzzyNumber(base, 1);
    }

    public static long getFuzzyNumber(long base) {
        return getFuzzyNumber(base, (long) (base * 0.92), (long) (base * 1.08));
    }

    public static long getFuzzyNumber(long base, long width) {
        long lowerMin = base - width;
        return lowerMin + nextLong(2 * width);
    }

    public static long getFuzzyNumber(long base, long small, long big) {
        long a = nextLong(small);
        long b = big;
        long c = (small / 2 + big);
        //System.out.println("a="+a);
        //System.out.println("b="+b);
        //System.out.println("c="+c);
        //System.out.println("((double)(a + b) / c)="+((double)(a + b) / c));

        return (long) (base * ((double) (a + b) / c));
    }

    public static int getFuzzyNumber(int base, int min) {
        int value = 0;
        while (value < min) {
            value = (int) (base * 2 * RANDOM.nextGaussian());
        }
        return value;
    }

    public static Map<String, List<String>> getBrowserMap() throws Exception {
        File folder = new File(com.sap.demo.pos.Utility.BASE_FOLDER + "data/browsers");
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        for (File file : folder.listFiles()) {
            String browserType = file.getName().replace(".txt", "");
            map.put(browserType, FileUtils.readLines(file));
        }
        return map;
    }

    public static AccessEntry getAccessEntry(String line) {
        // An AccessData object will be created for each line if possible
        AccessEntry accessEntry = null;
        try {
            accessEntry = new AccessEntry();
            // Parse the value separated line using space as the delimiter
            CSVParser csvParser = new CSVParser(new StringReader(line));
            csvParser.getStrategy().setDelimiter(' ');

            // Now get all the values from the line
            String[] values = csvParser.getLine();

            // Get the IP
            accessEntry.ip = values[0];

            // The time is split into 2 values so they have to be combined
            // then sent to match the time regular expression
            // "[02/Aug/2011:00:00:04" + " -0700]" = "[02/Aug/2011:00:00:04 -0700]"
            accessEntry.timestamp = new Timestamp(DATE_FORMAT.parse(values[3] + " " + values[4]).getTime());

            // The resource filed has 3 fields (HTTP Method, Page and HTTP protocol)
            // so it has to be further split by spaces
            String reqInfo = values[5];
            String[] reqInfoArr = reqInfo.split(" ");

            // Get the HTTP method
            accessEntry.method = reqInfoArr[0];

            // Get the page requested
            accessEntry.resource = reqInfoArr[1];

            // Get the HTTP response code
            accessEntry.httpCode = Integer.parseInt(values[6]);

            // Try to get the response data size in bytes, if a hyphen shows up,
            // that means the client has a cache of this page and no data is
            // sent back
            try {
                accessEntry.dataLength = Long.parseLong(values[7]);
            } catch (NumberFormatException nfe) {
                accessEntry.dataLength = 0;
            }

            if (values.length >= 9) {
                accessEntry.referrer = values[8];
            }

            if (values.length >= 10) {
                accessEntry.userAgent = values[9];
            }

            return accessEntry;
        } catch (IOException ioe) {
            LOG.info(ioe);
            return null;
        } catch (ParseException pe) {
            LOG.info(pe);
            return null;
        } catch (NumberFormatException nfe) {
            LOG.info(nfe);
            return null;
        }
    }

    /*
fcrawler.looksmart.com - - [26/Apr/2000:00:00:12 -0400] "GET /contacts.html HTTP/1.0" 200 4595 "-" "FAST-WebCrawler/2.1-pre2 (ashen@looksmart.net)"
fcrawler.looksmart.com - - [26/Apr/2000:00:17:19 -0400] "GET /news/news.html HTTP/1.0" 200 16716 "-" "FAST-WebCrawler/2.1-pre2 (ashen@looksmart.net)"

ppp931.on.bellglobal.com - - [26/Apr/2000:00:16:12 -0400] "GET /download/windows/asctab31.zip HTTP/1.0" 200 1540096 "http://www.htmlgoodies.com/downloads/freeware/webdevelopment/15.html" "Mozilla/4.7 [en]C-SYMPA  (Win95; U)"

123.123.123.123 - - [26/Apr/2000:00:23:48 -0400] "GET /pics/wpaper.gif HTTP/1.0" 200 6248 "http://www.jafsoft.com/asctortf/" "Mozilla/4.05 (Macintosh; I; PPC)"
123.123.123.123 - - [26/Apr/2000:00:23:47 -0400] "GET /asctortf/ HTTP/1.0" 200 8130 "http://search.netscape.com/Computers/Data_Formats/Document/Text/RTF" "Mozilla/4.05 (Macintosh; I; PPC)"
123.123.123.123 - - [26/Apr/2000:00:23:48 -0400] "GET /pics/5star2000.gif HTTP/1.0" 200 4005 "http://www.jafsoft.com/asctortf/" "Mozilla/4.05 (Macintosh; I; PPC)"
123.123.123.123 - - [26/Apr/2000:00:23:50 -0400] "GET /pics/5star.gif HTTP/1.0" 200 1031 "http://www.jafsoft.com/asctortf/" "Mozilla/4.05 (Macintosh; I; PPC)"
123.123.123.123 - - [26/Apr/2000:00:23:51 -0400] "GET /pics/a2hlogo.jpg HTTP/1.0" 200 4282 "http://www.jafsoft.com/asctortf/" "Mozilla/4.05 (Macintosh; I; PPC)"
123.123.123.123 - - [26/Apr/2000:00:23:51 -0400] "GET /cgi-bin/newcount?jafsof3&width=4&font=digital&noshow HTTP/1.0" 200 36 "http://www.jafsoft.com/asctortf/" "Mozilla/4.05 (Macintosh; I; PPC)"
     */

    public static void initOutputPath(IFileSystem fileSystem, String outPath) throws Exception {
        if (fileSystem.exists(outPath)) {
            fileSystem.deleteDirectory(outPath);
        }
    }

    public static void initOutputPath(FileSystem fileSystem, String outPath) throws Exception {
        Path path = new Path(outPath);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    public static List<String> getStringList(Configuration conf, String prefix) {
        List<String> list = new ArrayList<String>();
        String string = conf.get(prefix + "_0");
        int i = 1;
        while (string != null) {
            list.add(string);
            string = conf.get(prefix + "_" + i);
            i++;
        }
        return list;
    }

    public static int getIndex(String column, List<String> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (column.equalsIgnoreCase(columns.get(i))) {
                return i;
            }
        }
        return -1;
    }

    public static List<Header> getHeaders(File filename) throws Exception {
        List<Header> headers = new ArrayList<Header>();
        List<String> lines = FileUtils.readLines(filename);
        List<String> lineList = new ArrayList<String>();
        Header header = new Header();
        for (String line : lines) {
            lineList.add(line);
            if (line.equals("----------------------------------------------------------")) {
                if (lineList.get(0).contains("amazon.com/")) {
                    header.init(lineList);
                    headers.add(header);
                }
                header = new Header();
                lineList = new ArrayList<String>();
            }
        }
        return headers;
    }

    public static String getPaddedNumberString(int num, int length, String padChar) {
        StringBuilder sb = new StringBuilder(String.valueOf(num));
        while (sb.length() < length) {
            sb.insert(0, padChar);
        }

        while (sb.length() > length) {
            sb.deleteCharAt(0);
        }

        return sb.toString();
    }

    public static String getPaddedNumberString(String source, int length, String padChar) {
        StringBuilder sb = new StringBuilder(source);
        while (sb.length() < length) {
            sb.insert(0, padChar);
        }
        return sb.toString();
    }

    public static long nextLong(long n) {
        // error checking and 2^x checking removed for simplicity.
        long bits, val;
        do {
            bits = (RANDOM.nextLong() << 1) >>> 1;
            val = bits % n;
        } while (bits - val + (n - 1) < 0L);
        return val;
    }

    public static long getIpGeoNum(String ip) throws IllegalArgumentException {
        String[] ipWxyz = ip.split("\\.");
        if (ipWxyz.length != 4) {
            throw new IllegalArgumentException(ip + " is not a valid IP address.");
        }
        long ipNum = 0;
        return 16777216 * Long.parseLong(ipWxyz[0]) + 65536 * Long.parseLong(ipWxyz[1]) +
                256 * Long.parseLong(ipWxyz[2]) + Long.parseLong(ipWxyz[3]);
    }

    public static long[] getIntervalMs(long baseMs, int size) {
        int min15 = 900000;
        int chance = RANDOM.nextInt(100);
        long startMs = 0;
        long endMs = 0;
        if (chance < 80) {
            // within 30 mins
            startMs = baseMs - RANDOM.nextInt(min15);
            endMs = baseMs + RANDOM.nextInt(min15);
        } else {
            // within 4 hours
            startMs = baseMs - RANDOM.nextInt(min15 * RANDOM.nextInt(8));
            endMs = baseMs + RANDOM.nextInt(min15 * RANDOM.nextInt(8));
        }
        Set<Long> neededMs = new HashSet<Long>();

        while (neededMs.size() < size) {
            neededMs.add(startMs + nextLong(endMs - startMs));
        }
        System.out.println("neededMs.size()=" + neededMs.size());
        long[] arr = new long[neededMs.size()];
        int i = 0;
        Iterator<Long> it = neededMs.iterator();
        while (it.hasNext()) {
            arr[i] = it.next();
            i++;
        }
        return arr;
    }

    public static String getAccessLogLine(String ip, Header header, long ms) {
        StringBuilder sb = new StringBuilder();
        sb.append(ip).append(" - - ");
        sb.append(DATE_FORMAT.format(new Timestamp(ms))).append(" ");
        // line :
        // "GET /Sony-BRAVIA-KDL-46V5100-46-Inch-1080p/dp/B001T9N0EO/ HTTP/1.1"
        sb.append("\"");
        sb.append(header.method).append(" ");
        sb.append(header.resource).append(" ");
        sb.append(header.http);
        sb.append("\" ");
        sb.append(header.httpStatusCode).append(" ");
        if (header.reqMap.get("Referer") != null) {
            sb.append("\"");
            sb.append(header.reqMap.get("Referer"));
            sb.append("\"");
            sb.append(" ");
        } else {
            sb.append("-").append(" ");
        }
        if (header.reqMap.get("User-Agent") != null) {
            sb.append("\"");
            sb.append(header.reqMap.get("User-Agent"));
            sb.append("\"");
            sb.append(" ");
        } else {
            sb.append("-").append(" ");
        }
        return sb.toString();
    }

    public static int[] getRangeStartEnd(int length) {
        while (true) {
            int a = RANDOM.nextInt(length);
            int b = RANDOM.nextInt(length);
            if (Math.abs(a - b) <= 15 && Math.abs(a - b) >= 2) {
                return new int[]{Math.min(a, b), Math.max(a, b)};
            }
        }
    }

    public static void insert(Connection conn, String tableName, List<String> lines) throws Exception {
        boolean skipFirst = false;
        PreparedStatement stmt = null;
        String query = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");

        for (String line : lines) {
            System.out.println("line=" + line);
            String[] values = CSVUtils.parseLine(line);
            if (!skipFirst) {
                skipFirst = true;
                query = getQuery(tableName, values.length);
                System.out.println("query = \n" + query);
                stmt = conn.prepareStatement(query);
            } else {
                for (int i = 0; i < values.length; i++) {
                    if (i == 16) {
                        stmt.setString(14, values[i]);
                    } else if (i >= 13 && i <= 15) {
                        if (!values[i].equals("null")) {
                            Date date = sdf.parse(values[i]);
                            stmt.setTimestamp(i + 2, new Timestamp(date.getTime()));
                        } else {
                            stmt.setObject(i + 2, null);
                        }
                    } else {
                        stmt.setString(i + 1, values[i]);
                    }

                    //stmt.setString(i + 1, values[i]);
                }
                stmt.addBatch();
            }
        }
        stmt.executeBatch();
        stmt.close();
    }

    private static String getQuery(String tableName, int columnCount) {
        StringBuilder sb = new StringBuilder();
        sb.append(" INSERT INTO ").append(tableName).append(" VALUES ( ").append("\n");
        for (int i = 0; i < columnCount; i++) {
            sb.append("?,");
        }
        sb.deleteCharAt(sb.length() - 1).append(")");
        return sb.toString();
    }

    public static String getCsvTable(ResultSet rs) throws SQLException {
        return getCsvTable(rs, true, "\"");
    }

    public static String getCsvTable(ResultSet rs, boolean includeCmd, String quoteChar) throws SQLException {
        StringBuilder sb = new StringBuilder();
        ResultSetMetaData rsMetadata = rs.getMetaData();
        int columnCount = rsMetadata.getColumnCount();
        int rowCount = 0;
        while (rs.next()) {
            if (includeCmd && rowCount == 0) {
                for (int i = 0; i < columnCount; i++) {
                    sb.append(quoteChar);
                    sb.append(rsMetadata.getColumnName(i + 1));
                    sb.append(quoteChar);
                    if (i < columnCount - 1) {
                        sb.append(",");
                    }
                }
                sb.append("\n");
            }
            for (int i = 0; i < columnCount; i++) {
                sb.append(quoteChar);
                String val = rs.getString(i + 1);
                if (val != null) {
                    sb.append(rs.getString(i + 1).replace("\"", "\\\""));
                } else {
                    sb.append("null");
                }
                sb.append(quoteChar);
                if (i < columnCount - 1) {
                    sb.append(",");
                }
            }
            sb.append("\n");
            rowCount++;
            if (rowCount % 10000 == 0) {
                System.out.println(rowCount);
            }
        }
        return sb.toString();
    }

    public static Connection getConnection() throws Exception {
        //Class.forName("com.sap.db.jdbc.Driver").newInstance();
        //String url = "jdbc:sap://COE-HE-28.WDF.SAP.CORP:30115/I827779";
        //return DriverManager.getConnection(url, "I827779", "Google6377");


        String url = "jdbc:sap://lspal134:31015/HADOOP";
        Connection conn = DriverManager.getConnection(url, "SYSTEM", "Hana1234");
        return conn;
    }

    public static List<CombinedCategoryInfo> getCombinedCategoryInfo() throws Exception {
        String query = " select ddd.cat_combo, avg(unit_price-unit_cost) from " +
                "(   select category || '~' || sub_category as cat_combo, unit_price as unit_price, unit_cost as unit_cost " +
                "    from sportmart.dim_item  ) ddd " +
                "    group by ddd.cat_combo";
        Connection conn = getConnection();
        PreparedStatement stmt = conn.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();
        List<CombinedCategoryInfo> info = new ArrayList<CombinedCategoryInfo>();
        while (rs.next()) {
            CombinedCategoryInfo cat = new CombinedCategoryInfo(rs.getString(1), rs.getDouble(2));
            info.add(cat);
        }
        stmt.close();
        conn.close();
        return info;
    }

    public static List<Item> getItems(String addConstrain) throws Exception {
        String query = " select item_lookup, category, sub_category, unit_cost, unit_price from sportmart.dim_item where item_lookup is not null ";
        if (addConstrain != null) {
            query += addConstrain;
        }
        Connection conn = getConnection();
        PreparedStatement stmt = conn.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();
        List<Item> items = new ArrayList<Item>();
        while (rs.next()) {
            Item item = new Item(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getDouble(4), rs.getDouble(5));
            items.add(item);
        }
        stmt.close();
        conn.close();
        return items;
    }

    public static void generateItems() throws Exception {
        List<Item> items = getItems(null);
        BufferedWriter out = new BufferedWriter(new FileWriter("C:\\projects\\data\\sportmart\\items.csv"));
        for (Item item : items) {
            out.write(item.toCsvString() + "\n");
        }
        out.close();
    }

    public static Collection<Item> getItems() throws Exception {
        List<String> lines = FileUtils.readLines(new File("C:\\projects\\data\\sportmart\\items.csv"));
        Set<Item> items = new HashSet<Item>();
        for (String line : lines) {
            String[] values = CSVUtils.parseLine(line);
            Item item = new Item(Integer.parseInt(values[0]),
                    values[1], values[2],
                    Double.parseDouble(values[3]),
                    Double.parseDouble(values[4]));
            items.add(item);
        }
        return items;
    }

    public static Map<String, List<Item>> getCategoryItems(Collection<Item> items) {
        Map<String, List<Item>> map = new HashMap<String, List<Item>>();
        for (Item item : items) {
            List<Item> collection = map.get(item.getCategory());
            if (collection == null) {
                collection = new ArrayList<Item>();
            }
            collection.add(item);
            map.put(item.getCategory(), collection);
        }
        return map;
    }

    public static Map<String, List<Item>> getSubCategoryItems(Collection<Item> items) {
        Map<String, List<Item>> map = new HashMap<String, List<Item>>();
        for (Item item : items) {
            List<Item> collection = map.get(item.getSubCategory());
            if (collection == null) {
                collection = new ArrayList<Item>();
            }
            collection.add(item);
            map.put(item.getSubCategory(), collection);
        }
        return map;
    }

    private static Set<Integer> getStoreNums() throws Exception {
        String query = " select store_num from sportmart.dim_store";
        Connection conn = getConnection();
        PreparedStatement stmt = conn.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();
        Set<Integer> set = new HashSet<Integer>();
        while (rs.next()) {
            set.add(rs.getInt(1));
        }
        stmt.close();
        rs.close();
        conn.close();
        return set;
    }

    private static void dumpItemQty(Connection conn, int storeNum, String filename) throws Exception {
        String query = " select calendar_date, store_num, item_lookup, sold_qty from sportmart.fact_pos where store_num = ?  ";
        BufferedWriter out = new BufferedWriter(new FileWriter(new File(filename)));
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setInt(1, storeNum);
        ResultSet rs = stmt.executeQuery();
        StringBuilder sb = new StringBuilder();
        while (rs.next()) {
            sb.append(DATE_FORMAT.format(rs.getTimestamp(1))).append(",")
                    .append(rs.getString(2)).append(",")
                    .append(rs.getString(3)).append(",")
                    .append(rs.getString(4)).append("\n");

            out.write(sb.toString());
            sb = new StringBuilder();
        }
        stmt.close();
        rs.close();
        out.close();
    }

    public static List<CssUrl> getCssUrls(String filename) throws Exception {
        List<String> lines = FileUtils.readLines(new File(filename));
        List<CssUrl> list = new ArrayList<CssUrl>();
        for (String line : lines) {
            String[] values = CSVUtils.parseLine(line);
            list.add(new CssUrl(Integer.parseInt(values[0]), values[1]));
        }
        return list;
    }

    public static List<ImageUrl> getImageUrls(String filename) throws Exception {
        List<String> lines = FileUtils.readLines(new File(filename));
        List<ImageUrl> list = new ArrayList<ImageUrl>();
        for (String line : lines) {
            String[] values = CSVUtils.parseLine(line);
            list.add(new ImageUrl(Integer.parseInt(values[0]), values[1]));
        }
        return list;
    }

    public static List<JsUrl> getJsUrls(String filename) throws Exception {
        List<String> lines = FileUtils.readLines(new File(filename));
        List<JsUrl> list = new ArrayList<JsUrl>();
        for (String line : lines) {
            String[] values = CSVUtils.parseLine(line);
            list.add(new JsUrl(Integer.parseInt(values[0]), values[1]));
        }
        return list;
    }

    public static Map<String, Integer> getDateMap() throws Exception {
        String query = "select id, \"YYYY\", month_of_year, day_of_month\n" +
                "from HADOOP.DATE_DIM";
        Connection conn = getConnection();
        PreparedStatement stmt = conn.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();
        Map<String, Integer> map = new HashMap<String, Integer>();
        while (rs.next()) {
            String dateString = rs.getString(2) + "-" +
                    getPaddedNumberString(rs.getString(3), 2, "0") + "-" +
                    getPaddedNumberString(rs.getString(4), 2, "0");
            map.put(dateString, rs.getInt(1));
//System.out.println(dateString + ", " + rs.getInt(1));
        }
        stmt.close();
        conn.close();
        return map;
    }

    public static List<Store> getStores() throws Exception {
        String query = "select store_num, retailer_cd, store_type, city, state, region_desc\n" +
                "from sportmart.dim_store store\n" +
                "where store.zip_code <> ''";
        Connection conn = getConnection();
        PreparedStatement stmt = conn.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();

        List<Store> list = new ArrayList<Store>();

        while (rs.next()) {
            Store store = new Store(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5),
                    rs.getString(6));
            list.add(store);
        }
        return list;
    }

    public static Map<String, Item> getItemMap(Collection<Item> items) throws Exception {
        Map<String, Item> map = new HashMap<String, Item>();
        for (Item item : items) {
            map.put(String.valueOf(item.getItemLookup()), item);
        }
        return map;
    }

    public static String getItemAsin(String line) {
        String key = "PPSID=";
        int dpIdx = line.indexOf("PPSID=");
        int idEnd = line.indexOf("&", dpIdx + 1);
        if (idEnd < 0) {
            line.indexOf("\"", dpIdx + 1);
            idEnd = line.length();
        }
        if (dpIdx >= 0) {
            return line.substring(dpIdx + key.length(), idEnd);
        }
        return null;
    }

    public static List<Session> getSessions(TreeMap<Long, String> sortedMap, int sessionInMin) {
        Session session = null;
        List<Session> sessionList = new LinkedList<Session>();
        long prevTs = -1l;
        long entryTs = -1l;
        long endTs = -1l;
        long length = sessionInMin * 60 * 1000;
        for (Map.Entry<Long, String> entry : sortedMap.entrySet()) {
            if (session == null) {
                session = new Session();
            }
            entryTs = entry.getKey();
            if (prevTs == -1l) {
                session.addItemLookup(entryTs, entry.getValue());
            } else {
                endTs = prevTs + length;
                if (entryTs <= endTs && entryTs > prevTs) {
                    // Within session
                    session.addItemLookup(entryTs, entry.getValue());
                } else {
                    sessionList.add(session);
                    session = new Session();
                    session.addItemLookup(entryTs, entry.getValue());

                }
            }
            prevTs = entryTs;
        }
        // Add the last session
        if (session.itemLookups.size() > 0) {
            sessionList.add(session);
        }
        return sessionList;
    }

    public static void generateStoreItemSoldQty() throws Exception {
        List<Store> stores = getStores();
        for (String state : STATES) {
            System.out.println("Working on " + state);
            String filename = "f:\\sportmart\\storeItemQty\\" + state + ".csv";
            File file = new File(filename);
            if (!file.exists()) {
                file.createNewFile();
                List<Store> nowStore = getStoreByState(stores, state);
                Map<Integer, Double> nowItemQty = getItemSoldQty(nowStore);
                List<String> nowLines = new ArrayList<String>();
                for (Map.Entry<Integer, Double> entry : nowItemQty.entrySet()) {
                    nowLines.add(entry.getKey() + "," + entry.getValue() + " \n");
                }
                FileUtils.writeLines(file, nowLines);
            }
        }
    }

    public static void printMap(Map<String, Map<Integer, Float>> map) {
        for (Map.Entry<String, Map<Integer, Float>> entry : map.entrySet()) {
            System.out.println(entry.getKey());
            for (Map.Entry<Integer, Float> eee : entry.getValue().entrySet()) {
                System.out.println("    " + eee.getKey() + " : " + eee.getValue());
            }
        }
    }

    public static Map<String, Map<Integer, Double>> getStateItemSoldMap() throws Exception {
        File folder = new File("C:\\projects\\data\\itemQtyByState");
        Map<String, Map<Integer, Double>> map = new HashMap<String, Map<Integer, Double>>(51);
        for (File file : folder.listFiles()) {
            Map<Integer, Double> itemSoldMap = new HashMap<Integer, Double>();
            List<String> lines = FileUtils.readLines(file);
            for (String line : lines) {
                if (line.length() > 0) {
                    String[] values = CSVUtils.parseLine(line);
                    int itemLookup = Integer.parseInt(values[0]);
                    double qty = Double.parseDouble(values[1]);
                    if (itemSoldMap.containsKey(itemLookup)) {
                        qty = qty + itemSoldMap.get(itemLookup);
                    }
                    itemSoldMap.put(itemLookup, qty);
                }
            }
            map.put(file.getName().replace(".csv", ""), itemSoldMap);
        }
        return map;
    }

    public static Map<String, Map<Integer, Double>> getDateItemSoldMap() throws Exception {
        File folder = new File("C:\\projects\\data\\itemQtyByDate");
        Map<String, Map<Integer, Double>> map = new HashMap<String, Map<Integer, Double>>(1461);
        int fileCount = 0;
        File[] files = folder.listFiles();
        for (File file : files) {
//System.out.println(fileCount + "/" + files.length);
            Map<Integer, Double> itemSoldMap = new HashMap<Integer, Double>();
            List<String> lines = FileUtils.readLines(file);
            for (String line : lines) {
                if (line.length() > 0) {
                    String[] values = CSVUtils.parseLine(line);
                    int itemLookup = Integer.parseInt(values[0]);
                    double qty = Double.parseDouble(values[1]);
                    if (itemSoldMap.containsKey(itemLookup)) {
                        qty = qty + itemSoldMap.get(itemLookup);
                    }
                    itemSoldMap.put(itemLookup, qty);
                }
            }
            map.put(file.getName().replace(".csv", "").trim(), itemSoldMap);
            fileCount++;
        }
        return map;
    }

    public static void generateAllStoreItemSoldQty() throws Exception {
        List<Store> stores = getStores();
        Map<Integer, Double> itemQty = getItemSoldQty(stores);
        List<String> lines = new ArrayList<String>();
        for (Map.Entry<Integer, Double> entry : itemQty.entrySet()) {
            lines.add(entry.getKey() + "," + entry.getValue() + " \n");
        }
        FileUtils.writeLines(new File("f:\\sportmart\\storeItemQty\\allItemSold.csv"), lines);
    }

    public static void generateAllStoreItemSoldQtyByDate() throws Exception {
        //Date startDate = SIMPLE_DATE_FORMAT.parse("2006-04-15");
        //Date endDate = SIMPLE_DATE_FORMAT.parse("2009-12-10");
        BufferedReader reader = new BufferedReader(new FileReader("F:\\sportmart\\tableCsv\\fact_pos\\data.csv"));
        String line = reader.readLine();
        Map<String, Map<Integer, Float>> dateItemSoldMap = new HashMap<String, Map<Integer, Float>>(1500);
        long lineCount = 0;
        while (line != null) {
            if (line.length() > 0) {
                String[] values = CSVUtils.parseLine(line);
                String dateString = SIMPLE_DATE_FORMAT.format(DATE_FORMAT.parse(values[0]));
                int itemLookup = Integer.parseInt(values[1]);
                float soldQty = Float.parseFloat(values[6]);
                float existingSoldQty = 0f;
                Map<Integer, Float> itemSoldMap = dateItemSoldMap.get(dateString);
                if (itemSoldMap == null) {
                    itemSoldMap = new HashMap<Integer, Float>(5000);
                }
                if (itemSoldMap.containsKey(itemLookup)) {
                    existingSoldQty = itemSoldMap.get(itemLookup);
                }
                existingSoldQty = existingSoldQty + soldQty;
                itemSoldMap.put(itemLookup, existingSoldQty);
                dateItemSoldMap.put(dateString, itemSoldMap);
            }
            line = reader.readLine();
            lineCount++;
            if (lineCount % 1000000 == 0) {
                System.out.println("Read " + lineCount + " lines...");
            }
        }
        reader.close();
        int fileCount = 0;
        for (Map.Entry<String, Map<Integer, Float>> entry : dateItemSoldMap.entrySet()) {
            String filename = "F:\\sportmart\\storeItemQty\\" + entry.getKey() + " .csv";
            System.out.println(fileCount + "/" + dateItemSoldMap.size() + " --> Outputting to " + filename);
            File file = new File(filename);
            Map<Integer, Float> map = entry.getValue();
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Integer, Float> itemEntry : map.entrySet()) {
                sb.append(itemEntry.getKey()).append(",").append(itemEntry.getValue()).append("\n");
            }
            FileUtils.writeStringToFile(file, sb.toString());
            fileCount++;
        }
    }

    static List<Store> getStoreByState(List<Store> stores, String state) {
        List<Store> selected = new ArrayList<Store>();
        for (Store store : stores) {
            if (store.getState().equalsIgnoreCase(state)) {
                selected.add(store);
            }
        }
        return selected;
    }

    static Map<Integer, Double> getItemSoldQty(List<Store> stores) throws IOException {
        Map<Integer, Double> itemQty = new HashMap<Integer, Double>();
        String baseFolder = "F:\\sportmart\\storeItemQty\\";
        int index = 0;
        for (Store store : stores) {
            System.out.println(index + "/" + stores.size() + ", working on " + store.getNum());
            BufferedReader in = new BufferedReader(new FileReader(baseFolder + store.getNum() + ".csv"));
            String line = in.readLine();
            while (line != null) {
                String[] values = CSVUtils.parseLine(line);
                Integer itemId = Integer.parseInt(values[2]);
                Double qty = Double.parseDouble(values[3]);
                if (itemQty.containsKey(itemId.intValue())) {
                    double existing = itemQty.get(itemId.intValue());
                    qty += existing;
                }
                itemQty.put(itemId.intValue(), qty);
                line = in.readLine();
            }
            index++;
        }
        return itemQty;
    }

    /*
    static Map<Integer, Double> getItemSoldQtyByDate(String dateString, List<Store> stores) throws Exception {
        Map<Integer, Double> itemQty = new HashMap<Integer, Double>();
        String baseFolder = "F:\\sportmart\\storeItemQty\\";
        int index = 0;
        for (Store store: stores) {
System.out.println(index + "/" + stores.size() + ", working on " + store.getNum());
            BufferedReader in = new BufferedReader(new FileReader(baseFolder + store.getNum() + ".csv"));
            String line = in.readLine();
            while (line != null) {
                String[] values = CSVUtils.parseLine(line);
                String lineDateString = SIMPLE_DATE_FORMAT.format(new Date(DATE_FORMAT.parse(values[0]).getTime()));
                if (lineDateString.equals(dateString)) {
                    Integer itemAsin = Integer.parseInt(values[2]);
                    Double qty = Double.parseDouble(values[3]);
                    if (itemQty.containsKey(itemAsin.intValue())) {
                        double existing = itemQty.get(itemAsin.intValue());
                        qty += existing;
                    }
                    itemQty.put(itemAsin.intValue(), qty);
                }
                line = in.readLine();
            }
            index++;
        }
        return itemQty;
    }
    */


    static Map<Integer, Double> getItemSoldQtyByDate(Connection conn) throws Exception {
        Map<Integer, Double> itemQty = new HashMap<Integer, Double>();
        String query =
                "select to_char(calendar_date, 'YYYY-MM-DD'), item_lookup, sold_qty\n" +
                        "from sportmart.fact_pos f\n";
        PreparedStatement pstmt = conn.prepareStatement(query);
        System.out.println("About to send query : " + query);
        long start = System.currentTimeMillis();
        ResultSet rs = pstmt.executeQuery();
        long end = System.currentTimeMillis();
        System.out.println("................ took " + (end - start) / 1000 + " seconds");
        while (rs.next()) {
            int itemLookup = rs.getInt(1);
            double qty = 0d;
            if (itemQty.containsKey(itemLookup)) {
                qty += rs.getDouble(2);
            }
            itemQty.put(itemLookup, qty);
        }
        rs.close();
        pstmt.close();
        conn.clearWarnings();
        return itemQty;
    }

    private static void updateStateRegion() throws Exception {
        List<String> lines = FileUtils.readLines(new File("C:\\projects\\data\\store\\store_dim.csv"));
        Map<String, String> stateRegion = new HashMap<String, String>();
        Connection conn = getConnection();
        String query = " update sportmart.dim_store set region_cd = ? where state = ?";
        PreparedStatement pstmt = conn.prepareStatement(query);

        for (String line : lines) {
            String[] values = CSVUtils.parseLine(line);
            if (!stateRegion.containsKey(values[9])) {
                stateRegion.put(values[9], values[13]);
            }
        }


        for (Map.Entry<String, String> entry : stateRegion.entrySet()) {
            pstmt.setString(1, entry.getValue());
            pstmt.setString(2, entry.getKey());
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        pstmt.close();
        conn.close();
    }

    public static Map<String, State> getStateMap() throws Exception {
        String incomeFilaneme = com.sap.demo.pos.Utility.BASE_FOLDER + "data/state/stateIncome.csv";
        String populationFilaneme = com.sap.demo.pos.Utility.BASE_FOLDER + "data/state/statePopulation.csv";

        List<String> incomeLines = FileUtils.readLines(new File(incomeFilaneme));
        List<String> populationLines = FileUtils.readLines(new File(populationFilaneme));
        long totalIncome = 0l;
        long avgIncome = 0l;
        Map<String, State> stateMap = new HashMap<String, State>();

        for (String line : populationLines) {
            String[] values = CSVUtils.parseLine(line);
            State state = new State(STATE_ABBREVIATION.get(values[2]), values[2]);
            state.setPopulation(Long.parseLong(values[3]));
            state.setPopulationPercentage(Float.parseFloat(values[12].replace("%", "")));
            state.setRegion(STATE_REGION.get(state.getFullName()));
            stateMap.put(state.getAbbreviation(), state);
        }

        for (String line : incomeLines) {
            String[] values = CSVUtils.parseLine(line);
            State state = stateMap.get(STATE_ABBREVIATION.get(values[0]));
            long income = Long.parseLong(values[1]);
            totalIncome += income;
            state.setIncome(income);
        }
        avgIncome = totalIncome / stateMap.size();
        for (Map.Entry<String, State> entry : stateMap.entrySet()) {
            State state = entry.getValue();
            state.setIncomePercentage(((float) state.getIncome() / (float) avgIncome));
        }
        return stateMap;
    }

    public static void generateCategoryItems() throws Exception {
        String baseFolder = "W:\\itemCategory\\";
        List<Item> items = getItems(null);
        Map<String, Set<Integer>> categoryItems = new HashMap<String, Set<Integer>>();
        for (Item item : items) {
            Set<Integer> itemLookups = null;
            if (!categoryItems.containsKey(item.getCategory())) {
                itemLookups = new HashSet<Integer>();
            } else {
                itemLookups = categoryItems.get(item.getCategory());
            }
            itemLookups.add(item.getItemLookup());
            categoryItems.put(item.getCategory(), itemLookups);
        }

        for (Map.Entry<String, Set<Integer>> entry : categoryItems.entrySet()) {
            String filename = baseFolder + entry.getKey().replace("*", "") + ".csv";
            BufferedWriter out = new BufferedWriter(new FileWriter(filename));
            for (Integer itemlookup : entry.getValue()) {
                out.write(itemlookup.intValue() + "\n");
            }
            out.close();
        }
    }


    public static void generateSubcategoryItems() throws Exception {
        String baseFolder = "W:\\itemSubcategory\\";
        List<Item> items = getItems(null);
        Map<String, Set<Integer>> subCategoryItems = new HashMap<String, Set<Integer>>();
        for (Item item : items) {
            Set<Integer> itemLookups = null;
            if (!subCategoryItems.containsKey(item.getSubCategory())) {
                itemLookups = new HashSet<Integer>();
            } else {
                itemLookups = subCategoryItems.get(item.getSubCategory());
            }
            itemLookups.add(item.getItemLookup());
            subCategoryItems.put(item.getCategory(), itemLookups);
        }

        for (Map.Entry<String, Set<Integer>> entry : subCategoryItems.entrySet()) {
            String filename = baseFolder + entry.getKey().replace("*", "") + ".csv";
            BufferedWriter out = new BufferedWriter(new FileWriter(filename));
            for (Integer itemlookup : entry.getValue()) {
                out.write(itemlookup.intValue() + "\n");
            }
            out.close();
        }
    }

    public static void main(String[] arg) throws Exception {
        List<String> lines = FileUtils.readLines(new File("C:\\projects\\data\\posDemo\\accessLogs\\access_2008-01-01.log"));
        for (String line : lines) {
            AccessEntry accessEntry = getAccessEntry(line);
            if (accessEntry != null) {
                System.out.println(accessEntry);
            }
        }
    }

    public static void populateCalendar() throws Exception {
        Calendar startCalendar = Calendar.getInstance();
        startCalendar.set(Calendar.YEAR, 1970);
        startCalendar.set(Calendar.MONTH, Calendar.JANUARY);
        startCalendar.set(Calendar.DAY_OF_MONTH, 1);
        startCalendar.set(Calendar.HOUR_OF_DAY, 0);
        startCalendar.set(Calendar.MINUTE, 0);
        startCalendar.set(Calendar.SECOND, 0);
        startCalendar.set(Calendar.MILLISECOND, 0);

        Connection conn = Utility.getConnection();
        PreparedStatement stmt = conn.prepareStatement("INSERT INTO DIM_CALENDAR_DATE VALUES (?, ?, ?)");
        int count = 0;
        while (count < 365 * 100) {
            stmt.setString(1, SIMPLE_DATE_FORMAT.format(startCalendar.getTime()));
            stmt.setInt(2, Integer.parseInt(SIMPLE_DATE_FORMAT.format(startCalendar.getTime()).substring(0, 4)));
            stmt.setInt(3, Integer.parseInt(SIMPLE_DATE_FORMAT.format(startCalendar.getTime()).substring(5, 7)));
            stmt.executeUpdate();
            startCalendar.add(Calendar.DATE, 1);
            count++;
        }
        stmt.close();
        conn.close();

    }

    public static void removeEmptyLines() throws Exception {
        String foldername = "C:\\projects\\data\\itemQtyByState\\";
        File folder = new File(foldername);
        for (File file : folder.listFiles()) {
            List<String> lines = FileUtils.readLines(file);
            List<String> newLines = new LinkedList<String>();
            for (String line : lines) {
                if (line.length() > 0) {
                    newLines.add(line);
                }
            }
            String tempName = foldername + file.getName() + " .temp";
            String originalName = foldername + file.getName();
            FileUtils.writeLines(new File(tempName), newLines);
            file.delete();
            File tempFile = new File(tempName);
            tempFile.renameTo(new File(originalName));
        }
    }


    /*
    public static long getViewingStartMs(Calendar indexCalendar, String nowDateString, int hour) throws ParseException {
        long nowMs = indexCalendar.getTime().getTime() + hour * 3600000 + nextLong(3550000);
        long possibleMs = getFuzzyNumber(nowMs, 3 * 60 * 1000);
        while (!nowDateString.equals(SIMPLE_DATE_FORMAT.format(new Timestamp(possibleMs)))) {
//System.out.println("nowDateString="+nowDateString+", Utility.SIMPLE_DATE_FORMAT.format(new Timestamp(possibleMs))="+Utility.SIMPLE_DATE_FORMAT.format(new Timestamp(possibleMs)));
            nowMs = indexCalendar.getTime().getTime() + hour * 3600000 + nextLong(3550000);
            possibleMs = getFuzzyNumber(nowMs, 3 * 60 * 1000);
        }
        return possibleMs;
    }
    */

    public static long getViewingStartMs(Calendar calendar, int hour) throws ParseException {
        Calendar clearedCalendar = clearMinutes(calendar);
//System.out.println("111, " + clearedCalendar.getTime().toLocaleString());
        clearedCalendar.set(Calendar.HOUR, hour);
        long startMs = clearedCalendar.getTime().getTime();
//System.out.println("222, " + clearedCalendar.getTime().toLocaleString());
        clearedCalendar.add(Calendar.HOUR, 1);
        long endMs = clearedCalendar.getTime().getTime();
//System.out.println("333, " + clearedCalendar.getTime().toLocaleString());
        return startMs + nextLong(endMs - startMs);
    }

    public static long getNextMsWithin(long baseMs, long lengthMs) {
        return baseMs + nextLong(lengthMs);
    }

    private static Calendar clearMinutes(Calendar calendar) {
        Calendar clonedCalendar = (Calendar) calendar.clone();
        clonedCalendar.set(Calendar.HOUR_OF_DAY, 0);
        clonedCalendar.clear(Calendar.MINUTE);
        clonedCalendar.clear(Calendar.SECOND);
        clonedCalendar.clear(Calendar.MILLISECOND);
        return clonedCalendar;
    }

    static {
        STATES = new String[]{
                "MN", "CA", "WV", "UT", "WA",
                "TX", "FL", "GA", "OH", "NC",
                "TN", "MO", "IL", "AL", "PA",
                "IN", "OK", "LA", "MI", "KY",
                "VA", "AR", "SC", "AZ", "WI",
                "MS", "NY", "CO", "KS", "IA",
                "NM", "NE", "NV", "ME", "ID",
                "OR", "NH", "MD", "SD", "WY",
                "ND", "MT", "MA", "NJ", "DE",
                "CT", "AK", "RI", "HI", "VT",
                "PR"
        };


        STATE_ABBREVIATION.put("Alabama", "AL");
        STATE_ABBREVIATION.put("Alaska", "AK");
        STATE_ABBREVIATION.put("Arizona", "AZ");
        STATE_ABBREVIATION.put("Arkansas", "AR");
        STATE_ABBREVIATION.put("California", "CA");
        STATE_ABBREVIATION.put("Colorado", "CO");
        STATE_ABBREVIATION.put("Connecticut", "CT");
        STATE_ABBREVIATION.put("Delaware", "DE");
        STATE_ABBREVIATION.put("Washington DC", "DC");
        STATE_ABBREVIATION.put("Florida", "FL");
        STATE_ABBREVIATION.put("Georgia", "GA");
        STATE_ABBREVIATION.put("Hawaii", "HI");
        STATE_ABBREVIATION.put("Idaho", "ID");
        STATE_ABBREVIATION.put("Illinois", "IL");
        STATE_ABBREVIATION.put("Indiana", "IN");
        STATE_ABBREVIATION.put("Iowa", "IA");
        STATE_ABBREVIATION.put("Kansas", "KS");
        STATE_ABBREVIATION.put("Kentucky", "KY");
        STATE_ABBREVIATION.put("Louisiana", "LA");
        STATE_ABBREVIATION.put("Maine", "ME");
        STATE_ABBREVIATION.put("Montana", "MT");
        STATE_ABBREVIATION.put("Nebraska", "NE");
        STATE_ABBREVIATION.put("Nevada", "NV");
        STATE_ABBREVIATION.put("New Hampshire", "NH");
        STATE_ABBREVIATION.put("New Jersey", "NJ");
        STATE_ABBREVIATION.put("New Mexico", "NM");
        STATE_ABBREVIATION.put("New York", "NY");
        STATE_ABBREVIATION.put("North Carolina", "NC");
        STATE_ABBREVIATION.put("North Dakota", "ND");
        STATE_ABBREVIATION.put("Ohio", "OH");
        STATE_ABBREVIATION.put("Oklahoma", "OK");
        STATE_ABBREVIATION.put("Oregon", "OR");
        STATE_ABBREVIATION.put("Maryland", "MD");
        STATE_ABBREVIATION.put("Massachusetts", "MA");
        STATE_ABBREVIATION.put("Michigan", "MI");
        STATE_ABBREVIATION.put("Minnesota", "MN");
        STATE_ABBREVIATION.put("Mississippi", "MS");
        STATE_ABBREVIATION.put("Missouri", "MO");
        STATE_ABBREVIATION.put("Pennsylvania", "PA");
        STATE_ABBREVIATION.put("Rhode Island", "RI");
        STATE_ABBREVIATION.put("South Carolina", "SC");
        STATE_ABBREVIATION.put("South Dakota", "SD");
        STATE_ABBREVIATION.put("Tennessee", "TN");
        STATE_ABBREVIATION.put("Texas", "TX");
        STATE_ABBREVIATION.put("Utah", "UT");
        STATE_ABBREVIATION.put("Vermont", "VT");
        STATE_ABBREVIATION.put("Virginia", "VA");
        STATE_ABBREVIATION.put("Washington", "WA");
        STATE_ABBREVIATION.put("West Virginia", "WV");
        STATE_ABBREVIATION.put("Wisconsin", "WI");
        STATE_ABBREVIATION.put("Wyoming", "WY");
        STATE_ABBREVIATION.put("Puerto Rico", "PR");

        for (Map.Entry<String, String> entry : STATE_ABBREVIATION.entrySet()) {
            ABBREVIATION_STATE.put(entry.getValue(), entry.getKey());
        }

        //  Maine, New Hampshire, Vermont, Massachusetts, Rhode Island, Connecticut
        Utility.STATE_REGION.put("Maine", "Northeast");
        Utility.STATE_REGION.put("New Hampshire", "Northeast");
        Utility.STATE_REGION.put("Vermont", "Northeast");
        Utility.STATE_REGION.put("Massachusetts", "Northeast");
        Utility.STATE_REGION.put("Rhode Island", "Northeast");
        Utility.STATE_REGION.put("Connecticut", "Northeast");

        //  New York, Pennsylvania, New Jersey
        Utility.STATE_REGION.put("New York", "Northeast");
        Utility.STATE_REGION.put("Pennsylvania", "Northeast");
        Utility.STATE_REGION.put("New Jersey", "Northeast");

        // Wisconsin, Michigan, Illinois, Indiana, Ohio
        Utility.STATE_REGION.put("Wisconsin", "Midwest");
        Utility.STATE_REGION.put("Michigan", "Midwest");
        Utility.STATE_REGION.put("Illinois", "Midwest");
        Utility.STATE_REGION.put("Indiana", "Midwest");
        Utility.STATE_REGION.put("Ohio", "Midwest");

        //  Missouri, North Dakota, South Dakota, Nebraska, Kansas, Minnesota, Iowa
        Utility.STATE_REGION.put("Missouri", "Midwest");
        Utility.STATE_REGION.put("North Dakota", "Midwest");
        Utility.STATE_REGION.put("South Dakota", "Midwest");
        Utility.STATE_REGION.put("Nebraska", "Midwest");
        Utility.STATE_REGION.put("Kansas", "Midwest");
        Utility.STATE_REGION.put("Minnesota", "Midwest");
        Utility.STATE_REGION.put("Iowa", "Midwest");

        //  Delaware, Maryland, District of Columbia, Virginia, West Virginia, North Carolina, South Carolina, Georgia, Florida
        Utility.STATE_REGION.put("Delaware", "South");
        Utility.STATE_REGION.put("Maryland", "South");
        Utility.STATE_REGION.put("Washington DC", "South");
        Utility.STATE_REGION.put("Virginia", "South");
        Utility.STATE_REGION.put("West Virginia", "South");
        Utility.STATE_REGION.put("North Carolina", "South");
        Utility.STATE_REGION.put("South Carolina", "South");
        Utility.STATE_REGION.put("Georgia", "South");
        Utility.STATE_REGION.put("Florida", "South");

        // Kentucky, Tennessee, Mississippi, Alabama
        Utility.STATE_REGION.put("Kentucky", "South");
        Utility.STATE_REGION.put("Tennessee", "South");
        Utility.STATE_REGION.put("Mississippi", "South");
        Utility.STATE_REGION.put("Alabama", "South");

        //  Oklahoma, Texas, Arkansas, Louisiana
        Utility.STATE_REGION.put("Oklahoma", "South");
        Utility.STATE_REGION.put("Texas", "South");
        Utility.STATE_REGION.put("Arkansas", "South");
        Utility.STATE_REGION.put("Louisiana", "South");

        // Idaho, Montana, Wyoming, Nevada, Utah, Colorado, Arizona, New Mexico
        Utility.STATE_REGION.put("Idaho", "West");
        Utility.STATE_REGION.put("Montana", "West");
        Utility.STATE_REGION.put("Wyoming", "West");
        Utility.STATE_REGION.put("Nevada", "West");
        Utility.STATE_REGION.put("Utah", "West");
        Utility.STATE_REGION.put("Colorado", "West");
        Utility.STATE_REGION.put("Arizona", "West");
        Utility.STATE_REGION.put("New Mexico", "West");

        // Alaska, Washington, Oregon, California, Hawaii
        Utility.STATE_REGION.put("Alaska", "West");
        Utility.STATE_REGION.put("Washington", "West");
        Utility.STATE_REGION.put("Oregon", "West");
        Utility.STATE_REGION.put("California", "West");
        Utility.STATE_REGION.put("Hawaii", "West");
    }

    private static class MapEntry implements Comparable<MapEntry> {
        public String category;
        public double qty;

        public int compareTo(MapEntry mapEntry) {
            return (qty - mapEntry.qty < 0) ? 1 : -1;
        }
    }
}
