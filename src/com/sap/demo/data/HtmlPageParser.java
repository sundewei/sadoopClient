package com.sap.demo.data;


import org.apache.commons.csv.CSVUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/26/11
 * Time: 10:08 AM
 * To change this template use File | Settings | File Templates.
 */
public class HtmlPageParser {
    public static final String HEADER_FOLDER_NAME = "C:\\projects\\data\\headers\\";
    public static final String PAGE_FOLDER_NAME = "C:\\projects\\data\\pageInfo\\";
    public static final String CATEGORY_FILE_NAME = "C:\\projects\\data\\itemCategory\\category.csv";
    private static final String CONTENT_DIVIDER = "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~";
    private static final String HIDDEN_INPUT_PREFIX = "<input type=\"hidden\" sessionNum=\"";
    private static final List<String> BY_LINE_PREFIX = new ArrayList<String>();
    private static final Map<String, String> BY_LINE_PREFIX_REPLACEMENT = new HashMap<String, String>();


    private static final String META_TAG_PREFIX = "<meta name=";
    private static final List<String> PRICE_PREFIXES = new ArrayList<String>();

    static {
        PRICE_PREFIXES.add("<span class=\"price\">");
        PRICE_PREFIXES.add("<span class=\"priceLarge\">");
        PRICE_PREFIXES.add("<b class=\"priceLarge\">");
        PRICE_PREFIXES.add("<span sessionNum=\"pricePlusShippingQty\"><b class=\"price\">");
        PRICE_PREFIXES.add("<b class=\"priceLarge kitsunePrice\">");
        BY_LINE_PREFIX.add("<span class=\"shvl-byline\">");
        BY_LINE_PREFIX.add("(Author)");
        BY_LINE_PREFIX_REPLACEMENT.put("(Author)", "\">");
        BY_LINE_PREFIX.add("by&#160;<a href=\"");
        BY_LINE_PREFIX_REPLACEMENT.put("by&#160;<a href=\"", "\">");
        BY_LINE_PREFIX.add("Ships from and sold by <b>");
    }

    public static Map<String, String> getIdUrlMap() throws IOException {
        File folder = new File(HEADER_FOLDER_NAME);
        Map<String, String> idUrlMap = new HashMap<String, String>();
        for (File file : folder.listFiles()) {
            List<String> lines = FileUtils.readLines(file);
            String urlString = lines.get(0);
            int dpIndex = urlString.indexOf("/dp/");
            int searchIndex = urlString.indexOf("/search/");
            int gpIndex = urlString.indexOf("/gp/product/");
            int eIndex = urlString.indexOf("/e/");
            int homePageFromLogin = urlString.indexOf("ref=gno_logo");
            int searchResultIndex = urlString.indexOf("/s/ref=");
            String id = null;
            if (id == null && dpIndex > 0) {
                id = urlString.substring(dpIndex + "/dp/".length(), dpIndex + "/dp/".length() + 10);
            }
            if (id == null && gpIndex > 0) {
                id = urlString.substring(gpIndex + "/gp/product/".length(), gpIndex + "/gp/product/".length() + 10);
            }

            if (id == null && eIndex > 0) {
                id = urlString.substring(eIndex + "/e/".length(), eIndex + "/e/".length() + 10);
            }
            if (id != null) {
                idUrlMap.put(id, urlString);
            }
            System.out.println(id + " : " + urlString);
        }
        return idUrlMap;
    }

    public static Map<String, String> getHeaderIdItemIdMap() throws IOException {
        File folder = new File(HEADER_FOLDER_NAME);
        Map<String, String> idUrlMap = new HashMap<String, String>();
        for (File file : folder.listFiles()) {
            List<String> lines = FileUtils.readLines(file);
            String urlString = lines.get(0);
            int dpIndex = urlString.indexOf("/dp/");
            int searchIndex = urlString.indexOf("/search/");
            int gpIndex = urlString.indexOf("/gp/product/");
            int eIndex = urlString.indexOf("/e/");
            int homePageFromLogin = urlString.indexOf("ref=gno_logo");
            int searchResultIndex = urlString.indexOf("/s/ref=");
            String id = null;
            String key = file.getName().replace(".headers", "");
            if (id == null && dpIndex > 0) {
                id = urlString.substring(dpIndex + "/dp/".length(), dpIndex + "/dp/".length() + 10);
            }
            if (id == null && gpIndex > 0) {
                id = urlString.substring(gpIndex + "/gp/product/".length(), gpIndex + "/gp/product/".length() + 10);
            }

            if (id == null && eIndex > 0) {
                id = urlString.substring(eIndex + "/e/".length(), eIndex + "/e/".length() + 10);
            }
            if (id != null) {
                System.out.println("In getHeaderIdItemIdMap, key = " + key + ", id = " + id);
                idUrlMap.put(key, id);
            }
        }
        return idUrlMap;
    }

    public static HtmlPageInfo getHtmlPageInfo(String id, String targetAddress) throws Exception {
        /*
        URI uri = new URI(targetAddress);
        URLConnection urlConn = uri.toURL().openConnection();
        return urlConn.getHeaderFields();
        */
        HtmlPageInfo htmlPageInfo = new HtmlPageInfo();
        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpGet httpget = new HttpGet(targetAddress);
        HttpResponse response = httpclient.execute(httpget);
        HttpEntity entity = response.getEntity();
        List<String> lines = IOUtils.readLines(entity.getContent());
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line).append("\n");
        }
        htmlPageInfo.content = sb.toString().trim();
        htmlPageInfo.headers = toMap(response.getAllHeaders());
        htmlPageInfo.productId = id;
        return htmlPageInfo;
    }

    public static HtmlPageInfo getHtmlPageInfo(File targetFile) throws Exception {
        HtmlPageInfo htmlPageInfo = new HtmlPageInfo();
        List<String> lines = FileUtils.readLines(targetFile);
        String productId = lines.get(0);
        String filename = lines.get(1);
        Map<String, String> headers = new HashMap<String, String>();
        int count = 2;
        String key = null;
        while (true) {
            String nowLine = lines.get(count);
            if (nowLine.equals(CONTENT_DIVIDER)) {
                break;
            }
            if (count % 2 == 1) {
                key = nowLine;
            } else {
                headers.put(key, nowLine);
            }
            count++;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = count + 1; i < lines.size(); i++) {
            sb.append(lines.get(i)).append("\n");
        }
        htmlPageInfo.filename = targetFile.getName();
        htmlPageInfo.content = sb.toString().trim();
        htmlPageInfo.headers = headers;
        htmlPageInfo.productId = productId;
        return htmlPageInfo;
    }

    public static List<HtmlPageInfo> getHtmlPageInfoList() throws Exception {
        List<HtmlPageInfo> list = new ArrayList<HtmlPageInfo>();
        File folder = new File(PAGE_FOLDER_NAME);
        for (File file : folder.listFiles()) {
            list.add(getHtmlPageInfo(file));
        }
        return list;
    }

    private static void writeCategoryMap(Map<Integer, String> map) throws Exception {
        List<String> lines = new ArrayList<String>();
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            lines.add(entry.getKey(), "\"" + entry.getValue() + "\"");
        }
        FileUtils.writeLines(new File(CATEGORY_FILE_NAME), lines);
    }

    private static Map<Integer, String> getCategoryMap() throws Exception {
        File source = new File(CATEGORY_FILE_NAME);
        if (!source.exists()) {
            return null;
        }
        List<String> categories = FileUtils.readLines(source);
        Map<Integer, String> map = new LinkedHashMap<Integer, String>();
        for (String line : categories) {
            String[] values = CSVUtils.parseLine(line);
            map.put(Integer.parseInt(values[0]), values[1]);
        }
        return map;
    }

    public static Map<String, Integer> getReverseCategoryMap() throws Exception {
        File source = new File(CATEGORY_FILE_NAME);
        if (!source.exists()) {
            return null;
        }
        List<String> categories = FileUtils.readLines(source);
        Map<String, Integer> map = new LinkedHashMap<String, Integer>();
        for (String line : categories) {
            String[] values = CSVUtils.parseLine(line);
            map.put(values[1], Integer.parseInt(values[0]));
        }
        return map;
    }


    public static void main(String[] arg) throws Exception {
        //saveHtmlPageInfo();

        Map<String, Integer> catMap = getReverseCategoryMap();

        List<HtmlPageInfo> pages = getHtmlPageInfoList();
        int priceNullCount = 0;
        for (HtmlPageInfo page : pages) {
            //if (page.itemLookup.equals("B003VUO6H4")) {
            Map<String, String> metaMap = getInformationMap(page.content);

            //if (metaMap.get("price") == null) {
            priceNullCount++;
            System.out.println(priceNullCount + " -> page.filename : " + page.filename);
            System.out.println("productId : ->" + page.productId + "<-");

            int categoryId = catMap.get(getCategory(metaMap.get("description")));
            System.out.println("CategoryId: " + categoryId);
            System.out.println("Price: " + metaMap.get("price") + "\n");
            Double.parseDouble(metaMap.get("price"));
            break;
            //}

            //System.out.println("SubCategory:"+getSubCategory(metaMap.get("keywords"))+"\n");
            //}
        }


    }

    public static String getCategory(String descriptionMeta) {
        String[] values = descriptionMeta.split(":");
        return values[values.length - 1].trim();
    }

    public static Map<String, String> getInformationMap(String content) throws IOException {
        Map<String, String> metaMap = new HashMap<String, String>();
        BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content.getBytes())));
        String line = br.readLine();
        while (line != null) {
            if (line.startsWith(META_TAG_PREFIX)) {
                metaMap.put(getMetaName(line), getMetaContent(line));
            }
            for (String pricePrefix : PRICE_PREFIXES) {
                int prefixIndex = line.indexOf(pricePrefix);
                String priceInMap = metaMap.get("price");
                if (priceInMap == null && prefixIndex > 0) {
                    String priceStr = getPrice(line, pricePrefix);//
                    metaMap.put("price", priceStr.replaceAll(",", "").replace("$", ""));
                    priceInMap = metaMap.get("price");
                }
            }
            int inputIndex = line.indexOf(HIDDEN_INPUT_PREFIX);
            if (inputIndex >= 0) {
                String id = getInputElement(line, "sessionNum=\"");
                String value = getInputElement(line, "value=\"");
                metaMap.put(getInputElement(line, "sessionNum=\""), getInputElement(line, "value=\""));
            }


            String byLine = metaMap.get("brand");
            for (String byLinePrefix : BY_LINE_PREFIX) {
                if (byLine == null && line.indexOf(byLinePrefix) >= 0) {
                    byLine = getByLine(line, byLinePrefix);
                    if (byLine != null && !"null".equalsIgnoreCase(byLine)) {
                        metaMap.put("brand", byLine.trim());
                    }
                }
            }
            line = br.readLine();
        }
        return metaMap;
    }

    private static String getInputElement(String line, String prefix) {
        // <input type="hidden" sessionNum="sourceCustomerOrgListID" name="sourceCustomerOrgListID" value="" />
        int inputIndex = line.indexOf(prefix);
        return line.substring(inputIndex + prefix.length(), line.indexOf("\"", inputIndex + prefix.length()));
    }

    private static String getByLine(String line, String prefix) {
        // <span class="shvl-byline">by Greg Tang</span>
        int byLinIndex = line.indexOf(prefix);
        if (BY_LINE_PREFIX_REPLACEMENT.get(prefix) != null) {
            prefix = BY_LINE_PREFIX_REPLACEMENT.get(prefix);
            byLinIndex = line.indexOf(prefix);
        }
        return line.substring(byLinIndex + prefix.length(), line.indexOf("<", byLinIndex + prefix.length()));
    }

    private static String getPrice(String line, String prefix) {
        int pricePrefixIndex = line.indexOf(prefix);
        return line.substring(pricePrefixIndex + prefix.length(), line.indexOf("<", pricePrefixIndex + prefix.length()));
    }

    private static String getMetaName(String line) {
        String key = "name=\"";
        int nameIndex = line.indexOf(key);
        int nameEndIndex = -1;
        if (nameIndex >= 0) {
            nameEndIndex = line.indexOf("\"", nameIndex + key.length() + 1);
        }
        if (nameEndIndex >= 0) {
            return line.substring(nameIndex + key.length(), nameEndIndex);
        } else {
            return null;
        }
    }

    private static String getMetaContent(String line) {
        String key = "content=\"";
        int nameIndex = line.indexOf(key);
        int nameEndIndex = -1;
        if (nameIndex >= 0) {
            nameEndIndex = line.indexOf("\"", nameIndex + key.length() + 1);
        }
        if (nameEndIndex >= 0) {
            return line.substring(nameIndex + key.length(), nameEndIndex);
        } else {
            return null;
        }
    }

    private static Map<String, String> toMap(Header[] headers) {
        Map<String, String> map = new HashMap<String, String>();
        for (Header header : headers) {
            map.put(header.getName(), header.getValue());
        }
        return map;
    }

    public static void saveHtmlPageInfo() throws Exception {
        Map<String, String> idUrlMap = getIdUrlMap();
        int count = 0;
        for (Map.Entry<String, String> entry : idUrlMap.entrySet()) {
            System.out.println(count + "/" + idUrlMap.size());
            System.out.println(entry.getValue());
            HtmlPageInfo htmlPageInfo = getHtmlPageInfo(entry.getKey(), entry.getValue());
            FileUtils.write(new File(PAGE_FOLDER_NAME + entry.getKey() + ".info"), htmlPageInfo.toString());
            count++;
        }
    }

    public static class HtmlPageInfo {
        public String filename;
        public String productId;
        public Map<String, String> headers;
        public String content;

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(productId).append("\n");
            sb.append(filename).append("\n");
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                sb.append(entry.getKey()).append("\n");
                sb.append(entry.getValue()).append("\n");
            }
            sb.append(CONTENT_DIVIDER).append("\n");
            sb.append(content);
            return sb.toString();
        }
    }

}
