package com.sap.demo;

import java.io.File;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/24/11
 * Time: 4:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class ViewedPageGenerator {

    public static final String FOLDER_NAME = "C:\\projects\\data\\headers\\";

    private static String[] RANDOM_PAGE_ARRAY;

    public static List<String[]> PAGE_LIST = new ArrayList<String[]>();

    private static Set<String> RANDOM_PAGES = new HashSet<String>();

    public static int PAGE_LIST_SIZE = 0;

    public static String[] PAGE_PREFIXES = new String[]{
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
            "k", "l", "m", "n", "o", "p", "q", "s", "t", "u",
            "v", "w", "x", "y", "z",
    };

    static {
        initPageList();

        PAGE_LIST_SIZE = PAGE_LIST.size();

        initRandomPages();

        RANDOM_PAGE_ARRAY = RANDOM_PAGES.toArray(new String[RANDOM_PAGES.size()]);
    }

    private static void initPageList() {
        File folder = new File(FOLDER_NAME);
        for (String prefix : PAGE_PREFIXES) {
            List<String> files = new ArrayList<String>();
            for (File file : folder.listFiles()) {
                if (file.getName().startsWith(prefix)) {
                    files.add(file.getName().replace(".headers", ""));
                }
            }
            if (files.size() > 0) {
                PAGE_LIST.add(files.toArray(new String[files.size()]));
            }
        }
    }

    private static void initRandomPages() {
        File folder = new File(FOLDER_NAME);
        for (File file : folder.listFiles()) {
            RANDOM_PAGES.add(file.getName().replace(".headers", ""));
        }
    }

    public static ViewedPage[] getViewedPages() throws Exception {
        return getViewedPages(Utility.RANDOM.nextInt(100));
    }

    public static void main(String[] arg) throws Exception {
        for (int i = 0; i < 10; i++) {
            ViewedPage[] pages = getViewedPages(Utility.RANDOM.nextInt(100));
            System.out.println("Page size = " + pages.length);
            for (ViewedPage vp : pages) {
                for (TimedString ts : vp.getTimedStrings("1.1.1.1", 213123123l)) {
                    if (ts.line.indexOf("/images/") < 0 && ts.line.indexOf("/clog/") < 0) {
                        System.out.println("Lines: " + ts);
                    }
                }
            }
            System.out.println("\n");
        }
    }

    private static String[] getViewedPageIds(int index) {
        if (System.currentTimeMillis() % 100 < 70) {
            return PAGE_LIST.get(index % PAGE_LIST.size());
        } else {
            int[] startEnd = Utility.getRangeStartEnd(RANDOM_PAGE_ARRAY.length);
            return Arrays.copyOfRange(RANDOM_PAGE_ARRAY, startEnd[0], startEnd[1]);
        }
    }

    public static ViewedPage[] getViewedPages(int index) throws Exception {
        String[] ids = getViewedPageIds(index);
        ViewedPage[] pages = new ViewedPage[ids.length];
        for (int i = 0; i < pages.length; i++) {
            pages[i] = new ViewedPage(ids[i]);
        }
        return pages;
    }

    public static ViewedPage[] getAllViewedPages() throws Exception {
        List<String> allIds = new ArrayList<String>();
        for (int i = 0; i < PAGE_LIST.size(); i++) {
            allIds.addAll(Arrays.asList(PAGE_LIST.get(i)));
        }
        String[] ids = allIds.toArray(new String[allIds.size()]);
        ViewedPage[] pages = new ViewedPage[ids.length];
        for (int i = 0; i < pages.length; i++) {
            pages[i] = new ViewedPage(ids[i]);
        }
        return pages;
    }

    public static class ViewedPage {
        public String pageId;
        private static Map<String, List<Header>> CACHED_HEADERS = new HashMap<String, List<Header>>();
        public List<Header> headers;

        public ViewedPage(String pageId) throws Exception {
            this.pageId = pageId;
            if (!CACHED_HEADERS.containsKey(pageId)) {
                headers = Utility.getHeaders(new File(FOLDER_NAME + pageId + ".headers"));
                CACHED_HEADERS.put(pageId, headers);
            } else {
                headers = CACHED_HEADERS.get(pageId);
            }
        }

        public TimedString[] getTimedStrings(String ip, long baseMs) {
            // view all pages within 40 minutes
            TimedString[] tss = new TimedString[headers.size()];
            for (int i = 0; i < tss.length; i++) {
                tss[i] = new TimedString();
                // within 2 seconds, a page's all requests should be sent
                tss[i].ms = baseMs + Utility.nextLong(2400000l);
                tss[i].line = Utility.getAccessLogLine(ip, headers.get(i), tss[i].ms);
            }
            return tss;
        }
    }

}
