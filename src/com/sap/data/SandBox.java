package com.sap.data;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 8/19/11
 * Time: 10:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class SandBox {
    static Map<String, String> product = new TreeMap<String, String>();
    public static void main(String[] arg) {
        for (String[] pages: AccessLogGenerator.pageList) {
            for (String page: pages) {
                String[] productId = page.split("/dp/");
                String[] ppp = productId[1].split(" ");
                product.put(ppp[0].replace("/", ""), productId[0].replace("/", "").replace("\"GET ", ""));
            }
        }

        for (Map.Entry<String, String> pid: product.entrySet()) {
            System.out.println(pid.getKey() + " : " + pid.getValue());
        }
    }
}
