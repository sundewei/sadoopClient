package com.sap.demo;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/3/11
 * Time: 9:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class Session {
    public List<Long> timestamps = new LinkedList<Long>();
    public List<String> itemLookups = new LinkedList<String>();

    public void addItemLookup(Long ts, String itemLookup) {
        timestamps.add(ts);
        itemLookups.add(itemLookup);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < timestamps.size(); i++) {
            sb.append(timestamps.get(i)).append(":").append(itemLookups.get(i)).append("\n");
        }
        return sb.toString();
    }
}
