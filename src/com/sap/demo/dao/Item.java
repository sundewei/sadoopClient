package com.sap.demo.dao;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/1/11
 * Time: 3:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class Item {
    private int itemLookup;
    private String category;
    private String subCategory;
    private double unitCost;
    private double unitPrice;

    private String combinedCategory;

    public Item() {
    }

    public Item(int itemLookup, String category, String subCategory, double unitCost, double unitPrice) {
        this.itemLookup = itemLookup;
        this.category = category;
        this.subCategory = subCategory;
        this.unitCost = unitCost;
        this.unitPrice = unitPrice;
        this.combinedCategory = category + "~" + subCategory;
    }

    public String getCombinedCategory() {
        return combinedCategory;
    }

    public int getItemLookup() {
        return itemLookup;
    }

    public String getCategory() {
        return category;
    }

    public String getSubCategory() {
        return subCategory;
    }

    public double getUnitCost() {
        return unitCost;
    }

    public double getUnitPrice() {
        return unitPrice;
    }

    public Map<String, String> columns = new HashMap<String, String>();

    public void setColumn(String name, String value) {
        columns.put(name, value);
    }

    public String getColumn(String name) {
        return columns.get(name);
    }

    public String toCsvString() {
        /*
        this.itemLookup = itemLookup;
        this.category = category;
        this.subCategory = subCategory;
        this.unitCost = unitCost;
        this.unitPrice = unitPrice;
        this.combinedCategory = category + "~" + subCategory;
         */
        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        sb.append(itemLookup);
        sb.append("\"");
        sb.append(",");

        sb.append("\"");
        sb.append(category);
        sb.append("\"");
        sb.append(",");

        sb.append("\"");
        sb.append(subCategory);
        sb.append("\"");
        sb.append(",");

        sb.append("\"");
        sb.append(unitCost);
        sb.append("\"");
        sb.append(",");

        sb.append("\"");
        sb.append(unitPrice);
        sb.append("\"");

        return sb.toString();
    }
}
