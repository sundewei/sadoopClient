package com.sap.demo.dao;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/18/11
 * Time: 3:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class CombinedCategoryInfo implements Comparable<CombinedCategoryInfo> {
    private String name;
    private double averageProfit;
    private String category;
    private String subCategory;

    public CombinedCategoryInfo(String name, double averageProfit) {
        this.name = name;
        this.averageProfit = averageProfit;
        String[] catSubCat = name.split("~");
        category = catSubCat[0];
        subCategory = catSubCat[1];
    }

    public String getName() {
        return name;
    }

    public double getAverageProfit() {
        return averageProfit;
    }

    public String getCategory() {
        return category;
    }

    public String getSubCategory() {
        return subCategory;
    }

    public int compareTo(CombinedCategoryInfo o) {
        return this.name.compareTo(o.getName());
        /*
        if (this == o) {
            return 0;
        } else {
            return (int)((averageProfit - o.averageProfit) * 10000);
        }
        */
    }
}
