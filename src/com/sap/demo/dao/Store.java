package com.sap.demo.dao;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/28/11
 * Time: 2:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class Store {
    private String num;
    private String state;
    private String city;
    private String type;
    private String retailerCategory;
    private String region;

    public Store(String num, String retailerCategory, String type, String city, String state, String region) {
        this.num = num;
        this.retailerCategory = retailerCategory;
        this.type = type;
        this.city = city;
        this.state = state;
        this.region = region;
    }

    public String getNum() {
        return num;
    }

    public String getState() {
        return state;
    }

    public String getCity() {
        return city;
    }

    public String getType() {
        return type;
    }

    public String getRetailerCategory() {
        return retailerCategory;
    }

    public String getRegion() {
        return region;
    }

    @Override
    public String toString() {
        return "Store{" +
                "num='" + num + '\'' +
                ", state='" + state + '\'' +
                ", city='" + city + '\'' +
                ", type='" + type + '\'' +
                ", retailerCategory='" + retailerCategory + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
