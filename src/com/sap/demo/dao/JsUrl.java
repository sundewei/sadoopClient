package com.sap.demo.dao;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/18/11
 * Time: 2:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class JsUrl {
    private int id;
    private String url;

    public JsUrl(int id, String url) {
        this.id = id;
        this.url = url;
    }

    public int getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }

    @Override
    public String toString() {
        return "\"" + id + "\",\"" + url + "\"";
    }
}
