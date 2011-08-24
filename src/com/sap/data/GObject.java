package com.sap.data;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 7/25/11
 * Time: 3:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class GObject {
    private long id;
    private String name;
    private String platformName;
    private String commonName;

    private Map<String, String> alias = new HashMap<String, String>();
    private Map<String, String> localizedNames = new HashMap<String, String>();
    private Map<String, String> miscNames = new HashMap<String, String>();
    private Collection<String> collection = new HashSet<String>();

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }

    public String getCommonName() {
        return commonName;
    }

    public void setCommonName(String commonName) {
        this.commonName = commonName;
    }

    public void setAlias(String name, String value) {
        alias.put(name, value);
    }

    public void setLocalizedName(String name, String value) {
        localizedNames.put(name, value);
    }

    public void setMiscName(String name, String value) {
        miscNames.put(name, value);
    }

    public void addCollection(String str) {
        collection.add(str);
    }

    public Map<String, String> getAlias() {
        return alias;
    }

    public Map<String, String> getLocalizedNames() {
        return localizedNames;
    }

    public Map<String, String> getMiscNames() {
        return miscNames;
    }

    public Collection<String> getCollection() {
        return collection;
    }
}
