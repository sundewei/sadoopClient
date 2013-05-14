package com.sap.data.service;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 9/27/12
 * Time: 3:02 PM
 * To change this template use File | Settings | File Templates.
 */
public final class Configuration {
    private Mode mode;

    public static enum Mode {
        PIG_HDFS_TO_DB,
        SQOOP_HDFS_TO_DB,
        SQL_SCRIPT_TO_HANA,
    }

    public static final String HDFS_PATH_NAME_KEY = "hdfsPathNameKey";
    public static final String HANA_PATH_NAME_KEY = "hanaPathNameKey";
    public static final String HANA_ERROR_PATH_NAME_KEY = "hanaErrPathNameKey";
    public static final String TARGET_TABLE_NAME_KEY = "targetTableNameKey";
    public static final String PIG_COMMIT_PER_ROW_KEY = "pigCommitPerRowKey";
    //See your driver documentation for the proper format of this string :
    public static final String DB_CONN_STRING_KEY = "dbConnStringKey";
    //Provided by your driver documentation. In this case, a MySql driver is used :
    public static final String DRIVER_CLASS_NAME_KEY = "driverClassNameKey";
    public static final String USERNAME_KEY = "usernameKey";
    public static final String PASSWORD_KEY = "passwordKey";

    private Map<String, String> properties;

    public Configuration(Mode mode, Map<String, String> properties) {
        this.mode = mode;
        if (properties != null) {
            this.properties = new HashMap<String, String>(properties);
        } else {
            this.properties = new HashMap<String, String>();
        }
    }

    public String getProperty(String key) {
        return properties.get(key);
    }

    public void setProperty(String key, String value) {
        properties.put(key, value);
    }

    public static void throwIfEmpty(Configuration configuration, String name) throws IllegalArgumentException {
        if (StringUtils.isEmpty(configuration.getProperty(name))) {
            throw new IllegalArgumentException("Property \"" + name + "\" is null from the Configuration.");
        }
    }
}
