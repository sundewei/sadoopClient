package com.sap.data.service;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/9/12
 * Time: 3:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class SqlScriptToHanaWorker implements Worker {
    public void transferData() {
    }

    public void validateArguments(Configuration configuration) throws IllegalArgumentException {
        Configuration.throwIfEmpty(configuration, Configuration.HANA_PATH_NAME_KEY);
        Configuration.throwIfEmpty(configuration, Configuration.HANA_ERROR_PATH_NAME_KEY);
        Configuration.throwIfEmpty(configuration, Configuration.TARGET_TABLE_NAME_KEY);
    }

    private static String getControlFileContent(String table, String csvLocalPath, String errorFileLocalPath)
            throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("IMPORT DATA\n");
        sb.append("INTO TABLE ").append(table).append("\n");
        sb.append("FROM '").append(csvLocalPath).append("' \n");
        sb.append("ERROR LOG '").append(errorFileLocalPath).append("'");
        return sb.toString();
    }
}
