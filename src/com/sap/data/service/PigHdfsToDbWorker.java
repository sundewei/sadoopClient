package com.sap.data.service;

import java.util.LinkedHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/9/12
 * Time: 3:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class PigHdfsToDbWorker implements Worker {
    public void transferData() {
    }

    public void validateArguments(Configuration configuration) throws IllegalArgumentException {
        Configuration.throwIfEmpty(configuration, Configuration.HDFS_PATH_NAME_KEY);
        Configuration.throwIfEmpty(configuration, Configuration.TARGET_TABLE_NAME_KEY);
        Configuration.throwIfEmpty(configuration, Configuration.DRIVER_CLASS_NAME_KEY);
        Configuration.throwIfEmpty(configuration, Configuration.DB_CONN_STRING_KEY);
        Configuration.throwIfEmpty(configuration, Configuration.USERNAME_KEY);
        Configuration.throwIfEmpty(configuration, Configuration.PASSWORD_KEY);
        Configuration.throwIfEmpty(configuration, Configuration.PIG_COMMIT_PER_ROW_KEY);

    }

    private static String getPigControlFileContent(String table, String filePath,
                                                   String dbDriverName, String jdbcConnectionString,
                                                   String dbUsername, String dbPassword,
                                                   int commitPerRow, LinkedHashMap<String, String> parameters) {
        StringBuilder sb = new StringBuilder();
        sb.append("register '/usr/local/pig/ngdbc.jar';\n");
        sb.append("register '/usr/local/pig/piggybank.jar';\n");
        sb.append("CSV_CONTENT = LOAD '").append(filePath).append("' USING PigStorage(',') AS (ONEK_HDFS::DATE_STRING:chararray,ONEK_HDFS::USER_ID:chararray,ONEK_HDFS::OTHER_ID:chararray,ONEK_HDFS::EVENT_TYPE:int,ONEK_HDFS::COMMENTS:chararray);\n");
        sb.append("STORE CSV_CONTENT INTO '/tmp/dbLoad' using org.apache.pig.piggybank.storage.DBStorage('" + dbDriverName + "', '" + jdbcConnectionString + "', '" + dbUsername + "', '" + dbPassword + "', 'insert into ");
        sb.append(table);
        sb.append(" values (?,?,?,?,?)', '");
        sb.append(commitPerRow);
        sb.append("');\n");
        return sb.toString();
    }
}
