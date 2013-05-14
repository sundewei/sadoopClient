package com.sap;

import com.sap.hadoop.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/20/12
 * Time: 11:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class Test2 {
    public static void main(String[] arg) throws Exception {
        ConfigurationManager configurationManager = new ConfigurationManager("hadoop", "abcd1234");
        Configuration configuration = configurationManager.getConfiguration();
        Path base = new Path("/data/accessLogs/");
        FileSystem fileSystem = base.getFileSystem(configuration);
        FileStatus[] fileStatuses = fileSystem.listStatus(base);
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDir() && fileStatus.getPath().getName().startsWith("2012")) {
                // /data/accessLogs/2012-12-31/2012-12-31.log
                Path logPath = fileSystem.listStatus(fileStatus.getPath())[0].getPath();
                Path newPath = new Path("/data/accessLogs/" + logPath.getName());
                fileSystem.rename(logPath, newPath);
            }
        }
        fileSystem.close();
    }
}
