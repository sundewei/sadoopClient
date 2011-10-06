package com.sap.etl.example;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.etl.ContextFactory;
import com.sap.hadoop.etl.IContext;
import com.sap.hadoop.etl.SQLStep;
import com.sap.hadoop.etl.UploadStep;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 7/5/11
 * Time: 3:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class Tester {
    public static void main(String[] arg) throws Exception {

        ConfigurationManager cm = new ConfigurationManager("I827779", "hadoopsap");
        IContext context = ContextFactory.createContext(cm);

        UploadStep u = new UploadStep("freebase-wex-2011-04-30-sections.tsv");
        u.setLocalFilename("C:\\projects\\freebase-wex-2011-04-30\\freebase-wex-2011-04-30-sections.tsv");
        u.setRemoteFilename(cm.getRemoteFolder() + "freebase-wex-2011-04-30-sections.tsv");

        context.addStep(u);
        context.runSteps();
    }
}
