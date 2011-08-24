package com.sap.etl.example;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.etl.ContextFactory;
import com.sap.hadoop.etl.IContext;
import com.sap.hadoop.etl.UploadStep;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Apr 7, 2011
 * Time: 11:58:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class UploadExample {

    private static final String LOCAL_DATA_DIR = "C:\\data\\";

    public static void main(String[] arg) throws Exception {
        ConfigurationManager cm = new ConfigurationManager("I827779", "hadoopsap");
        IContext context = ContextFactory.createContext(cm);

        UploadStep uploadStep1 = new UploadStep("Category");
        uploadStep1.setLocalFilename(LOCAL_DATA_DIR + "small_category.tsv");
        uploadStep1.setRemoteFilename(context.getRemoteWorkingFolder() + "small_category.tsv");

        UploadStep uploadStep2 = new UploadStep("Section");
        uploadStep2.setLocalFilename(LOCAL_DATA_DIR + "small_sections.tsv");
        uploadStep2.setRemoteFilename(context.getRemoteWorkingFolder() + "small_sections.tsv");
        // If you want step 2 to depend on step 1
        // context.addStep(uploadStep2, uploadStep1);

        // To have both steps run in parallel, add step 1 and 2 like this
        context.addStep(uploadStep1);
        context.addStep(uploadStep2);

        context.runSteps();
    }
}
