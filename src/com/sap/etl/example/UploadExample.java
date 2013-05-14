package com.sap.etl.example;

import com.sap.hadoop.concurrent.ContextFactory;
import com.sap.hadoop.concurrent.IContext;
import com.sap.hadoop.conf.ConfigurationManager;
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
        ConfigurationManager cm = new ConfigurationManager("lroot", "abcd1234");
        IContext context = ContextFactory.createContext(cm);

        // Upload step #1
        UploadStep uploadStep1 = new UploadStep("Category");
        uploadStep1.setLocalFilename(LOCAL_DATA_DIR + "small_category.tsv");
        uploadStep1.setRemoteFilename(context.getRemoteWorkingFolder() + "small_category.tsv");

        // Upload step #2
        UploadStep uploadStep2 = new UploadStep("Section");
        uploadStep2.setLocalFilename(LOCAL_DATA_DIR + "small_sections.tsv");
        uploadStep2.setRemoteFilename(context.getRemoteWorkingFolder() + "small_sections.tsv");

        // To have both steps run in parallel, add step 1 and 2 like this
        context.addStep(uploadStep1);
        context.addStep(uploadStep2);

        // Or let step 2 depend on step 1
        //context.addStep(uploadStep2, uploadStep1);

        // Run the steps in the context
        context.runSteps();

        // These steps are also available
        //DownloadFolderStep
        //DownloadFileStep
        //UploadFolderStep
    }
}
