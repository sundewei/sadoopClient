package com.sap.etl.example;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.etl.ContextFactory;
import com.sap.hadoop.etl.ETLStepContextException;
import com.sap.hadoop.etl.IContext;
import com.sap.hadoop.etl.SQLStep;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Apr 7, 2011
 * Time: 12:42:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class SQLExample1 {

    public static void main(String[] arg) throws Exception {
        ConfigurationManager cm = new ConfigurationManager("hadoop", "hadoop");
        IContext context = ContextFactory.createContext(cm);

        ///////////////////////////////////////////////////////////////////////
        // <1> Now create "category" table
        ///////////////////////////////////////////////////////////////////////
        SQLStep createCategoryTable = new SQLStep("CREATE TABLE category");
        createCategoryTable.setSql( " CREATE EXTERNAL TABLE IF NOT EXISTS category " +
                                    " ( article_id INT, name STRING ) " +
                                    "   ROW FORMAT DELIMITED " +
                                    "   FIELDS TERMINATED BY '\t' " +
                                    "   LINES TERMINATED BY '\n'" +
                                    "   STORED AS TEXTFILE ");

        ///////////////////////////////////////////////////////////////////////
        // <2> Load the TSV to "category" table
        ///////////////////////////////////////////////////////////////////////
        SQLStep loadCategoryTable = new SQLStep("LOAD TABLE category");
        loadCategoryTable.setSql(   " LOAD DATA INPATH '" + context.getRemoteWorkingFolder() + "small_category.tsv' " +
                                    " OVERWRITE INTO TABLE category");

        ///////////////////////////////////////////////////////////////////////
        // <3> Now create "sections" table
        ///////////////////////////////////////////////////////////////////////
        SQLStep createSectionsTable = new SQLStep("CREATE TABLE sections");
        createSectionsTable.setSql( " CREATE EXTERNAL TABLE IF NOT EXISTS sections " +
                                    " ( sessionNum BIGINT, " +
                                    "   parent_id BIGINT, " +
                                    "   ordinal INT, " +
                                    "   article_id INT, " +
                                    "   name STRING, " +
                                    "   xml STRING ) " +
                                    "   ROW FORMAT DELIMITED " +
                                    "   FIELDS TERMINATED BY '\t' " +
                                    "   LINES TERMINATED BY '\n'" +
                                    "   STORED AS TEXTFILE ");

        ///////////////////////////////////////////////////////////////////////
        // <4> Load the TSV to "sections" table
        ///////////////////////////////////////////////////////////////////////
        SQLStep loadSectionsTable = new SQLStep("LOAD TABLE sections");
        loadSectionsTable.setSql(   " LOAD DATA INPATH '" + context.getRemoteWorkingFolder() + "small_sections.tsv' " +
                                    " OVERWRITE INTO TABLE sections");

        context.addStep(createCategoryTable);                      // Add <1>
        context.addStep(createSectionsTable);                      // Add <3>

        context.addStep(loadCategoryTable, createCategoryTable);   // Add <2> and make it depend on <1>
        context.addStep(loadSectionsTable, createSectionsTable);   // Add <4> and make it depend on <3>

        context.runSteps();
    }


    /*
   SQLStep createTableArticles = new SQLStep("CREATE TABLE articles");
       createTableArticles.setSql(" CREATE EXTERNAL TABLE IF NOT EXISTS articles " +
                                  " ( wpid BIGINT, " +
                                  "   name STRING, " +
                                  "   updated STRING, " +
                                  "   xml STRING, " +
                                  "   text STRING ) " +
                                  "   ROW FORMAT DELIMITED " +
                                  "   FIELDS TERMINATED BY '\t' " +
                                  "   LINES TERMINATED BY '\n'" +
                                  "   STORED AS TEXTFILE ");
       ///////////////////////////////////////////////////////////////////////
       // Load the TSV to "sections" table
       ///////////////////////////////////////////////////////////////////////
       SQLStep loadTableArticles = new SQLStep("LOAD TABLE articles");
       loadTableArticles.setSql(" LOAD DATA INPATH '" + context.getRemoteFolder() + "freebase-wex-2011-04-30-articles.tsv' " +
               " OVERWRITE INTO TABLE articles");
       context.addStep(createTableArticles);
       context.addStep(loadTableArticles, createTableArticles);
    */
}
