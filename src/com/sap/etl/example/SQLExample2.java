package com.sap.etl.example;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.etl.ContextFactory;
import com.sap.hadoop.etl.IContext;
import com.sap.hadoop.etl.SQLStep;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 5/19/11
 * Time: 3:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class SQLExample2 {
    public static void main(String[] arg) throws Exception {
        ConfigurationManager cm = new ConfigurationManager("hadoop", "hadoop");
        IContext context = ContextFactory.createContext(cm);

        // Create the join table
        SQLStep createJoinTable = new SQLStep("CREATE TABLE section_category");
        createJoinTable.setSql( " CREATE EXTERNAL TABLE IF NOT EXISTS section_category " +
                                    " ( sessionNum INT, parent_id INT, section_name STRING, category_name STRING ) " +
                                    "   ROW FORMAT DELIMITED " +
                                    "   FIELDS TERMINATED BY '\t' " +
                                    "   STORED AS TEXTFILE ");

        // Create the table exactly like the join table but we are going to store the section info whose parent sessionNum is NOT NULL
        SQLStep createChildTable = new SQLStep("CREATE TABLE child_section_category");
        createChildTable.setSql( " CREATE EXTERNAL TABLE IF NOT EXISTS child_section_category " +
                                    " ( sessionNum INT, parent_id INT, section_name STRING, category_name STRING ) " +
                                    "   ROW FORMAT DELIMITED " +
                                    "   FIELDS TERMINATED BY '\t' " +
                                    "   STORED AS TEXTFILE ");

        // Create the table exactly like the join table but we are going to store the section info whose parent sessionNum is NULL
        SQLStep createOrphanTable = new SQLStep("CREATE TABLE orphan_section_category");
        createOrphanTable.setSql( " CREATE EXTERNAL TABLE IF NOT EXISTS orphan_section_category " +
                                    " ( sessionNum INT, parent_id INT, section_name STRING, category_name STRING ) " +
                                    "   ROW FORMAT DELIMITED " +
                                    "   FIELDS TERMINATED BY '\t' " +
                                    "   STORED AS TEXTFILE ");

        // The actual Join step
        SQLStep joinTable = new SQLStep("JoinTables");
        joinTable.setSql(
                " INSERT OVERWRITE TABLE section_category " +
                " SELECT sections.sessionNum, sections.parent_id, sections.name, category.name " +
                " FROM sections JOIN category  " +
                "      ON (sections.article_id = category.article_id)");

        // The output step to write to 2 tables and 1 directory
        SQLStep multipleOutput = new SQLStep("MultipleOutput");
        multipleOutput.setSql(
                " FROM section_category " +
                " INSERT OVERWRITE TABLE child_section_category SELECT * WHERE section_category.parent_id IS NOT NULL " +
                " INSERT OVERWRITE TABLE orphan_section_category SELECT * WHERE section_category.parent_id IS NULL " +
                " INSERT OVERWRITE DIRECTORY '/user/hadoop/section10/' SELECT * WHERE section_category.parent_id <= 10");

        // The 3 table creation steps can run without dependency
        context.addStep(createJoinTable);
        context.addStep(createChildTable);
        context.addStep(createOrphanTable);

        // The joinTable step needs to depend on the create table step
        context.addStep(joinTable, createJoinTable);

        // The multipleOutput step needs to wait for the joinTable and 2 table creations before it can start
        context.addStep(multipleOutput, createChildTable, createOrphanTable, joinTable);

        context.runSteps();
    }
}
