package com.sap.data;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 7/25/11
 * Time: 3:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class GobParser {
    public Map<Long, GObject> getGobMap() throws Exception {
        File gamesCsv = new File("c:\\data\\games.csv");
        List<String> lines = FileUtils.readLines(gamesCsv);
        Map<Long, GObject> gobMap = new HashMap<Long, GObject>();
        boolean seeFirst = false;
        GObject gob = null;
        String name4 = null;
        String value4 = null;

        String name5 = null;
        String value5 = null;

        String name6 = null;
        String value6 = null;
        int count = 0;
        for (String line: lines) {
            count++;
            // skip the first line
            if (!seeFirst) {
                seeFirst = true;
                continue;
            }

            if (seeFirst) {
                String[] values = CSVUtils.parseLine(line);

                if (values[0].length() > 0) {
                    gob = new GObject();
                    name4 = null;
                    value4 = null;

                    name5 = null;
                    value5 = null;

                    name6 = null;
                    value6 = null;
                    gob.setId(Long.parseLong(values[0]));
                    if (values[1].length() > 0) {
                        gob.setName(values[1]);
                    }
                    if (values[2].length() > 0) {
                        gob.setPlatformName(values[2]);
                    }
                    if (values[3].length() > 0) {
                        gob.setCommonName(values[3]);
                    }
                    gobMap.put(gob.getId(), gob);
                }
                if (values[7].length() > 0) {
                    gob.addCollection(values[7]);
                }
//if (values[4].length() > 0) {
//System.out.println("values[4]="+values[4]);
//System.out.println("\n\n Before parsing, name4="+name4+", value4="+value4);
//}
                value4 = (name4 != null) ? values[4] : null;
//if (values[4].length() > 0) {
//System.out.println("value4="+value4);
//}
                name4 = (name4 == null && values[4].length() > 0) ? values[4] : name4;
//if (values[4].length() > 0) {
//System.out.println("name4="+name4);
//}

                if (name4 != null && value4 != null) {
//if (values[4].length() > 0) {
                    gob.setAlias(name4, value4);
//System.out.println("---> Added " + name4 + "="+ value4);
//}
                    name4 = null;
                    value4 = null;
                }

//if (values[5].length() > 0) {
//System.out.println("values[5]="+values[5]);
//System.out.println("\n\n Before parsing, name5="+name5+", value5="+value5);
//}
                value5 = (name5 != null) ? values[5] : null;
//if (values[5].length() > 0) {
//System.out.println("value5="+value5);
//}
                name5 = (value5 == null && name5 == null && values[5].length() > 0) ? values[5] : name5;
//if (values[5].length() > 0) {
//System.out.println("name5="+name5);
//}
                if (name5 != null && value5 != null) {
                    gob.setLocalizedName(name5, value5);
//System.out.println("Added " + name5 + "="+ value5);
                    name5 = null;
                    value5 = null;
                }

                value6 = (name6 != null) ? values[6] : null;
                name6 = (value6 == null && name6 == null && values[6].length() > 0) ? values[6] : name6;
                if (name6 != null && value6 != null) {
                    gob.setMiscName(name6, value6);
                    name6 = null;
                    value6 = null;
                }

            }
        }
        return gobMap;
    }

    public static void main(String[] args) throws Exception {
        GobParser g = new GobParser();
        g.getGobMap();
    }
}
