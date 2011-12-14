package com.sap.demo.data;

import org.apache.commons.csv.CSVUtils;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/28/11
 * Time: 10:28 AM
 * To change this template use File | Settings | File Templates.
 */
public class LocationParser {
    private static final String FOLDER = "C:\\projects\\data\\GeoLiteCity_20111004\\";

    private static final String LOCATION = FOLDER + "GeoLiteCity-Location.csv";
    private static final String IP_BLOCK = FOLDER + "GeoLiteCity-Blocks.csv";
    private static final String METRO = FOLDER + "metrocodes.csv";

    public static final int INT_NULL = -99999999;
    public static final double DOUBLE_NULL = -99999999d;

    public static Map<Integer, Location> getLocations() throws Exception {
        List<String> locationLines = FileUtils.readLines(new File(LOCATION));
        //List<String> ipLines = FileUtils.readLines(new File(IP_BLOCK));
        //List<String> metroLines = FileUtils.readLines(new File(METRO));
        Map<Integer, Location> locationMap = getLocationMap(locationLines);
//System.out.println("locationLines.size()="+locationLines.size());
//System.out.println("locationMap.size()="+locationMap.size());
        //addIpInfo(locationMap, IP_BLOCK);
        addMetroInfo(locationMap, METRO);
        /*
        int count = 0;
        for (Map.Entry<Integer, Location> entry: locationMap.entrySet()) {
            if (count % 1000 == 0) {
                System.out.println(entry.getValue());
            }
            count++;
        }
        */
        return locationMap;
    }

    private static void addMetroInfo(Map<Integer, Location> locationMap, String metroFilename) throws IOException {
        Map<Integer, Set<Location>> metroLoc = new HashMap<Integer, Set<Location>>();
        for (Map.Entry<Integer, Location> entry : locationMap.entrySet()) {
            if (entry.getValue().metroCode != INT_NULL) {
                Set<Location> set = metroLoc.get(entry.getValue().metroCode);
                if (set == null) {
                    set = new HashSet<Location>();
                }
                set.add(entry.getValue());
                metroLoc.put(entry.getValue().metroCode, set);
            }
        }

        DataInputStream in = new DataInputStream(new FileInputStream(metroFilename));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = br.readLine();
        int count = 0;
        while (line != null) {
            if (count >= 1) {
                String[] values = CSVUtils.parseLine(line);
                Set<Location> locSet = metroLoc.get(Integer.parseInt(values[2]));
                for (Location location : locSet) {
                    location.provinceName = values[0];
                    location.metroName = values[1];
                }
            }
            count++;
            line = br.readLine();
        }
    }

    private static Map<Integer, Location> getLocationMap(List<String> lines) throws IOException {
        Map<Integer, Location> map = new HashMap<Integer, Location>();
        for (int i = 2; i < lines.size(); i++) {
//System.out.println("lines.get(i)="+lines.get(i));
            String[] values = CSVUtils.parseLine(lines.get(i));
            Location location = new Location();
            location.locId = getInt(values[0], INT_NULL);
            location.country = values[1];
            location.region = values[2];
            location.city = values[3];
            location.postalCode = values[4];
            location.latitude = getDouble(values[5], DOUBLE_NULL);
            location.longitude = getDouble(values[6], DOUBLE_NULL);
            location.metroCode = getInt(values[7], INT_NULL);
            location.areaCode = getInt(values[8], INT_NULL);
            map.put(location.locId, location);
        }
        return map;
    }

    private static int getInt(String val, int ifNull) {
        int num = 0;
        try {
            num = Integer.parseInt(val);
        } catch (Exception e) {
            num = ifNull;
        }
        return num;
    }

    private static double getDouble(String val, double ifNull) {
        double num = 0;
        try {
            num = Double.parseDouble(val);
        } catch (Exception e) {
            num = ifNull;
        }
        return num;
    }

    public static class Location {
        public int locId;
        public String country;
        public String region;
        public String city;
        public String postalCode;
        public double latitude;
        public double longitude;
        public int metroCode;
        public int areaCode;
        public String provinceName;
        public String metroName;

        @Override
        public String toString() {
            return "Location{" +
                    ", locId=" + locId +
                    ", country='" + country + '\'' +
                    ", region='" + region + '\'' +
                    ", city='" + city + '\'' +
                    ", postalCode='" + postalCode + '\'' +
                    ", latitude=" + latitude +
                    ", longitude=" + longitude +
                    ", metroCode=" + metroCode +
                    ", areaCode=" + areaCode +
                    ", provinceName='" + provinceName + '\'' +
                    ", metroName='" + metroName + '\'' +
                    '}';
        }
    }
}
