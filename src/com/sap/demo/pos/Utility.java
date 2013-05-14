package com.sap.demo.pos;

import com.sap.demo.dao.State;
import org.apache.commons.csv.CSVUtils;
import org.apache.commons.io.FileUtils;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 2/7/12
 * Time: 3:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class Utility {
    public static final String LINUX_BASE_FOLDER = "/home/hadoop/projects/";
    public static final String WINDOWS_BASE_FOLDER = "C:\\projects\\";

    public static final String LINUX_DEST_FOLDER = "/hadoop/user/hadoop/";
    //public static final String WINDOWS_DEST_FOLDER = "F:\\";
    public static final String WINDOWS_DEST_FOLDER = "C:\\projects\\data\\";

    //public static String BASE_FOLDER = LINUX_BASE_FOLDER;
    public static String BASE_FOLDER = WINDOWS_BASE_FOLDER;
    public static String BASE_DEST_FOLDER = WINDOWS_DEST_FOLDER;


    public static WebDriver getWebDriver(int secondsToWait) throws MalformedURLException {
        WebDriver webDriver = new RemoteWebDriver(new URL("http://localhost:9515"), DesiredCapabilities.chrome());
        webDriver.manage().timeouts().implicitlyWait(secondsToWait, TimeUnit.SECONDS);
        return webDriver;
    }

    public static Map<String, State> getStateMap() throws Exception {
        String incomeFilaneme = BASE_FOLDER + "data/state/stateIncome.csv";
        String populationFilaneme = BASE_FOLDER + "data/state/statePopulation.csv";

        List<String> incomeLines = FileUtils.readLines(new File(incomeFilaneme));
        List<String> populationLines = FileUtils.readLines(new File(populationFilaneme));
        long totalIncome = 0l;
        long avgIncome = 0l;
        Map<String, State> stateMap = new HashMap<String, State>();

        for (String line : populationLines) {
            String[] values = CSVUtils.parseLine(line);
            State state = new State(com.sap.demo.Utility.STATE_ABBREVIATION.get(values[2]), values[2]);
            state.setPopulation(Long.parseLong(values[3]));
            state.setPopulationPercentage(Float.parseFloat(values[12].replace("%", "")));
            state.setRegion(com.sap.demo.Utility.STATE_REGION.get(state.getFullName()));
            stateMap.put(state.getAbbreviation(), state);
        }

        for (String line : incomeLines) {
            String[] values = CSVUtils.parseLine(line);
            State state = stateMap.get(com.sap.demo.Utility.STATE_ABBREVIATION.get(values[0]));
            long income = Long.parseLong(values[1]);
            totalIncome += income;
            state.setIncome(income);
        }
        avgIncome = totalIncome / stateMap.size();
        for (Map.Entry<String, State> entry : stateMap.entrySet()) {
            State state = entry.getValue();
            state.setIncomePercentage(((float) state.getIncome() / (float) avgIncome));
        }
        return stateMap;
    }
}