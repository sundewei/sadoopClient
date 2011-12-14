package com.sap.demo.dao;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/30/11
 * Time: 12:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class State {
    private String abbreviation;
    private String fullName;
    private String region;
    private long income;
    private float incomePercentage;
    private long population;
    private float populationPercentage;

    public State(String abbreviation, String fullName) {
        this.abbreviation = abbreviation;
        this.fullName = fullName;
    }

    public String getAbbreviation() {
        return abbreviation;
    }

    public void setAbbreviation(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public long getIncome() {
        return income;
    }

    public void setIncome(long income) {
        this.income = income;
    }

    public long getPopulation() {
        return population;
    }

    public void setPopulation(long population) {
        this.population = population;
    }

    public float getPopulationPercentage() {
        return populationPercentage;
    }

    public void setPopulationPercentage(float populationPercentage) {
        this.populationPercentage = populationPercentage;
    }

    public float getIncomePercentage() {
        return incomePercentage;
    }

    public void setIncomePercentage(float incomePercentage) {
        this.incomePercentage = incomePercentage;
    }

    @Override
    public String toString() {
        return "State{" +
                "abbreviation='" + abbreviation + '\'' +
                ", fullName='" + fullName + '\'' +
                ", region='" + region + '\'' +
                ", income=" + income +
                ", incomePercentage=" + incomePercentage +
                ", population=" + population +
                ", populationPercentage=" + populationPercentage +
                '}';
    }
}
