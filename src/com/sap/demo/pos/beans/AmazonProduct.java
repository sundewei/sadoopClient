package com.sap.demo.pos.beans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 2/6/12
 * Time: 2:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class AmazonProduct {
    private int id;

    private double price = -1d;
    private double cost = -1d;
    private String byCompany;
    private int byId;
    private String author;
    private String asin;
    private float avgRating = -1f;
    private String title;
    private String description;
    private int topCategoryId;

    private int numOfReviews = 0;
    private String categoryNote;
    private Map<Integer, List<Category>> categories;

    public int getTopCategoryId() {
        return topCategoryId;
    }

    public void setTopCategoryId(int topCategoryId) {
        this.topCategoryId = topCategoryId;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public int getById() {
        return byId;
    }

    public void setById(int byId) {
        this.byId = byId;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void addCategory(int level, Category category) {
        if (categories == null) {
            categories = new HashMap<Integer, List<Category>>();
        }

        List<Category> categoryList = categories.get(level);
        if (categoryList == null) {
            categoryList = new ArrayList<Category>();
        }

        categoryList.add(category);

        categories.put(level, categoryList);
    }

    public List<Category> getCategories(int level) {
        return categories.get(level);
    }

    public String getCategoryNote() {
        return categoryNote;
    }

    public void setCategoryNote(String categoryNote) {
        this.categoryNote = categoryNote;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getByCompany() {
        return byCompany;
    }

    public void setByCompany(String byCompany) {
        this.byCompany = byCompany;
    }

    public String getAsin() {
        return asin;
    }

    public void setAsin(String asin) {
        this.asin = asin;
    }

    public boolean valid() {
        return asin != null &&
                price > 0 &&
                (byCompany != null || author != null) &&
                title != null &&
                description != null &&
                numOfReviews >= 0;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public float getAvgRating() {
        return avgRating;
    }

    public void setAvgRating(float avgRating) {
        this.avgRating = avgRating;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public String getDescription(int maxLength) {
        if (description.length() >= (maxLength - 200)) {
            return description.substring(0, maxLength - 201);
        }
        return description;
    }

    public void setDescription(String description) {
        description = description.replaceAll("\n", " ");
        this.description = description;
    }

    public int getNumOfReviews() {
        return numOfReviews;
    }

    public void setNumOfReviews(int numOfReviews) {
        this.numOfReviews = numOfReviews;
    }
}
