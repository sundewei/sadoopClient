package com.sap.demo.amazon.movie.bean;

import java.util.Collection;
import java.util.HashSet;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/28/11
 * Time: 12:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class Review {
    private String asin;
    private String movieName;
    private String customer;
    private int rating;
    private String dateString;
    private Collection<String> filenames = new HashSet<String>();

    public String getAsin() {
        return asin;
    }

    public void setAsin(String asin) {
        this.asin = asin;
    }

    public void addFilename(String filename) {
        filenames.add(filename);
    }

    public String getMovieName() {
        return movieName;
    }

    public void setMovieName(String movieName) {
        this.movieName = movieName;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    public String getDateString() {
        return dateString;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public Collection<String> getFilenames() {
        return filenames;
    }

    @Override
    public String toString() {
        return "Review{" +
                "movieName='" + movieName + '\'' +
                ", filename='" + filenames.iterator().next() + '\'' +
                ", customer='" + customer + '\'' +
                ", rating=" + rating +
                ", dateString='" + dateString + '\'' +
                '}';
    }
}
