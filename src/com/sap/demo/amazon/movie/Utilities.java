package com.sap.demo.amazon.movie;

import com.sap.demo.Utility;
import com.sap.demo.amazon.movie.bean.Review;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/28/11
 * Time: 3:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class Utilities {
    public static void saveAmazonMovieData(Collection<Review> reviews) throws Exception {
        Connection conn = Utility.getConnection();
        Map<String, String> movieNameAsinMap = new TreeMap<String, String>();
        Collection<String> userNames = new TreeSet<String>();
        System.out.println("reviews.size()=" + reviews.size());
        for (Review review : reviews) {
            movieNameAsinMap.put(review.getMovieName(), review.getAsin());
            userNames.add(review.getCustomer());
        }
        System.out.println("Saving amazon movies");
        saveAmazonMovies(conn, movieNameAsinMap, true);
        System.out.println("Saving amazon users");
        saveAmazonUsers(conn, userNames, true);
        deleteAmazonMovieReviews(conn);
        int index = 0;
        for (Review review : reviews) {

            saveReview(conn, review);
            index++;
            if (index % 100 == 0) {
                System.out.println("Saving reviews: " + index);
            }
        }
        Utility.close(conn);
    }

    public static void deleteAmazonMovieReviews(Connection conn) throws SQLException {
        String deleteQuery = " DELETE FROM AMAZON_MOVIE_REVIEWS ";
        PreparedStatement deleteStmt = conn.prepareStatement(deleteQuery);
        deleteStmt.execute();
        Utility.close(deleteStmt);
    }

    public static void saveAmazonMovies(Connection conn, Map<String, String> movieNameAsinMap, boolean deleteOldData) throws SQLException {
        String deleteQuery = " DELETE FROM AMAZON_MOVIES ";
        String query = " INSERT INTO AMAZON_MOVIES VALUES (?, ?, ?)";
        PreparedStatement pstmt = conn.prepareStatement(query);
        if (deleteOldData) {
            PreparedStatement deleteStmt = conn.prepareStatement(deleteQuery);
            deleteStmt.execute();
            Utility.close(deleteStmt);
        }

        int id = 0;
        for (Map.Entry<String, String> entry : movieNameAsinMap.entrySet()) {
            pstmt.setInt(1, id);
            pstmt.setString(2, entry.getKey());
            pstmt.setString(3, entry.getValue());
            pstmt.addBatch();
            id++;
        }
        pstmt.executeBatch();
        Utility.close(pstmt);
    }

    public static void saveAmazonUsers(Connection conn, Collection<String> usernames, boolean deleteOldData) throws SQLException {
        String query = " INSERT INTO AMAZON_USERS VALUES (?, ?)";
        String deleteQuery = " DELETE FROM AMAZON_USERS ";
        if (deleteOldData) {
            PreparedStatement deleteStmt = conn.prepareStatement(deleteQuery);
            deleteStmt.execute();
            Utility.close(deleteStmt);
        }
        PreparedStatement pstmt = conn.prepareStatement(query);
        int id = 1000000;
        for (String username : usernames) {
            pstmt.setInt(1, id);
            pstmt.setString(2, username);
            pstmt.addBatch();
            id++;
        }
        pstmt.executeBatch();
        Utility.close(pstmt);
    }

    public static void saveReview(Connection conn, Review review) throws SQLException {
        String query = "INSERT INTO AMAZON_MOVIE_REVIEWS VALUES (?, ?, ?, ?) ";
        PreparedStatement pstmt = conn.prepareStatement(query);
        pstmt.setInt(1, getUserId(conn, review.getCustomer()));
        pstmt.setInt(2, getMovieId(conn, review.getMovieName()));
        pstmt.setInt(3, review.getRating());
        pstmt.setString(4, review.getDateString());
        pstmt.execute();
        Utility.close(pstmt);
    }

    public static int getUserId(Connection conn, String username) throws SQLException {
        String query = "SELECT ID FROM AMAZON_USERS WHERE customer = ?";
        PreparedStatement preparedStatement = conn.prepareStatement(query);
        preparedStatement.setString(1, username);
        ResultSet rs = preparedStatement.executeQuery();
        int id = -1;
        while (rs.next()) {
            id = rs.getInt(1);
        }
        Utility.close(rs);
        Utility.close(preparedStatement);
        return id;
    }

    public static int getMovieId(Connection conn, String name) throws SQLException {
        String query = "SELECT ID FROM AMAZON_MOVIES WHERE name = ?";
        PreparedStatement preparedStatement = conn.prepareStatement(query);
        preparedStatement.setString(1, name);
        ResultSet rs = preparedStatement.executeQuery();
        int id = -1;
        while (rs.next()) {
            id = rs.getInt(1);
        }
        Utility.close(rs);
        Utility.close(preparedStatement);
        return id;
    }
}
