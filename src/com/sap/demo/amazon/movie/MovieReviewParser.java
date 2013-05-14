package com.sap.demo.amazon.movie;

import com.sap.demo.Utility;
import com.sap.demo.amazon.movie.bean.Review;
import com.sap.demo.data.HtmlPageParser;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashSet;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 12/28/11
 * Time: 11:30 AM
 * To change this template use File | Settings | File Templates.
 */
public class MovieReviewParser {
    private static final String NUMERIC_REG = "((-|\\\\+)?[0-9]+(\\\\.[0-9]+)?)+";
    private static final String RATING_PREFIX = "swSprite s_star_";
    private static final String CONTENT_START_PREFIX = "Newest First";
    private static final String TITLE_START_PREFIX = "<title>";
    private static final String TITLE_END_PREFIX = "</title>";
    private static final String AMAZON_TITLE_PREFIX = "Amazon.com: Customer Reviews: ";
    private static final String USERNAME_START_PREFIX = "<span style = \"font-weight: bold;\">";
    private static final String USERNAME_END_PREFIX = "</span>";
    private static final String DATE_STRING_START_PREFIX = "<nobr>";
    private static final String DATE_STRING_END_PREFIX = "</nobr>";
    private static final String ASIN_START_PREFIX = "<span class=\"asinReviewsSummary\" name=\"";
    private static final String ASIN_END_PREFIX = "\"";

    private static Collection<Review> getReviews(File source) throws Exception {
        Collection<Review> reviews = new HashSet<Review>();
        String content = FileUtils.readFileToString(source);
        String movieName = getMovieName(content);
        int asinStart = content.indexOf(ASIN_START_PREFIX) + ASIN_START_PREFIX.length();
        int asinEnd = content.indexOf(ASIN_END_PREFIX, asinStart + 1);
        String asin = content.substring(asinStart, asinEnd);
        content = content.substring(content.indexOf(CONTENT_START_PREFIX), content.length());
        int index = content.indexOf(RATING_PREFIX, 0);
        while (index >= 0) {
            String ratingString = content.substring(index, content.indexOf("\"", index + 1));
//System.out.print(ratingString + ", getRating=" + getRating(ratingString) + ", ");
            int rawDateStringStart = content.indexOf(DATE_STRING_START_PREFIX, index) + DATE_STRING_START_PREFIX.length();
            int rawDateStringEnd = content.indexOf(DATE_STRING_END_PREFIX, index + 1);
            //String dateString = getDateString(content.substring(rawDateStringStart, rawDateStringEnd));
//System.out.print("dateString=" + dateString + ", ");
            int usernameStart = content.indexOf(USERNAME_START_PREFIX, index) + USERNAME_START_PREFIX.length();
            int usernameEnd = content.indexOf(USERNAME_END_PREFIX, usernameStart);

            //String username = content.substring(usernameStart, usernameEnd);
//System.out.println(content.substring(usernameStart, usernameEnd));
            String customer = content.substring(usernameStart, usernameEnd);
            String dateString = null;
            try {
                dateString = getDateString(content.substring(rawDateStringStart, rawDateStringEnd));
            } catch (ParseException pe) {
                dateString = null;
            }
            if (customer.indexOf("\n") > 0 || dateString == null) {
                System.out.println("Problem... in " + source.getName() + " review size = " + reviews.size());
            } else {
                Review review = new Review();
                review.setMovieName(movieName);
                review.setCustomer(customer);
                review.setRating(getRating(ratingString));
                review.setDateString(dateString);
                review.addFilename(source.getName());
                review.setAsin(asin);
                reviews.add(review);
            }
            index = content.indexOf(RATING_PREFIX, index + 1);
        }
        return reviews;
    }

    private static String getDateString(String raw) throws Exception {
        return Utility.SIMPLE_DATE_FORMAT.format(Utility.MEDIUM_DATE_FORMAT.parse(raw));
    }

    private static int getRating(String ratingString) {
        // swSprite s_star_5_0
        int start = RATING_PREFIX.length();
        int end = ratingString.indexOf("_", start + 1);
        return Integer.parseInt(ratingString.substring(start, end));
    }

    private static String getMovieName(String content) {
        int start = content.indexOf(TITLE_START_PREFIX);
        int end = content.indexOf(TITLE_END_PREFIX, start + 1);
        return content.substring(start + TITLE_START_PREFIX.length(), end).replace(AMAZON_TITLE_PREFIX, "");
    }

    public static void saveAmazonMovieFromRawFiles(String folderName) throws Exception {
        Collection<Review> reviews = new HashSet<Review>();
        //String folderName = "C:\\projects\\data\\AmazonMovieData\\";
        File folder = new File(folderName);
        File[] moviePageFiles = folder.listFiles();
        for (File moviePageFile : moviePageFiles) {
            reviews.addAll(getReviews(moviePageFile));
        }
        Utilities.saveAmazonMovieData(reviews);
    }

    public static void downloadAmazonMovieReviewPages(String baseFolder, String asin) throws Exception {
        String urlPrefix = "http://www.amazon.com/product-reviews/";
        String urlFirst = urlPrefix + asin + "/ref=cm_cr_pr_top_link_2?ie=UTF8&showViewpoints=0&pageNumber=";
        //"http://www.amazon.com/Beauty-Beast-Paige-OHara/product-reviews/B003DZX3SA/ref=cm_cr_pr_top_link_2?ie=UTF8&showViewpoints=0&pageNumber=";
        String urlLast = "&sortBy=bySubmissionDateDescending";
        System.out.println("asin=" + asin);
        System.out.println(urlFirst + 2 + urlLast);
        int index = 1;

        while (true) {
            HtmlPageParser.HtmlPageInfo info =
                    HtmlPageParser.getHtmlPageInfo("",
                            urlFirst + index + urlLast);
            String filename = baseFolder + File.separator + asin + ".htm" + index;
            File file = new File(filename);
            FileUtils.write(file, info.content);
            if (file.length() < 100000) {
                System.out.println("Found " + filename + " with " + file.length() + " bytes, delete it and exit...");
                file.delete();
                break;
            }
            index++;
            System.out.println(index);
        }
    }


    public static void main(String[] arg) throws Exception {
        saveAmazonMovieFromRawFiles("C:\\projects\\data\\AmazonMovieData\\");

        //String asin = "B000W7F5SS";
        //downloadAmazonMovieReviewPages("C:\\projects\\data\\AmazonMovieData", asin);
    }
}
