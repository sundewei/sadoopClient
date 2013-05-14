package com.sap.demo.pos;

import com.sap.demo.pos.beans.AmazonProduct;
import com.sap.http.HttpGetUtil;
import org.apache.commons.io.FileUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 2/6/12
 * Time: 2:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class AmazonProductPageParser {
    private static int NEXT_ID = 50000;

    NumberFormat usMoneyFormat = new DecimalFormat("$###,###.###");
    NumberFormat usNumberFormat = new DecimalFormat("###,###.###");

    private AmazonProduct getAmazonProduct(WebDriver webDriver, String htmlPage) throws IOException, ParseException {

        AmazonProduct amazonProduct = new AmazonProduct();
        int slashIndex = htmlPage.lastIndexOf("\\");
        amazonProduct.setAsin(htmlPage.substring(slashIndex + 1).replace(".htm", ""));

        String url = "file:///" + htmlPage;
        webDriver.get(url);

        List<WebElement> prodImageTags = webDriver.findElements(new By.ById("prodImage"));
        String imageUrlString = null;
        if (prodImageTags.size() > 0) {
            imageUrlString = prodImageTags.get(0).getAttribute("src");
        } else {
            prodImageTags = webDriver.findElements(new By.ByXPath("//img[@onload]"));
            imageUrlString = prodImageTags.get(0).getAttribute("src");
        }
        String filename = "C:\\projects\\dataCollector\\amazonData\\products\\image\\" + amazonProduct.getAsin() + imageUrlString.substring(imageUrlString.lastIndexOf("."));
        File imageFile = new File(filename);
        if (!imageFile.exists()) {
            imageFile.createNewFile();
            System.out.println("imageUrlString=" + imageUrlString);
            System.out.println("filename=" + filename);
            HttpGetUtil.getBinaryToFile(new URL(imageUrlString), filename);
        }

        /*
        List<WebElement> priceSpan = webDriver.findElements(new By.ById("actualPriceValue"));
        if (priceSpan.size() >= 1) {
            String possiblePrice = priceSpan.get(0).getText();
            if (!possiblePrice.contains("To")) {
                amazonProduct.setPrice(usMoneyFormat.parse(possiblePrice).doubleValue());
            }
        }

        if (amazonProduct.getByCompany() == null) {
            WebElement byCompany = webDriver.findElement(new By.ByXPath("//div[@class=\"buying\"]/span[1]/a[1]"));
            if (byCompany != null) {
                amazonProduct.setByCompany(byCompany.getText().trim());
            }
        }

        if (amazonProduct.getByCompany() == null) {
            WebElement brand = webDriver.findElement(new By.ById("brandLink"));
            if (brand != null) {
                amazonProduct.setByCompany(brand.getText().trim());
            }
        }

        List<WebElement> salesRanks = webDriver.findElements(new By.ById("SalesRank"));
        if (salesRanks.size() > 0) {
            WebElement salesRank = salesRanks.get(0);
            amazonProduct.setCategoryNote(salesRank.getText().trim());
        } else {
            WebElement dropDown = webDriver.findElement(new By.ByXPath("//select[@id=\"searchDropdownBox\"]/option[@selected=\"selected\"]"));
            amazonProduct.setCategoryNote(dropDown.getText().trim());
        }
        ///*
        if (amazonProduct.getAvgRating() < 0) {
            List<WebElement> numOfCustReview = webDriver.findElements(new By.ByPartialLinkText("customer reviews"));
            if (numOfCustReview.size() > 0) {
                String strNum = numOfCustReview.get(0).getText().split(" ")[0];
                amazonProduct.setNumOfReviews(usNumberFormat.parse(strNum).intValue());
            } else {
                numOfCustReview = webDriver.findElements(new By.ByPartialLinkText("customer review"));
                if (numOfCustReview.size() > 0) {
                    amazonProduct.setNumOfReviews(1);
                }
            }
            List<WebElement> score = webDriver.findElements(new By.ByXPath("//span[@name=\"" + amazonProduct.getAsin() + "\"]/a[1]/span[1]/span[1]"));
            if (score.size() > 0) {
                amazonProduct.setAvgRating(Float.parseFloat(score.get(0).getText().substring(0, 3)));
            } else {
                List<WebElement> scoreList = webDriver.findElements(new By.ByXPath("//span[@class=\"crAvgStars\"]/span[@class=\"asinReviewsSummary\" and @name=\"" + amazonProduct.getAsin() + "\"]"));
                if (scoreList.size() == 1) {
                    amazonProduct.setAvgRating(Float.parseFloat(scoreList.get(0).getText().substring(0, 3)));
                }
            }
        }

        if (amazonProduct.getTitle() == null) {
            WebElement title = webDriver.findElement(new By.ById("btAsinTitle"));
            amazonProduct.setTitle(title.getText());
        }

        if (amazonProduct.getDescription() == null) {
            List<WebElement> descriptionList = webDriver.findElements(new By.ById("productDescription"));
            if (descriptionList.size() > 0) {
                amazonProduct.setDescription(descriptionList.get(0).getText().replace("Product Description", "").trim());
            } else {
                List<WebElement> psContent = webDriver.findElements(new By.ById("ps-content"));
                if (psContent.size() > 0) {
                    amazonProduct.setDescription(psContent.get(0).getText().replace("Book Description", "").trim());
                } else {
                    WebElement metaDescription = webDriver.findElement(new By.ByXPath("//meta[@name=\"description\"]"));
                    amazonProduct.setDescription(metaDescription.getAttribute("content").trim());
                }
            }
        }
        */
        return amazonProduct;
    }

    public static void getItemInfo() throws Exception {
        WebDriver webDriver = Utility.getWebDriver(5);
        String folder = "C:\\projects\\dataCollector\\amazonData\\products\\";
        String byCompanyFolder = "C:\\projects\\dataCollector\\amazonData\\products\\info\\";
        String byAuthorFolder = "C:\\projects\\dataCollector\\amazonData\\products\\info\\";
        String categoryFolder = "C:\\projects\\dataCollector\\amazonData\\products\\info\\";

        File folderFile = new File(folder);
        AmazonProductPageParser parser = new AmazonProductPageParser();
        int index = 0;
        for (File file : folderFile.listFiles()) {
            if (file.isFile() && file.getName().contains(".htm")) {
                String byCompanyFilename = byCompanyFolder + file.getName().replace(".htm", ".byCompany");
                //String byAutorFilename = byAuthorFolder + file.getName().replace(".htm", ".byAuthor");
                String byCategoryFilename = categoryFolder + file.getName().replace(".htm", ".category");
                File byCompanyFile = new File(byCompanyFilename);
                //File byAutorFile = new File(byAutorFilename);
                File byCategoryFile = new File(byCategoryFilename);
                if (!byCompanyFile.exists() && !byCategoryFile.exists() /*  */) {
                    byCompanyFile.createNewFile();
                    byCategoryFile.createNewFile();
                    AmazonProduct amazonProduct = parser.getAmazonProduct(webDriver, file.getAbsolutePath());

                    if (amazonProduct.getByCompany() != null) {
                        FileUtils.write(new File(byCompanyFilename), "\"" + amazonProduct.getAsin() + "\",\"" + amazonProduct.getByCompany() + "\"\n");
                    }
                    /*
                    if (amazonProduct.getAuthor() != null) {
                        FileUtils.write(new File(byAutorFilename), "\"" + amazonProduct.getAsin() + "\",\"" + amazonProduct.getByCompany() + "\"\n");
                    }
                    */
                    if (amazonProduct.getCategoryNote() != null) {
                        FileUtils.write(new File(byCategoryFilename), amazonProduct.getCategoryNote());
                    }
                    System.out.println(index + ", created " + byCategoryFilename);
                }
            }
            index++;
        }
    }

    public static void main(String[] arg) throws Exception {
        NEXT_ID = Integer.parseInt(arg[0]);
        System.out.println("Starting with " + NEXT_ID);
        populateItemDim();
    }

    public static synchronized int getNextId() {
        NEXT_ID++;
        return NEXT_ID;
    }

    public static void populateItemDim() throws Exception {

        Connection conn = DatabaseUtility.getConnection();
        String folder = "C:\\projects\\dataCollector\\amazonData\\products\\";
        String workFolder = "C:\\projects\\dataCollector\\amazonData\\products\\work\\";
        String noRatingFolder = "C:\\projects\\dataCollector\\amazonData\\products\\notValid\\noRating\\";
        String noPriceFolder = "C:\\projects\\dataCollector\\amazonData\\products\\notValid\\noPrice\\";
        String noCategoriesFolder = "C:\\projects\\dataCollector\\amazonData\\products\\notValid\\noCategories\\";
        String noNumOfReviewsFolder = "C:\\projects\\dataCollector\\amazonData\\products\\notValid\\noNumOfReviews\\";
        File folderFile = new File(folder);
        WebDriver webDriver = Utility.getWebDriver(1);
        int index = 0;

        String query = "INSERT INTO HADOOP.item_dim VALUES (?,?,?,?,?, ?,?,?,?,?)";
        PreparedStatement pstmt = conn.prepareStatement(query);
        for (File file : folderFile.listFiles()) {
            if (index >= 0) {
                if (file.isFile()) {
                    File workFlag = new File(workFolder + file.getName() + ".work");
                    File imageFile = new File("C:\\projects\\dataCollector\\amazonData\\products\\image\\" + file.getName().replace(".html", "") + ".jpg");
                    System.out.println("C:\\projects\\dataCollector\\amazonData\\products\\image\\" + file.getName().replace(".htm", "") + ".jpg");
                    if (!workFlag.exists() || !imageFile.exists()) {
                        workFlag.createNewFile();
                        System.out.println(index + ", " + file.getName());
                        AmazonProductPageParser parser = new AmazonProductPageParser();
                        AmazonProduct amazonProduct = parser.getAmazonProduct(webDriver, file.getAbsolutePath());
                        /*
                        boolean moved = false;
                        if (amazonProduct.getCategoryNote() == null) {
                            System.out.println("NO CATEGORIES: " + file.getName());
                            System.out.println("About to move " + file.getName() + " to " + noCategoriesFolder);
                            file.renameTo(new File(noCategoriesFolder + file.getName()));
                            moved = true;
                        }

                        if (amazonProduct.getPrice() < 0) {
                            System.out.println("NO PRICE:: " + file.getName());
                            System.out.println("About to move " + file.getName() + " to " + noPriceFolder);
                            file.renameTo(new File(noPriceFolder + file.getName()));
                            moved = true;
                        }

                        if (amazonProduct.getAvgRating() < 0) {
                            System.out.println("NO RATING:" + file.getName());
                            System.out.println("About to move " + file.getName() + " to " + noRatingFolder);
                            file.renameTo(new File(noRatingFolder + file.getName()));
                            moved = true;
                        }


                        if (!moved && !amazonProduct.valid()) {
                            System.out.println("NOT VALID: " + amazonProduct);
                            break;
                        }
                        */

                        /*
                        if (!moved && amazonProduct.getNumOfReviews() < 0) {
                            System.out.println("NO NUM OF REVIEWS: " + file.getName());
                            //System.out.println("About to move " + file.getName() + " to " + noNumOfReviewsFolder);
                            //file.renameTo(new File(noNumOfReviewsFolder + file.getName()));
                        }

                        if (!moved && amazonProduct.getDescription().length() > 4000) {
                            System.out.println("LONG DESCRIPTION: " + file.getName());
                        }
                        */
                        /*
                        if (!moved && amazonProduct.valid()) {
                            int nextId = getNextId();
                            System.out.println("Inserting " + nextId + ", " + amazonProduct.getAsin());
                            pstmt.setInt(1, nextId);
                            pstmt.setString(2, amazonProduct.getDescription(5000));
                            pstmt.setString(3, amazonProduct.getAsin());
                            //if (amazonProduct.getByCompany() != null) {
                                pstmt.setInt(4, 0);
                            //} else {
                                //pstmt.setObject(4, null);
                            //}


                            pstmt.setString(5, amazonProduct.getTitle());
                            pstmt.setInt(6, amazonProduct.getNumOfReviews());

                            pstmt.setFloat(7, amazonProduct.getAvgRating());
                            pstmt.setDouble(8, amazonProduct.getPrice() * (0.5 + Math.random() / 2));
                            pstmt.setDouble(9, amazonProduct.getPrice());
                            pstmt.setInt(10, -1);
                            try {
                                pstmt.execute();
                            } catch (Exception e) {
                                System.out.println("Unable to insert " + amazonProduct.getAsin() + " " + e.getMessage());
                                pstmt.close();
                                conn.prepareStatement(query);
                            }
                        } */
                    }

                }
            }
            index++;
        }
        conn.close();
    }

    /*
    private AmazonProduct getAmazonProduct(String htmlPage) throws IOException, ParseException  {
        File htmlFile = new File(htmlPage);
        AmazonProduct amazonProduct = new AmazonProduct();
        amazonProduct.setAsin(htmlFile.getName().replace(".htm", ""));
        BufferedReader reader = new BufferedReader(new FileReader(htmlFile));
        String line = reader.readLine();
        String price = null;
        String byCompany = null;
        String author = null;
        while (line != null) {
            if (price == null) {
                price = getValue(line, null, "<span id=\"actualPriceValue\"><b class=\"priceLarge\">", "</b></span>");
                if (price != null && amazonProduct.getPrice() < 0) {
                    amazonProduct.setPrice(usMoneyFormat.parse(price).doubleValue());
                }
            }

            if (byCompany == null) {
                byCompany = getValue(line, "by&#160;", ">", "<");
                if (byCompany == null) {
                    byCompany = getValue(line, "by ", ">", "<");
                }
                if (byCompany == null) {
                    byCompany = getValue(line, null, ">", "<", "field-brandtextbin");
                }
                if (byCompany != null && amazonProduct.getByCompany() == null) {
                    amazonProduct.setByCompany(byCompany);
                }
            }

            if (author == null) {
                author = getValue(line, null, ">", "<", "(Author)");
                if (author == null) {
                    author = getValue(line, null, ">", "<", "(Author, Editor)");
                }
                if (author != null && amazonProduct.getAuthor() == null) {
                    amazonProduct.setAuthor(author);
                }
            }

            if (amazonProduct.getFeatures() == null && line.contains("<h2>Product Features</h2>")) {
                amazonProduct.setFeatures(getNextRows(reader, new String[]{"</ul>"}, "<li>", "</li>"));
            }

            if (amazonProduct.getTechnicalDetails() == null && line.contains("<h2>Technical Details</h2>")) {
                amazonProduct.setTechnicalDetails(getNextRows(reader, new String[]{"</ul>", "See more technical details"}, "<li>", "</li>"));
            }

            if (amazonProduct.getProductDetails() == null && line.contains("<h2>Product Details</h2>")) {
                amazonProduct.setProductDetails(getNextRows(reader, new String[]{"</ul>"}, "<li>", "</li>"));
            }
            line = reader.readLine();
        }
        reader.close();
        return amazonProduct;
    }

    private List<String> getNextRows(BufferedReader reader, String[] endLineKeywords, String startKey, String endKey)
            throws IOException {
        List<String> rowValues = null;
        String line = reader.readLine();
        while (line != null) {
            for (String endLineKeyword: endLineKeywords) {
                if (line.contains(endLineKeyword)) {
                    break;
                }
            }

            String value = getValue(line, null, startKey, endKey);
            if (value != null) {
                if(rowValues == null) {
                    rowValues = new ArrayList<String>();
                }
                rowValues.add(value);
            }
            line = reader.readLine();
        }

        return rowValues;
    }

    private String getValue(String line, String startedWithKey, String startKey, String endKey, String... containedKeys) {
        if (startedWithKey != null && !line.startsWith(startedWithKey)) {
            return null;
        }

        if (containedKeys != null && containedKeys.length > 0) {
            for (String cKey: containedKeys) {
                if (!line.contains(cKey)) {
                    return null;
                }
            }
        }

        int startIndex = line.indexOf(startKey);
        int endIndex = 0;
        if (startIndex >= 0) {
            endIndex = line.indexOf(endKey, startIndex + 1);
        }

        if (startIndex >= 0 && endIndex > 0) {
            return line.substring(startIndex + startKey.length(), endIndex);
        }
        return null;
    }

    public static void main(String[] arg) throws Exception {
        AmazonProductPageParser appp = new AmazonProductPageParser();
        //String line = "    <td id=\"actualPriceContent\"><span id=\"actualPriceValue\"><b class=\"priceLarge\">$42.99</b></span>";
        //System.out.println(appp.getValue(line, "<span id=\"actualPriceValue\"><b class=\"priceLarge\">", "</b></span>"));
        //System.out.println(appp.getAmazonProduct("C:\\projects\\dataCollector\\amazonData\\products\\noPrice\\1556344732.htm"));

        String folder = "C:\\projects\\dataCollector\\amazonData\\products\\";
        String noCompanyFolder = "C:\\projects\\dataCollector\\amazonData\\products\\notValid\\noCompany\\";
        File folderFile = new File(folder);
        int index = 0;
        File[] files = folderFile.listFiles();
        for (File file: files) {
            file = new File(folder + "0911121013.htm");
            AmazonProduct amazonProduct = appp.getAmazonProduct(file.getAbsolutePath());
            if(!amazonProduct.valid()) {
                System.out.println(amazonProduct);
                System.out.println(file.getAbsolutePath());
                break;
            }
            //if (!amazonProduct.valid()) {
            //    file.renameTo(new File(noCompanyFolder + file.getName()));
            //}
            index++;
        }

    }
    */

}
