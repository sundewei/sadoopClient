package com.sap.http;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 1/12/12
 * Time: 3:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class HttpGetUtil {
    public static String getUrlContent(String targetAddress) throws Exception {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        HttpGet httpget = new HttpGet(targetAddress);
        HttpResponse response = httpClient.execute(httpget);
        HttpEntity entity = response.getEntity();
        List<String> lines = IOUtils.readLines(entity.getContent());
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    public static void getBinaryToFile(URL url, String filename) throws IOException {
        InputStream is = null;
        try {
            is = url.openStream();
            IOUtils.copy(is, new FileOutputStream(filename));
        } catch (IOException e) {
            System.err.printf("Failed while reading bytes from %s: %s", url.toExternalForm(), e.getMessage());
            e.printStackTrace();
            // Perform any other exception handling that's appropriate.
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }
}
