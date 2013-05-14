package com.sap.demo.amazon.product;

import com.sap.http.HttpGetUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 1/12/12
 * Time: 3:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class SearchResultGetter {
    public static Set<String> getAsins(String resultUrl) throws Exception {
        String urlPrefix = "http://www.amazon.com/s/ref=sr_nr_scat_502394_ln?rh=n%3A502394%2Ck%3Adigital+camera&keywords=digital+camera&ie=UTF8&qid=1326477294&scn=502394&h=3350be905a6dff098bede8ef2a7491e741ad4659#/ref=sr_pg_4?rh=n%3A172282%2Cn%3A%21493964%2Cn%3A502394%2Ck%3Adigital+camera&d=1&keywords=digital+camera&ie=UTF8&qid=1326477325&page=";
        String folder = "C:\\projects\\data\\AmazonProductData\\";
        int index = 1;
        while (true) {
            String filename = folder + "camera+digital_" + index + ".htm";
            String content = HttpGetUtil.getUrlContent(urlPrefix + index);
            FileUtils.write(new File(filename), content);
            if (index > 4) {
                break;
            }
            index++;
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        getAsins("aaa");
    }
}
