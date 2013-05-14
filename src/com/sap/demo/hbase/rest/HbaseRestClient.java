package com.sap.demo.hbase.rest;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.*;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/2/12
 * Time: 4:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class HbaseRestClient {
    private static final String HBASE_BASE_URL = "http://llbpal36.pal.sap.corp:8888/";
    private DefaultHttpClient defaultHttpClient;

    public HbaseRestClient() {
        defaultHttpClient = new DefaultHttpClient();
    }

    public String getHbaseVersion() throws Exception {
        return getResponseString(HBASE_BASE_URL + "version/cluster", "get", null);
    }

    public String createTable(String mimeType, String tableName, String spec) throws Exception {
        StringEntity entity = new StringEntity(spec, mimeType, "UTF-8");
        return getResponseString(HBASE_BASE_URL + tableName + "/schema", "put", entity);
    }

    public String deleteTable(String tableName) throws Exception {
        return getResponseString(HBASE_BASE_URL + tableName + "/schema", "delete", null);
    }

    private String getResponseString(String url, String method, AbstractHttpEntity entity) throws IOException {
        HttpRequestBase httpMethod;
        if ("post".equalsIgnoreCase(method)) {
            httpMethod = new HttpPost(url);
            if (entity != null) {
                ((HttpPost) httpMethod).setEntity(entity);
            }
        } else if ("put".equalsIgnoreCase(method)) {
            httpMethod = new HttpPut(url);
            if (entity != null) {
                ((HttpPut) httpMethod).setEntity(entity);
            }
        } else if ("delete".equalsIgnoreCase(method)) {
            httpMethod = new HttpDelete(url);
        } else {
            httpMethod = new HttpGet(url);
        }
        HttpResponse response = defaultHttpClient.execute(httpMethod);
        System.out.println("response.getStatusLine().getStatusCode()=" + response.getStatusLine().getStatusCode());
        for (Header header : response.getAllHeaders()) {
            System.out.println(header.getName() + "=" + header.getValue());
        }
        HttpEntity httpEntity = response.getEntity();
        List<String> lines = IOUtils.readLines(httpEntity.getContent());
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    public static void main(String[] arg) throws Exception {
        HbaseRestClient client = new HbaseRestClient();
        String tableSpec =
                "   TableSchema { \n" +
                        "       name: \"TestTable\"\n" +
//                "       ColumnSchema { \n" +
//                "           name: \"personal\" \n" +
//                "       } \n " +
                        "   }";
        //client.deleteTable("TestTable");
        System.out.println(client.createTable(Constants.MIMETYPE_PROTOBUF, "TestTable", tableSpec));
    }
}
