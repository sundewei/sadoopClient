package com.sap.demo;

import java.sql.Timestamp;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/18/11
 * Time: 4:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccessEntry {

    public String ip;
    public Timestamp timestamp;
    public String method;
    public String resource;
    public int httpCode;
    public long dataLength;
    public String referrer;
    public String userAgent;

    public String getAttribute(String name) {
        if ("ip".equalsIgnoreCase(name)) {
            return ip;
        } else if ("timestamp".equalsIgnoreCase(name)) {
            return String.valueOf(timestamp.getTime());
        } else if ("method".equalsIgnoreCase(name)) {
            return method;
        } else if ("resource".equalsIgnoreCase(name)) {
            return resource;
        } else if ("httpCode".equalsIgnoreCase(name)) {
            return String.valueOf(httpCode);
        } else if ("dataLength".equalsIgnoreCase(name)) {
            return String.valueOf(dataLength);
        } else if ("referrer".equalsIgnoreCase(name)) {
            return String.valueOf(referrer);
        } else if ("userAgent".equalsIgnoreCase(name)) {
            return String.valueOf(userAgent);
        }

        return null;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IP : ").append(ip).append("\n");
        sb.append("Timestamp : ").append(timestamp).append("\n");
        sb.append("Method : ").append(method).append("\n");
        sb.append("Resource : ").append(resource).append("\n");
        sb.append("HttpCode : ").append(httpCode).append("\n");
        sb.append("Length : ").append(dataLength).append("\n");
        sb.append("Referrer : ").append(referrer).append("\n");
        sb.append("UserAgent : ").append(userAgent).append("\n");
        return sb.toString();
    }

}
