package com.sap.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Header {
    public String url;
    public String method;
    public String resource;
    public String http;
    public int httpStatusCode;
    public Map<String, String> reqMap = new HashMap<String, String>();
    public Map<String, String> resMap = new HashMap<String, String>();

    public void init(List<String> lines) {
        String firstResponseLine = null;
        url = lines.get(0);
        String[] line3Vals = lines.get(2).split(" ");
        method = line3Vals[0];
        resource = line3Vals[1];
        http = line3Vals[2];
        Map<String, String> map = reqMap;
        boolean seenBlankLine = false;
        for (int i = 3; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.length() > 0) {
                if (line.indexOf(": ") > 0) {
                    String[] lineVals = line.split(": ");
                    map.put(lineVals[0], lineVals[1]);
                }
                if (seenBlankLine && firstResponseLine == null) {
                    firstResponseLine = line;
                    String[] responses = firstResponseLine.split(" ");
                    httpStatusCode = Integer.parseInt(responses[1]);
                }
            } else {
                seenBlankLine = true;
            }
            if (line.length() == 0) {
                map = resMap;
            }
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("url = ").append(url).append("\n");
        sb.append("method = ").append(method).append("\n");
        sb.append("resource = ").append(resource).append("\n");
        sb.append("http = ").append(http).append("\n");
        sb.append("httpStatusCode = ").append(httpStatusCode).append("\n");
        sb.append("request: ").append("\n");
        for (Map.Entry<String, String> entry : reqMap.entrySet()) {
            sb.append(entry.getKey()).append(" = ").append(entry.getValue()).append("\n");
        }
        sb.append("response: ").append("\n");
        for (Map.Entry<String, String> entry : resMap.entrySet()) {
            sb.append(entry.getKey()).append(" = ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }
}