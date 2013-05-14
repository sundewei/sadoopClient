package com.sap.demo;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/21/11
 * Time: 4:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class TimedString implements Comparable {
    public long ms;
    public String line;

    public TimedString() {
    }

    public TimedString(long ms, String line) {
        this.ms = ms;
        this.line = line;
    }

    public int compareTo(Object other) {
        TimedString otherTimedString = (TimedString) other;
        return this.ms - otherTimedString.ms > 0 ? 1 : -1;
    }

    public String toString() {
        return this.line;
    }
}
