package com.sap.data.service;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 9/27/12
 * Time: 3:37 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Worker {
    public void transferData();

    public void validateArguments(Configuration configuration) throws IllegalArgumentException;
}
