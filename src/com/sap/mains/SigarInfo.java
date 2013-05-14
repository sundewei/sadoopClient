package com.sap.mains;

import org.hyperic.sigar.Sigar;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 8/13/12
 * Time: 12:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class SigarInfo {

    Sigar sigar = new Sigar();
    long processId;

    public SigarInfo() {
        processId = sigar.getPid();
    }

    public SigarInfo(long targetId) {
        processId = targetId;
    }
}
