package com.sap.demo;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/30/11
 * Time: 11:09 AM
 * To change this template use File | Settings | File Templates.
 */
public class Bag {
    Map<String, Long> balls = new HashMap<String, Long>();
    private long total = -1;

    public void addBalls(String ballName, long amount) {
        balls.put(ballName, amount);
    }

    private void initTotal() {
        if (total < 0) {
            total = 0;
            for (Long amount : balls.values()) {
                total += amount;
            }
        }
    }

    public String drawBall() {
        initTotal();
        return scanBag(Utility.nextLong(total));
    }

    private String scanBag(long target) {
        long nowTotal = 0;
        for (Map.Entry<String, Long> entry : balls.entrySet()) {
            nowTotal += entry.getValue();
//System.out.println("target = " + target + ", Adding " + entry.getKey()+" : " + entry.getValue() + " balls, total = " + nowTotal);
            if (nowTotal >= target) {
                return entry.getKey();
            }
            /*
            if (oldBallName == null) {
                oldBallName = entry.getKey();
            }
            String nowBallName = entry.getKey();
            nowTotal += entry.getValue();
            if (nowTotal >= target) {
                return oldBallName;
            } else {
                oldBallName = nowBallName;
            }
            */
        }
        return null;
    }

    public static void main(String[] arg) {
        Bag bag = new Bag();
        bag.addBalls("CA", 2000);
        bag.addBalls("NV", 500);
        bag.addBalls("TX", 1600);
        bag.addBalls("NY", 1400);
        bag.addBalls("NJ", 800);
        System.out.println("Try... " + bag.drawBall() + "\n\n\n");

        System.out.println("Try... " + bag.drawBall() + "\n\n\n");

        System.out.println("Try... " + bag.drawBall() + "\n\n\n");
    }
}
