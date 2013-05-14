package com.sap.demo.amazon.product;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.datatransfer.DataFlavor;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.awt.image.BufferedImage;
import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 1/16/12
 * Time: 11:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class Test {
    private static int NOW_IMG_CENTER_POS_X = -1;
    private static int NOW_IMG_CENTER_POS_Y = -1;

    private static void printMousePosition() throws Exception {
        while (true) {
            Thread.sleep(100);
            System.out.println("(" + MouseInfo.getPointerInfo().getLocation().x + ", " + MouseInfo.getPointerInfo().getLocation().y + ")");
        }
    }

    private static void highlightPrimaryWindow(Robot robot, int delayMs) {
        // Move back to the main display
        robot.mouseMove(1884, 95);
        robot.delay(delayMs);
        robot.mousePress(InputEvent.BUTTON1_MASK);
        robot.delay(delayMs);
        robot.mouseRelease(InputEvent.BUTTON1_MASK);
        robot.delay(delayMs);
        robot.keyPress(KeyEvent.VK_CONTROL);
        robot.delay(delayMs);
        robot.keyPress(KeyEvent.VK_F);
        robot.delay(delayMs);
        robot.keyRelease(KeyEvent.VK_CONTROL);
        robot.delay(delayMs);
        robot.keyRelease(KeyEvent.VK_F);
        robot.delay(delayMs);
        robot.keyPress(KeyEvent.VK_DELETE);
        robot.delay(delayMs);
        robot.keyRelease(KeyEvent.VK_DELETE);
        robot.delay(delayMs);
        robot.mousePress(InputEvent.BUTTON1_MASK);
        robot.delay(delayMs);
        robot.mouseRelease(InputEvent.BUTTON1_MASK);
        robot.delay(delayMs);
        robot.mousePress(InputEvent.BUTTON1_MASK);
        robot.delay(delayMs);
        robot.mouseRelease(InputEvent.BUTTON1_MASK);
        robot.delay(delayMs);
        robot.mousePress(InputEvent.BUTTON1_MASK);
        robot.delay(delayMs);
        robot.mouseRelease(InputEvent.BUTTON1_MASK);
        robot.delay(delayMs);
    }

    private static void searchByCtrlF(Robot robot, int delayMs, boolean first) {
        if (first) {
            robot.keyPress(KeyEvent.VK_PAGE_UP);
            robot.delay(delayMs * 2);
            robot.keyRelease(KeyEvent.VK_PAGE_UP);
            robot.delay(delayMs * 2);
            robot.keyPress(KeyEvent.VK_PAGE_UP);
            robot.delay(delayMs * 2);
            robot.keyRelease(KeyEvent.VK_PAGE_UP);
            robot.delay(delayMs * 2);
            robot.keyPress(KeyEvent.VK_PAGE_UP);
            robot.delay(delayMs * 2);
            robot.keyRelease(KeyEvent.VK_PAGE_UP);
            robot.delay(delayMs * 2);
            robot.keyPress(KeyEvent.VK_PAGE_UP);
            robot.delay(delayMs * 2);
            robot.keyRelease(KeyEvent.VK_PAGE_UP);
            robot.delay(delayMs * 2);
            robot.keyPress(KeyEvent.VK_PAGE_UP);
            robot.delay(delayMs * 2);
            robot.keyRelease(KeyEvent.VK_PAGE_UP);
        }
        robot.delay(delayMs);
        robot.keyPress(KeyEvent.VK_CONTROL);
        robot.delay(delayMs);
        robot.keyPress(KeyEvent.VK_F);
        robot.delay(delayMs);
        robot.keyRelease(KeyEvent.VK_CONTROL);
        robot.delay(delayMs);
        robot.keyRelease(KeyEvent.VK_F);
        robot.delay(delayMs);
        robot.keyPress(KeyEvent.VK_DELETE);
        robot.delay(delayMs);
        robot.keyRelease(KeyEvent.VK_DELETE);
        robot.delay(delayMs);
        if (first) {
            robot.keyRelease(KeyEvent.VK_PAGE_UP);
            robot.delay(delayMs * 2);
            robot.keyPress(KeyEvent.VK_PAGE_UP);
            robot.delay(delayMs * 2);
        }
    }

    private static void pressKey(int delayMs, int... keys) {
        for (int key : keys) {

        }
    }

    public static void main(String[] arg) throws Exception {
        //printMousePosition();

        Robot robot = new Robot();
        Keyboard keyboard = new Keyboard(robot);

        highlightPrimaryWindow(robot, 500);

        Rectangle rectangle = new Rectangle(0, 0, 1900, 1100);

        int oldRowNum = 1;
        for (int i = 1; i < 2; i++) {
            boolean rowChange = false;
            int rowNum = getRowNum(i);
            System.out.println("Working on " + i + " , at row : " + rowNum);
            if (rowNum > oldRowNum) {
                oldRowNum = rowNum;
                rowChange = true;
            }
            if (i == 1) {
                searchByCtrlF(robot, 20, true);
            } else {
                searchByCtrlF(robot, 20, false);
            }
            keyboard.type(i + ".", 25);
            BufferedImage afterImage = robot.createScreenCapture(rectangle);
            File afterFile = new File("c:\\data\\after.png");
            ImageIO.write(afterImage, "png", afterFile);
            String url = null;

            getHyperlinkUrl(null, afterImage, robot, rowChange);

            System.out.println(url);
            System.out.println("\n\n");
        }


    }

    private static int getRowNum(int num) {
        if (num % 3 == 0) {
            return num / 3;
        } else {
            return num / 3 + 1;
        }
    }

    private static String getHyperlinkUrl(Toolkit toolkit, BufferedImage afterImage, Robot robot, boolean rowChanged) throws Exception {
        int foundX = -1;
        int foundY = -1;
        boolean found = false;
        int startX = 216;
        int startY = 300;
        int delayMs = 2000;
        if (NOW_IMG_CENTER_POS_Y == -1 && NOW_IMG_CENTER_POS_X == -1) {
            NOW_IMG_CENTER_POS_Y = startY;
            NOW_IMG_CENTER_POS_X = startX;
        }
        if (rowChanged) {
            NOW_IMG_CENTER_POS_X = startX;
            NOW_IMG_CENTER_POS_Y = NOW_IMG_CENTER_POS_Y + 243;
        }
        System.out.println("Start X, Y = " + NOW_IMG_CENTER_POS_X + ", " + NOW_IMG_CENTER_POS_Y);
        for (int y = NOW_IMG_CENTER_POS_Y; y < 1100; y++) {
            for (int x = NOW_IMG_CENTER_POS_X; x < 1541; x++) {
                //if (afterImage.getRGB(x, y) != beforeImage.getRGB(x, y)) {
                if (afterImage.getRGB(x, y) == -106 || afterImage.getRGB(x, y) == -27086) {
                    foundX = x;
                    foundY = y;
                    System.out.println("(" + x + ", " + y + ") = " + afterImage.getRGB(x, y));
                    found = true;
                    if (found) {
                        break;
                    }
                }

                //}
            }
            if (found) {
                break;
            }

        }
        if (foundX == -1 && foundY == -1) {
            throw new Exception("Cannot find the highlighted color...");
        }

        int imageCenterPosX = foundX + 92;
        int imageCenterPosY = foundY + 92;

        NOW_IMG_CENTER_POS_X = foundX;
        NOW_IMG_CENTER_POS_Y = foundY;

        System.out.println("imageCenterPosX=" + imageCenterPosX + ", imageCenterPosY=" + imageCenterPosY);


        robot.mouseMove(imageCenterPosX, imageCenterPosY);
        robot.delay(delayMs);
        robot.mousePress(InputEvent.BUTTON3_MASK);
        robot.delay(delayMs);
        robot.mouseRelease(InputEvent.BUTTON3_MASK);
        robot.delay(delayMs);

        imageCenterPosX += 11;
        if (imageCenterPosY >= 815) {
            imageCenterPosY -= 170;
        } else {
            imageCenterPosY += 122;
        }
        robot.mouseMove(imageCenterPosX, imageCenterPosY);
        robot.delay(delayMs);
        robot.mousePress(InputEvent.BUTTON1_MASK);
        robot.delay(delayMs);
        robot.mouseRelease(InputEvent.BUTTON1_MASK);
        robot.delay(delayMs);
        //Toolkit toolkit = Toolkit.getDefaultToolkit();
        String url = Toolkit.getDefaultToolkit().getSystemClipboard().getContents(new Object()).getTransferData(DataFlavor.stringFlavor).toString();
        return url;
    }
}
