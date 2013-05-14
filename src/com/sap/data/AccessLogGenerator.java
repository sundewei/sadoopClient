package com.sap.data;

import org.apache.commons.io.IOUtils;

import java.io.FileOutputStream;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 8/17/11
 * Time: 10:56 AM
 * To change this template use File | Settings | File Templates.
 */
public class AccessLogGenerator {
    static String[] ips = new String[10];
    public static List<String[]> pageList = new ArrayList<String[]>();
    static Random randomGenerator = new Random();
    static DateFormat df = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss Z]");

    static {
        ips[0] = "10.48.58.42";
        ips[1] = "10.48.101.113";
        ips[2] = "169.145.89.205";
        ips[3] = "74.125.225.81";
        ips[4] = "67.195.160.76";
        ips[5] = "198.93.34.21";
        ips[6] = "122.147.51.224";
        ips[7] = "64.208.126.34";
        ips[8] = "210.244.31.148";
        ips[9] = "64.208.126.49";

        // 00000
        pageList.add(new String[]{
                "\"GET /Canon-T3i-Digital-Imaging-18-55mm/dp/B004J3V90Y/ HTTP/1.1\" 200 15390",
                "\"GET /Canon-T2i-Digital-3-0-Inch-18-55mm/dp/B0035FZJHQ/ HTTP/1.1\" 200 15390",
                "\"GET /Canon-55-250mm-4-0-5-6-Telephoto-Digital/dp/B0011NVMO8/ HTTP/1.1\" 200 31536",
                "\"GET /Canon-T2i-Digital-3-0-Inch-18-55mm/dp/B0035FZJHQ/ HTTP/1.1\" 200 15390",
                "\"GET /AmazonBasics-Lens-Pen-Cleaning-System/dp/B003ES61EE HTTP/1.1\" 200 34543",
                "\"GET /Canon-T2i-Digital-3-0-Inch-18-55mm/dp/B0035FZJHQ/ HTTP/1.1\" 200 15390",
        });

        // 00003
        pageList.add(new String[]{
                "\"GET /Night-Vision-Infrared-Stealth-Binoculars/dp/B003AUF1XI/ HTTP/1.1\" 200 302",
                "\"GET /Wild-Planet-70055-Voice-Scrambler/dp/B0001WWKU0/ HTTP/1.1\" 200 302",
                "\"GET /Night-Vision-Infrared-Stealth-Binoculars/dp/B003AUF1XI/ HTTP/1.1\" 200 302",
                "\"GET /Wild-Planet-70041-Motion-Alarm/dp/B0001A7VHO/ HTTP/1.1\" 200 302",
                "\"GET /Night-Vision-Infrared-Stealth-Binoculars/dp/B003AUF1XI/ HTTP/1.1\" 200 302",
        });

        // 00006
        pageList.add(new String[]{
                "\"GET /LG-37LV3500-Accessory-Cables-Cleaning/dp/B00510JKAA/ HTTP/1.1\" 200 302",
                "\"GET /Sony-BRAVIA-KDL-46V5100-46-Inch-1080p/dp/B001T9N0EO/ HTTP/1.1\" 200 12806",
                "\"GET /VIZIO-XVT373SV-37-Inch-Internet-Application/dp/B003GDFU/ HTTP/1.1\" 200 12806",
                "\"GET /Sony-BRAVIA-KDL-46V5100-46-Inch-1080p/dp/B001T9N0EO/ HTTP/1.1\" 200 12806",
                "\"GET /Panasonic-VIERA-TC-P50G25-50-Inch-Plasma/dp/B003924UCK/ HTTP/1.1\" 200 12806",
                "\"GET /Sony-BRAVIA-KDL-46V5100-46-Inch-1080p/dp/B001T9N0EO/ HTTP/1.1\" 200 12806",
        });

        // 00008
        pageList.add(new String[]{
                "\"GET /umi-Toddler-Sandal-Infant-Little/dp/B0043EVXUK/ HTTP/1.1\" 200 833",
                "\"GET /Razor-Wild-Style-Kick-Scooter/dp/B002S0YPUG/ HTTP/1.1\" 200 833",
                "\"GET /Peg-Perego-Polaris-Outlaw-Pink/dp/B003JI62HU/ HTTP/1.1\" 200 833",
                "\"GET /Razor-Aggressive-Youth-Multi-sport-Helmet/dp/B000I55OJO/ HTTP/1.1\" 200 833",
                "\"GET /Peg-Perego-Polaris-Outlaw-Pink/dp/B003JI62HU/ HTTP/1.1\" 200 833",
                "\"GET /Razor-Aggressive-Youth-Multi-sport-Helmet/dp/B000I55OJO/ HTTP/1.1\" 200 833",
                "\"GET /Razor-Aggressive-Youth-Multi-sport-Helmet/dp/B000I55OJO/ HTTP/1.1\" 200 833",
                "\"GET /Razor-Aggressive-Youth-Multi-sport-Helmet/dp/B000I55OJO/ HTTP/1.1\" 200 833",
        });

        // 00009
        pageList.add(new String[]{
                "\"GET /FitFlop-Womens-Superboot-Tall-Toning/dp/B003FZA9OE/ HTTP/1.1\" 200 17543",
                "\"GET /Michael-Antonio-Womens-Ladina-Pump/dp/B004V2BXZ4/ HTTP/1.1\" 200 17543",
                "\"GET /FitFlop-Womens-Superboot-Tall-Toning/dp/B003FZA9OE/ HTTP/1.1\" 200 17543",
                "\"GET /Michael-Antonio-Womens-Dress-Shoes/dp/B004VWKV5C/ HTTP/1.1\" 200 17543",
                "\"GET /Michael-Antonio-Womens-Theronn-Platform/dp/B004V2BRC8/ HTTP/1.1\" 200 17543",
                "\"GET /Loreal-Colour-Lipstick-415-Cherry/dp/B001ADOUHA/ HTTP/1.1\" 200 17543",
                "\"GET /Michael-Antonio-Womens-Theronn-Platform/dp/B004V2BRC8/ HTTP/1.1\" 200 17543",

        });

        // 00011
        pageList.add(new String[]{
                "\"GET /KitchenAid-KSM150PSOB-Artisan-5-Quart-Mixer/dp/B00005UP2L/ HTTP/1.1\" 200 1524",
                "\"GET /Amana-Front-Washer-NFW7300WW-White/dp/B003TOFVP8/ HTTP/1.1\" 200 17677",
                "\"GET /KitchenAid-KSM150PSOB-Artisan-5-Quart-Mixer/dp/B00005UP2L/ HTTP/1.1\" 500 124",
                "\"GET /Amana-Front-Washer-NFW7300WW-White/dp/B003TOFVP8/ HTTP/1.1\" 200 17677",
                "\"GET /KitchenAid-Refurbished-Artisan-Stand-Mixer/dp/B005D7PDYI/ HTTP/1.1\" 500 124",
                "\"GET /Cuisinart-Nonstick-Hard-Anodized-14-Piece-Cookware/dp/B000C239HM/ HTTP/1.1\" 200 17677",
        });

        // 00013
        pageList.add(new String[]{
                "\"GET /Bose-SoundDock-Portable-Digital-System/dp/B000V2FJAS/ HTTP/1.1\" 200 7577",
                "\"GET /Bosch-FGR7DQI-Platinum-Fusion-Spark/dp/B000PKZ8EI/ HTTP/1.1\" 200 7577",
                "\"GET /Bose-SoundDock-Portable-Digital-System/dp/B000V2FJAS/ HTTP/1.1\" 200 7577",
                "\"GET /Travel-SoundDock®-Portable-Digital-System/dp/B000V2BYJI/ HTTP/1.1\" 200 7577",
                "\"GET /Antec-EA-380D-Power-Supply/dp/B002UOR17Y/ HTTP/1.1\" 200 7577",

        });

        // 00014
        pageList.add(new String[]{
                "\"GET /Michelin-Pilot-Road-Rear-Tire/dp/B004MDSTW2/ HTTP/1.1\" 200 75744",
                "\"GET /Intex-Recreation-Giant-59252EP-Inflatable/dp/B0006N01AU/ HTTP/1.1\" 200 75744",
                "\"GET /Michelin-Pilot-Road-Rear-Tire/dp/B004MDSTW2/ HTTP/1.1\" 200 75744",
                "\"GET /Intex-Recreation-Giant-59252EP-Inflatable/dp/B0006N01AU/ HTTP/1.1\" 200 75744",
                "\"GET /Michelin-Pilot-Road-Rear-Tire/dp/B004MDSTW2/ HTTP/1.1\" 200 75744",
                "\"GET /Michelin-Symmetry-Radial-Tire-60R16/dp/B004QL6OCW/ HTTP/1.1\" 200 75744",
                "\"GET /Intex-Recreation-Giant-59252EP-Inflatable/dp/B0006N01AU/ HTTP/1.1\" 200 75744",
        });

        // 00016
        pageList.add(new String[]{
                "\"GET /Kindle-Wireless-Reading-Display-Generation/dp/B002Y27P3M/ HTTP/1.1\" 200 424235",
                "\"GET /Kindle-Wireless-Reading-Display-Generation/dp/B002Y27P3M/ HTTP/1.1\" 500 5478"
        });

        // 00017
        pageList.add(new String[]{
                "\"GET /PlayStation-3-160GB-System/dp/B003VUO6H4/ HTTP/1.1\" 200 15254",
                "\"GET /PlayStation-Dualshock-Wireless-Controller-Black-3/dp/B0015AARJI/ HTTP/1.1\" 200 4253",
                "\"GET /Xbox-360-Gears-Limited-Console-Bundle/dp/B0050SYZCQ/ HTTP/1.1\" 200 14523",
                "\"GET /PlayStation-Dualshock-Wireless-Controller-Black-3/dp/B0015AARJI/ HTTP/1.1\" 200 4253",
                "\"GET /Halo-Combat-Evolved-Anniversary-Xbox-360/dp/B0050SYY5E/ HTTP/1.1\" 200 4253",
                "\"GET /PlayStation-Dualshock-Wireless-Controller-Black-3/dp/B0015AARJI/ HTTP/1.1\" 200 4253",
                "\"GET /PlayStation-Dualshock-Wireless-Controller-Black-3/dp/B0015AARJI/ HTTP/1.1\" 200 4253",
                "\"GET /Crysis-2-Playstation-3/dp/B002BRZ6UE/ HTTP/1.1\" 200 4253",
                "\"GET /PlayStation-Dualshock-Wireless-Controller-Black-3/dp/B0015AARJI/ HTTP/1.1\" 200 4253",
        });
    }

    private static String getPaddedNumberString(int num, int length, String padChar) {
        StringBuilder sb = new StringBuilder(String.valueOf(num));
        while (sb.length() < length) {
            sb.insert(0, padChar);
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        for (int intDay = 1; intDay <= 31; intDay++) {
            String day = getPaddedNumberString(intDay, 2, "0");
            FileOutputStream out = new FileOutputStream("c:\\data\\newLogs\\localhost_access_log.2011-08-" + day + ".txt", true);
            int pageCount = 0;
            long startMs = 1312182000812l + 86400000l * (intDay - 1);
            long endMs = startMs + 86400000l - 20l;
            long nowMs = startMs;
            TreeMap<Long, String> sortedMsLines = new TreeMap<Long, String>();
            String oldIp = null;
            String nowIp = null;
            while (true) {
                if (nowMs >= endMs) {
                    break;
                }
                int pageIndex = randomGenerator.nextInt(pageList.size());
                nowIp = ips[randomGenerator.nextInt(10)];
                List<MsLine> msLines = getLines(nowIp, nowMs, pageList.get(pageIndex));
                for (MsLine msLine : msLines) {
                    sortedMsLines.put(msLine.ms, msLine.line);
                }
                if (oldIp != null && oldIp.equals(nowIp)) {
                    nowMs += msLines.size() * randomGenerator.nextInt(500);
                } else {
                    nowMs += randomGenerator.nextInt(250);
                }
                pageCount++;
                if (pageCount % 1000 == 0) {
                    System.out.println(new Timestamp(nowMs));
                }
                oldIp = nowIp;
            }
            for (String line : sortedMsLines.values()) {
                IOUtils.write(line, out);
            }
            out.close();

        }
    }

    private static List<MsLine> getLines(String ip, long ms, String[] lines) {

        List<MsLine> outLines = new ArrayList<MsLine>();
        StringBuilder sb = null;
        for (String line : lines) {
            if (randomGenerator.nextInt(100) >= 75) {
                sb = new StringBuilder();
                sb.append(ip).append(" - - ");
                sb.append(df.format(new Timestamp(ms))).append(" ");
                sb.append(line).append("\n");
                ms = ms + randomGenerator.nextInt(3) * randomGenerator.nextInt(500);
                MsLine msLine = new MsLine();
                msLine.ms = ms;
                msLine.line = sb.toString();
                outLines.add(msLine);
            }
        }
        return outLines;
    }

    private static class MsLine {
        long ms;
        String line;
    }
}
