package com.sap.mapred;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.conf.IFileSystem;
import com.sap.hadoop.task.ITask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 8/18/11
 * Time: 10:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class ProductTrend implements ITask {
    private static final Logger LOG = Logger.getLogger(ProductTrend.class.getName());

    private String outputPath;
    private String inputPath;

    private static Set<String> PRODUCTS = new HashSet<String>();

    private static Map<String, String> ALL_PRODUCTS = new HashMap<String, String>();

    private static final NumberFormat PERCENT_FORMATTER = NumberFormat.getPercentInstance();

    private static final int INTERVAL_MS = 2000;

    private static final double RELEVANCE_PERCENTAGE = 0.05d;

    static {

        PERCENT_FORMATTER.setMinimumFractionDigits(2);
        PERCENT_FORMATTER.setMaximumFractionDigits(2);
        PERCENT_FORMATTER.setMinimumIntegerDigits(2);
        PERCENT_FORMATTER.setMaximumIntegerDigits(2);

        ALL_PRODUCTS.put("B00005UP2L", "KitchenAid-KSM150PSOB-Artisan-5-Quart-Mixer");
        ALL_PRODUCTS.put("B0001A7VHO", "Wild-Planet-70041-Motion-Alarm");
        ALL_PRODUCTS.put("B0001WWKU0", "Wild-Planet-70055-Voice-Scrambler");
        ALL_PRODUCTS.put("B0006N01AU", "Intex-Recreation-Giant-59252EP-Inflatable");
        ALL_PRODUCTS.put("B000C239HM", "Cuisinart-Nonstick-Hard-Anodized-14-Piece-Cookware");
        ALL_PRODUCTS.put("B000I55OJO", "Razor-Aggressive-Youth-Multi-sport-Helmet");
        ALL_PRODUCTS.put("B000PKZ8EI", "Bosch-FGR7DQI-Platinum-Fusion-Spark");
        ALL_PRODUCTS.put("B000V2BYJI", "Travel-SoundDock®-Portable-Digital-System");
        ALL_PRODUCTS.put("B000V2FJAS", "Bose-SoundDock-Portable-Digital-System");
        ALL_PRODUCTS.put("B0011NVMO8", "Canon-55-250mm-4-0-5-6-Telephoto-Digital");
        ALL_PRODUCTS.put("B0015AARJI", "PlayStation-Dualshock-Wireless-Controller-Black-3");
        ALL_PRODUCTS.put("B001ADOUHA", "Loreal-Colour-Lipstick-415-Cherry");
        ALL_PRODUCTS.put("B001T9N0EO", "Sony-BRAVIA-KDL-46V5100-46-Inch-1080p");
        ALL_PRODUCTS.put("B002BRZ6UE", "Crysis-2-Playstation-3");
        ALL_PRODUCTS.put("B002S0YPUG", "Razor-Wild-Style-Kick-Scooter");
        ALL_PRODUCTS.put("B002UOR17Y", "Antec-EA-380D-Power-Supply");
        ALL_PRODUCTS.put("B002Y27P3M", "Kindle-Wireless-Reading-Display-Generation");
        ALL_PRODUCTS.put("B0035FZJHQ", "Canon-T2i-Digital-3-0-Inch-18-55mm");
        ALL_PRODUCTS.put("B003924UCK", "Panasonic-VIERA-TC-P50G25-50-Inch-Plasma");
        ALL_PRODUCTS.put("B003AUF1XI", "Night-Vision-Infrared-Stealth-Binoculars");
        ALL_PRODUCTS.put("B003ES61EE", "AmazonBasics-Lens-Pen-Cleaning-System");
        ALL_PRODUCTS.put("B003FZA9OE", "FitFlop-Womens-Superboot-Tall-Toning");
        ALL_PRODUCTS.put("B003GDFU", "VIZIO-XVT373SV-37-Inch-Internet-Application");
        ALL_PRODUCTS.put("B003JI62HU", "Peg-Perego-Polaris-Outlaw-Pink");
        ALL_PRODUCTS.put("B003TOFVP8", "Amana-Front-Washer-NFW7300WW-White");
        ALL_PRODUCTS.put("B003VUO6H4", "PlayStation-3-160GB-System");
        ALL_PRODUCTS.put("B0043EVXUK", "umi-Toddler-Sandal-Infant-Little");
        ALL_PRODUCTS.put("B004J3V90Y", "Canon-T3i-Digital-Imaging-18-55mm");
        ALL_PRODUCTS.put("B004MDSTW2", "Michelin-Pilot-Road-Rear-Tire");
        ALL_PRODUCTS.put("B004QL6OCW", "Michelin-Symmetry-Radial-Tire-60R16");
        ALL_PRODUCTS.put("B004V2BRC8", "Michael-Antonio-Womens-Theronn-Platform");
        ALL_PRODUCTS.put("B004V2BXZ4", "Michael-Antonio-Womens-Ladina-Pump");
        ALL_PRODUCTS.put("B004VWKV5C", "Michael-Antonio-Womens-Dress-Shoes");
        ALL_PRODUCTS.put("B0050SYY5E", "Halo-Combat-Evolved-Anniversary-Xbox-360");
        ALL_PRODUCTS.put("B0050SYZCQ", "Xbox-360-Gears-Limited-Console-Bundle");
        ALL_PRODUCTS.put("B00510JKAA", "LG-37LV3500-Accessory-Cables-Cleaning");
        ALL_PRODUCTS.put("B005D7PDYI", "KitchenAid-Refurbished-Artisan-Stand-Mixer");

        // PS3 Console
        PRODUCTS.add("B003VUO6H4");

        // Canon T3i
        PRODUCTS.add("B004J3V90Y");

        // Sony BRAVIA TV 46V5100 46-Inch
        PRODUCTS.add("B001T9N0EO");

        // Michael-Antonio-Womens-Dress-Shoes
        PRODUCTS.add("B004VWKV5C");

    }

    public ProductTrend(String customizedFolder) {
        if (customizedFolder != null && !customizedFolder.endsWith(File.separator)) {
            customizedFolder = customizedFolder + File.separator;
        }
        this.inputPath = customizedFolder;
    }

    /**
     * The map class
     */
    public static class ProductTrendMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text MAP_OUT_VALUE = new Text();
        private static final Text MAP_OUT_KEY = new Text();

        public void map(LongWritable inKey, Text inValue, Context context)
                throws IOException, InterruptedException {
            System.out.println("In ProductTrendMapper...");
            // Get the visited timestamp and product list
            String timeProducts = inValue.toString().split("\t")[1];
            System.out.println("timeProducts=" + timeProducts);
            // Now divide the list by comma
            String[] timeProduct = timeProducts.split(",");

            String nowP = null;
            String prevP = null;

            long nowMs = 0;
            long prevMs = 0;

            for (String tps : timeProduct) {
                String[] tp = tps.split("_");
                if (tp.length == 1) {
                    break;
                }
                nowP = tp[1];
                nowMs = Long.parseLong(tp[0]);

                if (prevMs != 0 && ((nowMs - prevMs) < INTERVAL_MS)) {
                    MAP_OUT_KEY.set(prevP);
                    MAP_OUT_VALUE.set(nowP);

                    if (PRODUCTS == null || PRODUCTS.size() == 0 || PRODUCTS.contains(prevP)) {
                        context.write(MAP_OUT_KEY, MAP_OUT_VALUE);
                    }
                }
                prevMs = nowMs;
                prevP = nowP;
            }
        }
    }

    /**
     * The reduce class
     */
    public static class ProductTrendReducer
            extends Reducer<Text, Text, Text, Text> {
        private static final Text REDUCE_OUT_VALUE = new Text();
        private static final Text REDUCE_OUT_KEY = new Text();

        public void reduce(Text inKey, Iterable<Text> inValues, Context context)
                throws IOException, InterruptedException {
            String nowProduct = inKey.toString();
            String outKeyString = ALL_PRODUCTS.get(nowProduct).replace("-", " ");
            Map<String, Integer> pairCountMap = new HashMap<String, Integer>();
            // Read the Map's output
            int pairCount = 0;
            for (Text value : inValues) {
                pairCount++;
                String nextProduct = value.toString();
                String pair = nowProduct + "_" + nextProduct;
                // increase the count for each product pair so we can compute the percentage later
                Integer count = pairCountMap.get(pair);
                if (count == null) {
                    pairCountMap.put(pair, 1);
                } else {
                    count = new Integer(count.intValue() + 1);
                    pairCountMap.put(pair, count);
                }
            }

            // Create a reverse ordering TreeMap so we can sort the pair based on the counts in "Descending" order
            Map<String, String> percentageMap = new TreeMap<String, String>(Collections.reverseOrder());
            for (Map.Entry<String, Integer> pc : pairCountMap.entrySet()) {
                if (((double) pc.getValue() / (double) pairCount) > RELEVANCE_PERCENTAGE) {
                    String nextProduct = pc.getKey().split("_")[1];
                    String value = "," + ALL_PRODUCTS.get(nextProduct).replace("-", " ");
                    String key = outKeyString + "," + PERCENT_FORMATTER.format((double) pc.getValue() / (double) pairCount);
                    percentageMap.put(key, value);
                }
            }

            // Now write the percentages to the output
            for (Map.Entry<String, String> kv : percentageMap.entrySet()) {
                REDUCE_OUT_VALUE.set(kv.getValue());
                REDUCE_OUT_KEY.set(kv.getKey());
                context.write(REDUCE_OUT_KEY, REDUCE_OUT_VALUE);
            }
        }
    }

    public String getOutputPath() {
        return outputPath;
    }

    public String getInputPath() {
        return inputPath;
    }

    public Job getMapReduceJob() throws Exception {
        // Get a configuration from the Hadoop jars in the classpath at the server side
        ConfigurationManager cm = new ConfigurationManager("I827779", "hadoopsap");
        Configuration configuration = cm.getConfiguration();

        // The output folder MUST NOT be created, Hadoop will do it automatically
        IFileSystem filesystem = cm.getFileSystem();

        if (inputPath == null) {
            inputPath = cm.getRemoteFolder() + "accessLogs/parsed/";
        }
        outputPath = inputPath + "trend/";

        // Delete the output directory if it already exists
        if (filesystem.exists(outputPath)) {
            filesystem.deleteDirectory(outputPath);
        }
        Job job = new Job(configuration, "ProductTrend");
        // This is a must step to tell Hadoop to load the jar file containing this class
        job.setJarByClass(ProductTrend.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ProductTrend.ProductTrendMapper.class);
        job.setReducerClass(ProductTrend.ProductTrendReducer.class);

        //job.setInputFormatClass(FileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    public static void main(String[] arg) throws Exception {
        ProductTrend lp;
        if (arg.length > 0) {
            lp = new ProductTrend(arg[0]);
        } else {
            lp = new ProductTrend(null);
        }
        lp.getMapReduceJob().waitForCompletion(true);
    }

}
