package dsp.ass1.utils;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Created by doubled on 0002, 02, 4, 2016.
 */
public class Constants {

    public static Region REGION = Region.getRegion(Regions.US_EAST_1);
    public static int INSTANCE_LIMIT = 20;
    public static String TERMINATION_MESSAGE = "terminate";
    public static int TWEETS_PER_WORKER = 10;
}
