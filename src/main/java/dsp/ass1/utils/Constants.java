package dsp.ass1.utils;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Created by doubled on 0002, 02, 4, 2016.
 *
 */
public class Constants {

    public final static Region REGION = Region.getRegion(Regions.US_EAST_1);
    public final static int INSTANCE_LIMIT = 20;
    public final static int TWEETS_PER_WORKER = 10;
    public final static String TERMINATION_MESSAGE = "terminate";
    public final static String JOB_ID_ATTRIBUTE = "job_id";
    public final static String  BUCKET_NAME = "dsp-ass1";
}
