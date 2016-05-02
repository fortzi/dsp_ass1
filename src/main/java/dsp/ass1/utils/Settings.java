package dsp.ass1.utils;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Created by doubled on 0002, 02, 4, 2016.
 *
 */
public class Settings {
    public final static Region  REGION                = Region.getRegion(Regions.US_EAST_1);
    public final static int     INSTANCE_LIMIT        = 20;
    public final static int     TWEETS_PER_WORKER     = 10;
    public final static int     INSTANCES_PER_QUANTA  = 5;
    public final static String  TERMINATION_ATTRIBUTE = "terminate";
    public final static String  JOB_ID_ATTRIBUTE      = "job_id";
    public final static String  REFUSE_ATTRIBUTE      = "job_refused";
    public final static String  RATIO_ATTRIBUTE       = "workers_files_ratio";
    public final static String  BUCKET_NAME           = "dsp-ass1";
    public final static String  HTTP_ERROR            = "Error 404: Cannot access tweet ";
    public final static String  INSTANCE_WORKER       = "worker";
    public final static String  INSTANCE_MANAGER      = "manager";
    public final static String  ERROR_ATTRIBUTE       = "error_in_manager";
    public final static int     SLEEP_INTERVAL        = 1000;
}

//TODO add function to send N number of messages at the same time (for termination, for tweets)
