package dsp.ass1.manager;

import dsp.ass1.utils.S3Helper;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by user1 on 04/02/2016.
 *   Get jobs links from SQS
     For each job:
         Download job from S3
         Add entry to dictionary
         Make file to append results to
         For each tweet:
            Send (job_id, link) to SQS
         Check SQS message count
         Add additional workers if needed
     Remove the job links from SQS
 */
public class PendingJobsHandler implements Runnable {
    S3Helper s3 = new S3Helper();
    String jobURL = null;
    ArrayList<Job> allJobs = null;
    ArrayList<Job> tweets = null;

    public void run() {
        while (true) {
            // Get Jobs
            jobURL = "https://s3.amazonaws.com/dsp-ass1/pending-jobs/tweetLinks.txt";

            for (String rawJob : s3.getAllObjects(S3Helper.Folders.PENDING_JOBS)) {
                Job job = null;

                try {
                    job = new Job(rawJob.split("\n"));
                } catch (IOException e) {
                    System.err.println("Error parsing job at: " + jobURL);
                    continue;
                }

                allJobs.add(job);

            }
        }
    }
}
