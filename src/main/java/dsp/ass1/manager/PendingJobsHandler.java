package dsp.ass1.manager;

/**
 * Created by user1 on 04/02/2016.
 * Get jobs links from SQS
     For each job:
         Download job from S3
         Add entry to dictionary
         Make file to append results to
         For each tweet:
         Send (job_id, link) to SQS
         Check SQS message count
         Add additional workers if needed
 */
public class PendingJobsHandler implements Runnable {
    public void run() {

    }
}
