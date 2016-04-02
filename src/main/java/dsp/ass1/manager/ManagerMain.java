package dsp.ass1.manager;

/**
 * Created by Ofer Caspi on 03/28/2016.
 */

import java.io.IOException;
import java.util.Dictionary;

public class ManagerMain {
    public static void main(String[] args) throws IOException {
        /*

        Threads:
            Listen to dsp-ass1-pending-jobs:
                Get jobs links from SQS
                For each job:
                    Download job from S3
                    Add entry to dictionary
                    Make file to append results to
                    For each tweet:
                        Send (job_id, link) to SQS
                    Check SQS message count
                    Add additional workers if needed
            Listen to dsp-ass1-finished-tweets:
                Get tweet-result link from SQS
                For each result:
                    Download result from S3
                    Append to answer the specific result file
                    Update dictionary entry with current count
                    If all answers for specific job have arrived;
                        Upload response to S3
                        Send results URL to SQS
                        Finalize local job state
        */

        final TwitterWorkerFactory twitterWorkerFactory = new TwitterWorkerFactory();

        (new Thread(new PendingJobsHandler())).start();
        (new Thread(new FinishedTweetsHandler())).start();

        new Thread() {
            public void run() {
                try {
                    System.out.println("Creating worker instance");
                    twitterWorkerFactory.makeInstance();
                    System.out.println("Created worker instance");
                } catch (IOException e) {
                    System.out.println("TwitterWorkerFactory Error: " + e);
                }
            }
        }.start();
    }
}
