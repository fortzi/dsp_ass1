package dsp.ass1.manager;

import java.util.Map;

/**
 * Created by user1 on 04/02/2016.
 */
public class FinishedTweetsHandler implements Runnable {
    /*
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

    public FinishedTweetsHandler(Map<String, Job> allJobs) {

    }

    public void run() {

    }
}
