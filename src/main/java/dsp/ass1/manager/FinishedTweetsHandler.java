package dsp.ass1.manager;

import com.amazonaws.services.sqs.model.Message;
import dsp.ass1.utils.Constants;
import dsp.ass1.utils.SQSHelper;

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

    SQSHelper sqs;
    Map<String, Job> allJobs;

    public FinishedTweetsHandler(Map<String, Job> allJobs) {
        this.sqs = new SQSHelper();
        this.allJobs = allJobs;
    }

    public void run() {
        while (true) {
            Message tweetMessage = sqs.getMsgFromQueue(SQSHelper.Queues.FINISHED_TWEETS);
            String jobId = tweetMessage.getAttributes().get(Constants.JOB_ID_ATTRIBUTE);
            String resultObjectKey = tweetMessage.getBody();
            break;
        }
    }
}
