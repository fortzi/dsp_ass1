package dsp.ass1.manager;

import com.amazonaws.services.sqs.model.Message;
import dsp.ass1.utils.Constants;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.SQSHelper;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Ofer Caspi on 04/02/2016.
 *
 */
public class FinishedTweetsHandler implements Runnable {
    /*
    Get tweet-result from SQS
    For each result:
        Append to answer the specific result file
        If all answers for specific job have arrived;
            Upload results to S3
            Send results URL to SQS
            Finalize local job state
     */

    S3Helper s3;
    SQSHelper sqs;
    Map<String, Job> allJobs;

    public FinishedTweetsHandler(Map<String, Job> allJobs) {
        this.s3 = new S3Helper();
        this.sqs = new SQSHelper();
        this.allJobs = allJobs;
    }

    public void run() {
        System.out.println("Starting finished tweets handler");
        while (true) {
            System.out.println("Waiting for tweets results");
            Message tweetMessage = sqs.getMsgFromQueue(SQSHelper.Queues.FINISHED_TWEETS);

            System.out.println("Found a tweet results");
            String tweetResult = tweetMessage.getBody();
            Job job = allJobs.get(tweetMessage.getAttributes().get(Constants.JOB_ID_ATTRIBUTE));
            job.addResult(tweetResult);

            if (!job.isComplete()) {
                continue;
            }

            File results = job.getResultsFile();
            System.out.println("Uploading job results to S3");
            String finishedJobObjectKey = s3.putObject(S3Helper.Folders.FINISHED_JOBS, results);

            System.out.println("Sending job results message to SQS");
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(Constants.JOB_ID_ATTRIBUTE, job.getId());
            sqs.sendMsgToQueue(SQSHelper.Queues.FINISHED_JOBS, finishedJobObjectKey, attributes);

            job.close();
            allJobs.remove(job.getId());

            if (allJobs.isEmpty() && ManagerMain.Auxiliary.terminate) {
                System.out.println("Sending termination messages to " + ManagerMain.Auxiliary.workerCount + " workers");
                attributes = new HashMap<String, String>();
                attributes.put(Constants.TERMINATION_MESSAGE, "true");

                while (ManagerMain.Auxiliary.workerCount-- > 0) {
                    sqs.sendMsgToQueue(SQSHelper.Queues.PENDING_TWEETS, "Termination message", attributes);
                }

                break;
            }
        }
    }
}
