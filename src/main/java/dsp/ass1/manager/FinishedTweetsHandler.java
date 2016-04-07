package dsp.ass1.manager;

import com.amazonaws.services.sqs.model.Message;
import dsp.ass1.utils.Settings;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.SQSHelper;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

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
    ConcurrentHashMap<String, Job> allJobs;

    public FinishedTweetsHandler(ConcurrentHashMap<String, Job> allJobs) {
        this.s3 = new S3Helper();
        this.sqs = new SQSHelper();
        this.allJobs = allJobs;
    }

    public void run() {
        System.out.println("Starting finished tweets handler");
        boolean isJobComplete = false;

        while (true) {
            System.out.println("Waiting for tweets results");
            Message tweetMessage = sqs.getMsgFromQueue(SQSHelper.Queues.FINISHED_TWEETS);
            String tweetResult = tweetMessage.getBody();
            String jobId = tweetMessage.getMessageAttributes().get(Settings.JOB_ID_ATTRIBUTE).getStringValue();
            Job job = allJobs.get(jobId);
            System.out.println("Found a tweet results for job " + job.getId());

            try {
                isJobComplete = job.addResult(tweetResult);
                sqs.removeMsgFromQueue(SQSHelper.Queues.FINISHED_TWEETS, tweetMessage);
            } catch (NullPointerException e) {
                System.err.println("Error with job " + jobId);
                return;
            }

            if (!isJobComplete) {
                continue;
            }

            job.close();
            File results = job.getResultsFile();
            System.out.println("Uploading job " + job.getId() + " results to S3");
            String finishedJobObjectKey = s3.putObject(S3Helper.Folders.FINISHED_JOBS, results);

            System.out.println("Sending job results message to SQS");
            sqs.sendMsgToQueue(SQSHelper.Queues.FINISHED_JOBS, finishedJobObjectKey, job.getId());

            allJobs.remove(job.getId());

            if (allJobs.isEmpty() && ManagerMain.Auxiliary.terminate.get()) {
                break;
            }
        }
    }
}
