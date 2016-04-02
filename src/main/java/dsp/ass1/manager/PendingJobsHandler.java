package dsp.ass1.manager;

import com.amazonaws.services.sqs.model.Message;
import dsp.ass1.utils.Constants;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.SQSHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
    S3Helper s3;
    SQSHelper sqs;
    Map<String, Job> allJobs;
    TwitterWorkerFactory workerFactory;
    int workerCount = 0;

    public PendingJobsHandler(Map<String, Job> allJobs) {
        this.s3 = new S3Helper();
        this.sqs = new SQSHelper();
        this.allJobs = allJobs;
        this.workerFactory = new TwitterWorkerFactory();
    }

    public void run() {
        while (true) {
            Message jobMessage = sqs.getMsgFromQueue(SQSHelper.Queues.PENDING_JOBS);
            if (jobMessage.getAttributes().containsKey(Constants.TERMINATION_MESSAGE)) {
                break;
            }

            String jobObjectKey = jobMessage.getBody().toString();
            String rawJob = s3.getObject(jobObjectKey);
            Job job = null;

            try {
                job = new Job(rawJob.split("\n"));
            } catch (IOException e) {
                System.err.println("Error parsing job at: " + jobObjectKey);
                continue;
            }

            allJobs.put(jobObjectKey, job);
            // Send SQS Messages
            for (String tweetURL : job.getUrls()) {
                Map<String, String> attributes = new HashMap<String, String>();
                attributes.put("job_id", jobObjectKey);
                sqs.sendMsgToQueue(SQSHelper.Queues.PENDING_TWEETS, tweetURL, attributes);
            }

            int messageCount = sqs.getMsgCount(SQSHelper.Queues.PENDING_TWEETS);
            int newWorkersCount = messageCount / workerCount - Constants.TWEETS_PER_WORKER;
            if (newWorkersCount > 0) {
                workerFactory.makeInstances(newWorkersCount);
            }

            sqs.removeMsgFromQueue(SQSHelper.Queues.PENDING_JOBS, jobMessage);
        }
    }
}
