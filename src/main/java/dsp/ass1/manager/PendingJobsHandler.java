package dsp.ass1.manager;

import com.amazonaws.services.sqs.model.Message;
import dsp.ass1.utils.Constants;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.SQSHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Ofer Caspi on 04/02/2016.
 *
 */
public class PendingJobsHandler implements Runnable {
    S3Helper s3;
    SQSHelper sqs;
    Map<String, Job> allJobs;
    TwitterWorkerFactory workerFactory;

    public PendingJobsHandler(Map<String, Job> allJobs) {
        this.s3 = new S3Helper();
        this.sqs = new SQSHelper();
        this.allJobs = allJobs;
        this.workerFactory = new TwitterWorkerFactory();
    }

    public void run() {
        System.out.println("Starting pending jobs handler");
        ManagerMain.Auxiliary.workerCount = 0;

        while (true) {
            System.out.println("Waiting for jobs");
            Message jobMessage = sqs.getMsgFromQueue(SQSHelper.Queues.PENDING_JOBS);
            System.out.println("Found new job");

            if (jobMessage.getMessageAttributes().containsKey(Constants.TERMINATION_MESSAGE)) {
                System.out.println("Terminating");
                ManagerMain.Auxiliary.terminate = true;
                break;
            }

            String jobObjectKey = jobMessage.getBody();
            Job job;

            try {
                System.out.println("Retrieving job contents from S3");
                String rawJob = S3Helper.getStringFromInputStream(s3.getObject(jobObjectKey).getObjectContent());
                job = new Job(rawJob.split("\n"), jobMessage.getMessageId());
            } catch (IOException e) {
                System.err.println("Error with job: " + jobObjectKey);
                e.printStackTrace();
                continue;
            }

            allJobs.put(job.getId(), job);
            System.out.println("Dispatching tweet tasks to SQS");
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(Constants.JOB_ID_ATTRIBUTE, job.getId());
            for (String tweetURL : job.getUrls()) {
                sqs.sendMsgToQueue(SQSHelper.Queues.PENDING_TWEETS, tweetURL, attributes);
            }

            int messageCount = sqs.getMsgCount(SQSHelper.Queues.PENDING_TWEETS);
            int newWorkersCount = messageCount / Constants.TWEETS_PER_WORKER - ManagerMain.Auxiliary.workerCount;

            if (newWorkersCount > 0) {
                System.out.println("Creating " + newWorkersCount + " new workers");
                ManagerMain.Auxiliary.workerCount += workerFactory.makeInstances(newWorkersCount);
            }

            System.out.println("Removing job from SQS queue");
            sqs.removeMsgFromQueue(SQSHelper.Queues.PENDING_JOBS, jobMessage);
        }
    }
}
