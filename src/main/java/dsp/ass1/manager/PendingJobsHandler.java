package dsp.ass1.manager;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import dsp.ass1.utils.Settings;
import dsp.ass1.utils.InstanceFactory;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.SQSHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Ofer Caspi on 04/02/2016.
 *
 */
public class PendingJobsHandler implements Runnable {
    S3Helper s3;
    SQSHelper sqs;
    ConcurrentHashMap<String, Job> allJobs;
    InstanceFactory workerFactory;

    public PendingJobsHandler(ConcurrentHashMap<String, Job> allJobs) {
        this.s3 = new S3Helper();
        this.sqs = new SQSHelper();
        this.allJobs = allJobs;
        this.workerFactory = new InstanceFactory(Settings.INSTANCE_WORKER);
    }

    public void run() {
        System.out.println("Starting pending jobs handler");

        while (!ManagerMain.Auxiliary.terminate.get()) {
            System.out.println("Waiting for jobs");
            Message jobMessage = sqs.getMsgFromQueue(SQSHelper.Queues.PENDING_JOBS);
            System.out.println("Found new job");

            sqs.extendMessageVisibility(SQSHelper.Queues.PENDING_JOBS, jobMessage);

            String jobObjectKey = jobMessage.getBody();
            Job job;
            S3Object jobObject;

            try {
                System.out.println("Retrieving job contents from S3");
                jobObject = s3.getObject(jobObjectKey);
                String rawJob = S3Helper.getStringFromInputStream(jobObject.getObjectContent());
                job = new Job(rawJob.split("\n"), jobMessage.getMessageId());
                s3.removeObject(jobObject);
            } catch (IOException e) {
                System.err.println("Error with job: " + jobObjectKey);
                e.printStackTrace();
                continue;
            }

            allJobs.put(job.getId(), job);
            System.out.println("Dispatching tweet tasks for job " + job.getId() + " to SQS");
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(Settings.JOB_ID_ATTRIBUTE, job.getId());
            for (String tweetURL : job.getUrls()) {
                sqs.sendMsgToQueue(SQSHelper.Queues.PENDING_TWEETS, tweetURL, attributes);
            }

            if (jobMessage.getMessageAttributes().containsKey(Settings.TERMINATION_ATTRIBUTE)) {
                System.out.println("Terminating...");
                ManagerMain.Auxiliary.terminate.set(true);
            }

            System.out.println("Removing job " + job.getId() + " from SQS queue");
            sqs.removeMsgFromQueue(SQSHelper.Queues.PENDING_JOBS, jobMessage);
        }

        // Actively refuse incoming requests until done
        while (!allJobs.isEmpty()) {
            Message message = sqs.getMsgFromQueue(SQSHelper.Queues.PENDING_JOBS, false);

            if (message != null) {
                String jobId = message.getMessageId();
                sqs.removeMsgFromQueue(SQSHelper.Queues.PENDING_JOBS, message);

                Map<String, String> attributes = new HashMap<String, String>();
                attributes.put(jobId, "true");
                attributes.put(Settings.REFUSE_ATTRIBUTE, "true");
                sqs.sendMsgToQueue(SQSHelper.Queues.FINISHED_JOBS, "Message refused - Manager is terminating.", attributes);
            }

            try {
                // Don't busy wait too intensely
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
