package dsp.ass1.manager;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import dsp.ass1.utils.Settings;
import dsp.ass1.utils.InstanceFactory;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.SQSHelper;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Ofer Caspi on 04/02/2016.
 *
 */
public class PendingJobsHandler implements Runnable {
    S3Helper s3;
    SQSHelper sqs;
    Jobs allJobs;
    InstanceFactory workerFactory;
    ExecutorService executor;

    public PendingJobsHandler(Jobs allJobs) {
        this.s3 = new S3Helper();
        this.sqs = new SQSHelper();
        this.allJobs = allJobs;
        this.workerFactory = new InstanceFactory(Settings.INSTANCE_WORKER);
        this.executor = Executors.newFixedThreadPool(10);
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
                job = new Job(jobMessage.getMessageId());
                allJobs.put(job.getId(), job);
                executor.submit(new JobParser(jobObjectKey, job));
            } catch (Exception e) {
                System.err.println("Error with job: " + jobObjectKey);
                e.printStackTrace();
                handle_panic(jobMessage, "JobHandler: Error creating new job tmp file " + jobMessage.getMessageId());
                continue;
            }

            if (jobMessage.getMessageAttributes().containsKey(Settings.TERMINATION_ATTRIBUTE)) {
                System.out.println("Received termination request, all new jobs will be refused...");
                ManagerMain.Auxiliary.terminate.set(true);
            }

            /* getting workers ratio */
            if(!jobMessage.getMessageAttributes().containsKey(Settings.RATIO_ATTRIBUTE)) {
                System.err.println("Error with job: " + job.getId() + " can't find ratio");
                handle_panic(jobMessage, "JobHandler: new job arrived without ratio settings");
                continue;
            }

            /* updating ratio from current job */
            int ratio = Integer.parseInt(jobMessage.getMessageAttributes().get(Settings.RATIO_ATTRIBUTE).getStringValue());
            if((ManagerMain.Auxiliary.ratio.get() > ratio) || (ManagerMain.Auxiliary.ratio.get() == 0))
                ManagerMain.Auxiliary.ratio.set(ratio);

            System.out.println("Removing job " + job.getId() + " from pending jobs SQS queue");
            sqs.removeMsgFromQueue(SQSHelper.Queues.PENDING_JOBS, jobMessage);
        }

        //TODO should we wait for all jobParser to finish ? maybe bot beacsue then alljobs.isempty always be negative
        executor.shutdown();

        // Actively refuse incoming requests until done
        while (!allJobs.isEmpty()) {
            Message message = sqs.getMsgFromQueue(SQSHelper.Queues.PENDING_JOBS);

            if (message != null) {
                System.out.println("Refusing job " + message.getMessageId());
                String jobId = message.getMessageId();
                String jobObjectKey = message.getBody();
                sqs.removeMsgFromQueue(SQSHelper.Queues.PENDING_JOBS, message);

                /* clearing s3 from refused job's file ! */
                s3.removeObject(jobObjectKey);

                Map<String, String> attributes = new HashMap<String, String>();
                attributes.put(jobId, "true");
                attributes.put(Settings.REFUSE_ATTRIBUTE, "true");
                sqs.sendMsgToQueue(SQSHelper.Queues.FINISHED_JOBS, "Message refused - Manager is terminating.", attributes);
            } else {
                try {
                    Thread.sleep(Settings.SLEEP_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * Removing defected message from queue and send notice back to user
     * @param jobMessage the defected message
     * @param err error message to be sent to user
     */
    private void handle_panic(Message jobMessage, String err) {
        System.out.println("~@~ error");
        /* send error message to debugging queue */
        sqs.sendMsgToQueue(SQSHelper.Queues.DEBUGGING, err);
        /* send notice back to user */
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(jobMessage.getMessageId(), "true");
        attributes.put(Settings.ERROR_ATTRIBUTE, "true");
        sqs.sendMsgToQueue(SQSHelper.Queues.FINISHED_JOBS, err, attributes);
        /* remove defected message from SQS queue */
        sqs.removeMsgFromQueue(SQSHelper.Queues.PENDING_JOBS, jobMessage);
    }


}
