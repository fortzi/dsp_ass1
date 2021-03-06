package dsp.ass1.manager;

import com.amazonaws.services.sqs.model.Message;
import dsp.ass1.utils.InstanceFactory;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.SQSHelper;
import dsp.ass1.utils.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Ofer Caspi on 04/02/2016.
 *
 */
class PendingJobsHandler implements Runnable {
    private S3Helper s3;
    private SQSHelper sqs;
    private Jobs allJobs;
    private ExecutorService executor;

    PendingJobsHandler(Jobs allJobs) {
        this.s3 = new S3Helper();
        this.sqs = new SQSHelper();
        this.allJobs = allJobs;
        this.executor = Executors.newFixedThreadPool(10);
    }

    public void run() {
        try {
            runMethod();
        } catch (Exception e) {
            sqs.debug(e);
        }
    }

    private void runMethod() {
        Message jobMessage;

        System.out.println("Starting pending jobs handler");

        while (!ManagerMain.Auxiliary.terminate.get()) {
            System.out.println("Waiting for jobs");

            while((jobMessage = sqs.getMsgFromQueue(SQSHelper.Queues.PENDING_JOBS)) == null) {
                sqs.sleep("PendingJobsHandler");
            }

            System.out.println("Found new job");

            sqs.extendMessageVisibility(SQSHelper.Queues.PENDING_JOBS, jobMessage);

            String jobObjectKey = jobMessage.getBody();
            Job job;

            try {
                job = new Job(jobMessage.getMessageId());
                allJobs.put(job.getId(), job);
                executor.submit(new JobParser(jobObjectKey, job));
            } catch (Exception e) {
                System.err.println("Error with job: " + jobObjectKey);
                e.printStackTrace();
                handle_panic(jobMessage,e,"JobHandler: Error creating new job tmp file " + jobMessage.getMessageId());
                continue;
            }

            if (jobMessage.getMessageAttributes().containsKey(Settings.TERMINATION_ATTRIBUTE)) {
                System.out.println("Received termination request, all new jobs will be refused...");
                ManagerMain.Auxiliary.terminate.set(true);
            }

            /* getting workers ratio */
            int ratio = Integer.parseInt(jobMessage.getMessageAttributes().get(Settings.RATIO_ATTRIBUTE).getStringValue());

            /* updating ratio from current job */
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
                sqs.sleep("PendingJobHandler Refuse Section");
            }
        }

    }

    /**
     * Removing defected message from queue and send notice back to user
     * @param jobMessage the defected message
     * @param err error message to be sent to user
     */
    private void handle_panic(Message jobMessage, Exception e, String err) {
        /* send error message to debugging queue */
        sqs.debug(e,err);
        /* send notice back to user */
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(jobMessage.getMessageId(), "true");
        attributes.put(Settings.ERROR_ATTRIBUTE, "true");
        sqs.sendMsgToQueue(SQSHelper.Queues.FINISHED_JOBS, err, attributes);
        /* remove defected message from SQS queue */
        sqs.removeMsgFromQueue(SQSHelper.Queues.PENDING_JOBS, jobMessage);
    }


}
