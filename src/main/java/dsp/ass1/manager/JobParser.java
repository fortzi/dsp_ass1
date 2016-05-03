package dsp.ass1.manager;

import com.amazonaws.services.s3.model.S3Object;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.SQSHelper;
import dsp.ass1.utils.Settings;

import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Created by doubled on 0002, 02, 5, 2016.
 */
class JobParser implements Runnable {

    private String jobObjectKey;
    private Job job;
    private S3Helper s3;
    private SQSHelper sqs;

    JobParser(String jobObjectKey, Job job) {
        this.jobObjectKey = jobObjectKey;
        this.job = job;

        s3 = new S3Helper();
        sqs = new SQSHelper();
    }

    public void run() {

        String tweetUrl;
        S3Object jobObject;
        BufferedReader reader;

        try {
            System.out.println("Retrieving job contents from S3");
            jobObject = s3.getObject(jobObjectKey);
            reader = s3.getLineReaderFromObject(jobObject);
            s3.removeObject(jobObject);

            System.out.println("Dispatching tweet tasks for job " + job.getId() + " to SQS");
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(Settings.JOB_ID_ATTRIBUTE, job.getId());

            while((tweetUrl = reader.readLine()) != null) {
                sqs.sendMsgToQueue(SQSHelper.Queues.PENDING_TWEETS, tweetUrl, attributes);
                job.incRemainingUrls();
            }

            job.finishInitializing();

        } catch (Exception e) {
            System.err.println("Error with job: " + jobObjectKey);
            e.printStackTrace();
            job.setAsBroken();
            handle_panic(job,e,"JobHandler: Error with s3 file from job " + job.getId());
        }
    }

    /**
     * Removing defected message from queue and send notice back to user
     * @param job the defected job
     * @param err error message to be sent to user
     */
    private void handle_panic(Job job, Exception e,String err) {
        System.out.println("~@~ error");
        /* send error message to debugging queue */
        sqs.debug(e,err);
        /* send notice back to user */
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(job.getId(), "true");
        attributes.put(Settings.ERROR_ATTRIBUTE, "true");
        sqs.sendMsgToQueue(SQSHelper.Queues.FINISHED_JOBS, err, attributes);
    }
}
