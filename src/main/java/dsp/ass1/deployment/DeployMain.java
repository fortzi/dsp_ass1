package dsp.ass1.deployment;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import dsp.ass1.utils.Settings;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by doubled on 0002, 02, 5, 2016.
 */
public class DeployMain {

    public static void main(String[] args) {

        try {
            System.out.println("start deplyoment");

            purgeQueues();

            s3cleanFolder("statistics");
            s3cleanFolder("pending-jobs");
            s3cleanFolder("finished-jobs");

            //makePackageFile();

            uploadPackageFile();

            System.out.println("done deplyoment");
        }
        catch (Exception e) {
            System.out.println("error: " + e.getMessage());
            e.printStackTrace();
        }

    }

    private static void purgeQueues() throws InterruptedException {
        System.out.println("cleaning the queues");

        AmazonSQSClient sqs = new AmazonSQSClient();
        PurgeQueueRequest purgeReq = new PurgeQueueRequest();

        purgeReq.withQueueUrl(sqs.getQueueUrl("dsp-ass1-pending-jobs").getQueueUrl());
        sqs.purgeQueue(purgeReq);
        Thread.sleep(1000);

        purgeReq.withQueueUrl(sqs.getQueueUrl("dsp-ass1-finished-jobs").getQueueUrl());
        sqs.purgeQueue(purgeReq);
        Thread.sleep(1000);

        purgeReq.withQueueUrl(sqs.getQueueUrl("dsp-ass1-pending-tweets").getQueueUrl());
        sqs.purgeQueue(purgeReq);
        Thread.sleep(1000);

        purgeReq.withQueueUrl(sqs.getQueueUrl("dsp-ass1-finished-tweets").getQueueUrl());
        sqs.purgeQueue(purgeReq);
        Thread.sleep(1000);

        purgeReq.withQueueUrl(sqs.getQueueUrl("dsp-ass1-debugging").getQueueUrl());
        sqs.purgeQueue(purgeReq);
    }

    private static void s3cleanFolder(String folder) {
        System.out.println("cleaning s3 folder " + folder);

        AmazonS3 s3 = new AmazonS3Client();
        DeleteObjectsRequest delReq = new DeleteObjectsRequest(Settings.BUCKET_NAME);
        ObjectListing listing;
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(Settings.BUCKET_NAME)
                .withPrefix(folder);

        ArrayList<DeleteObjectsRequest.KeyVersion> list = new ArrayList<DeleteObjectsRequest.KeyVersion>();

        do {
            listing = s3.listObjects(listObjectsRequest);
            for (S3ObjectSummary objectSummary : listing.getObjectSummaries()) {
                if(!objectSummary.getKey().equals(folder + "/"))
                    list.add(new DeleteObjectsRequest.KeyVersion(objectSummary.getKey()));
            }
            listObjectsRequest.setMarker(listing.getNextMarker());
        } while (listing.isTruncated());

        if(list.isEmpty())
            return;

        delReq.setKeys(list);
        s3.deleteObjects(delReq);
    }

    private static void makePackageFile() throws IOException, InterruptedException {
        System.out.println("recreacting package file");

        File f = new File("classes\\artifacts\\package.zip");

        if(f.exists())
            f.delete();

        String[] command = {"cmd.exe", "/C", "Start", "classes\\artifacts\\makeauto.bat"};
        Process p =  Runtime.getRuntime().exec(command);
        p.waitFor();
    }

    private static void uploadPackageFile() throws InterruptedException {
        System.out.println("uploading package file to s3");

        AmazonS3 s3 = new AmazonS3Client();
        String objectKey = "package.zip";
        File file;

        while(!(file = new File("classes\\artifacts\\package.zip")).exists());

        Thread.sleep(500);

        PutObjectRequest putReq = new PutObjectRequest(Settings.BUCKET_NAME, objectKey, file);
        putReq.withCannedAcl(CannedAccessControlList.PublicRead);
        s3.putObject(putReq);
    }
}