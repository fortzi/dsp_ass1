package dsp.ass1.deployment;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.Settings;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by doubled on 0002, 02, 5, 2016.
 */
public class DeployMain {

    private static boolean clean = true;
    private static boolean repackage = false;
    private static boolean statistics = false;


    public static void main(String[] args) {

        try {
            System.out.println("start deplyoment");

            if(repackage) makePackageFile();

            if(statistics) getStatistics();

            if(clean) purgeQueues();

            if(clean) s3cleanFolder("statistics");
            if(clean) s3cleanFolder("pending-jobs");
            if(clean) s3cleanFolder("finished-jobs");

            if(repackage) uploadPackageFile();

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
            //noinspection ResultOfMethodCallIgnored
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

    private static void getStatistics() throws IOException {
        AmazonS3 s3 = new AmazonS3Client();
        PrintWriter writer = new PrintWriter("statistics.txt");
        ObjectListing listing;
        InputStream stream;
        String line;
        BufferedReader reader;
        int i=0;
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(Settings.BUCKET_NAME)
                .withPrefix("statistics/");

        System.out.println("extracting the statistcs");

        do {
            listing = s3.listObjects(listObjectsRequest);
            for (S3ObjectSummary objectSummary : listing.getObjectSummaries()) {
                System.out.println(objectSummary.getKey());
                stream = new S3Helper().getObject(objectSummary.getKey()).getObjectContent();
                reader = new BufferedReader(new InputStreamReader(stream));
                writer.write("worker " + i++ + "\n");
                while((line = reader.readLine()) != null)
                    writer.write(line + "\n");
            }
            listObjectsRequest.setMarker(listing.getNextMarker());
        } while (listing.isTruncated());
        System.out.println("\n");
        writer.close();
    }
}