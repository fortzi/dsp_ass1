package dsp.ass1.client;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import dsp.ass1.utils.Constants;
import dsp.ass1.utils.InstanceFactory;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.SQSHelper;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by doubled on 0002, 02, 4, 2016.
 */
public class ClientMain {

    public static void main(String[] args) {

        if(args.length < 1){
            System.out.println("arguments are missing");
            return;
        }

        // checking whether user is asking to terminate manager when done
        if(args.length > 1 && "terminate".equals(args[1]));
            sendTeminateSignal();

        if(!isManagerAlive()) {
            System.out.println("Manager was not found !");
            if(!createManager())
                return;
        }
        else
            System.out.println("Manager was found !");

        System.out.println("uploading " + args[0] + " to S3");
        String fileKey = sendInputFile(args[0]);
        System.out.println("file was uploaded successfully with key: " + fileKey);

        System.out.println("sending newJobSignal to SQS");
        String myId = sendNewJobSignal(fileKey);
        System.out.println("message sent successfully");

        System.out.println("my id: " + myId);







    }

    /**
     *
     * @param fileKey the file id of the file that
     * @return
     */
    private static String sendNewJobSignal(String fileKey) {
        SQSHelper sqs = new SQSHelper();
        return sqs.sendMsgToQueue(SQSHelper.Queues.PENDING_JOBS,fileKey);
    }

    private static String sendInputFile(String fileName) {
        S3Helper s3 = new S3Helper();
        return s3.putObject(S3Helper.Folders.PENDING_JOBS, new File(fileName));
    }

    private static void sendTeminateSignal() {

    }

    private static boolean createManager() {
        try {
            System.out.println("Creating manager !");
            new ManagerFactory().makeInstance();
            System.out.println("Manager Created !");
        }
        catch (IOException e) {
            System.out.println("cannot create Manager instance");
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }

    private static boolean isManagerAlive() {

        //creating ec2 object instance
        AmazonEC2Client amazonEC2Client = new AmazonEC2Client();
        amazonEC2Client.setRegion(Constants.REGION);

        // defining filters:
        // first filter for tag "Type = manager"
        Filter filterType = new Filter("tag:Type");
        filterType.withValues("manager");
        // second filter for status != terminated
        Filter filterState = new Filter("instance-state-name");
        filterState.withValues("pending","running");

        // creating request for list of instances
        DescribeInstancesRequest describeReq = new DescribeInstancesRequest();
        describeReq.withFilters(filterType, filterState);

        // executing the request
        DescribeInstancesResult describeRes = amazonEC2Client.describeInstances(describeReq);

        return describeRes.getReservations().size() != 0;
    }
}
