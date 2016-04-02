package dsp.ass1.client;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import dsp.ass1.utils.InstanceFactory;

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

        AWSCredentials credentials = InstanceFactory.getCredentials();

        // checking whether user is asking to terminate manager when done
        if(args.length > 1 && "terminate".equals(args[1]));
            sendTeminateSignal(credentials);





    }

    private static void sendTeminateSignal(AWSCredentials credentials) {

    }

    private static boolean createManager(AWSCredentials credentials) {
        try {
            new ManagerFactory(credentials).makeInstance();
        }
        catch (IOException e) {
            System.out.println("cannot create Manager instance");
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }

    private static boolean isManagerAlive(AWSCredentials credentials) {

        //creating ec2 object instance
        Region instanceRegion = Region.getRegion(Regions.US_EAST_1);
        AmazonEC2Client amazonEC2Client = new AmazonEC2Client(credentials);
        amazonEC2Client.setRegion(instanceRegion);

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
