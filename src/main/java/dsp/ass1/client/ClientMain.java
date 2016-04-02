package dsp.ass1.client;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.Filter;
import dsp.ass1.utils.InstanceFactory;

import java.util.Arrays;

/**
 * Created by doubled on 0002, 02, 4, 2016.
 */
public class ClientMain {

    public static void main(String[] args) {

        if(args.length < 2){
            System.out.println("arguments are missing");
            return;
        }

        AWSCredentials credentials = InstanceFactory.getCredentials();

        AmazonEC2Client amazonEC2Client = new AmazonEC2Client(credentials);
        amazonEC2Client.setEndpoint("ec2.us-east-1.amazonaws.com");

        DescribeInstancesRequest describe = new DescribeInstancesRequest();
        describe.setFilters(Arrays.asList(new Filter()));

        amazonEC2Client.

    }
}
