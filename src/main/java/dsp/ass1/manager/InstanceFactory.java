package dsp.ass1.manager;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.ShutdownBehavior;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by Ofer on 03/30/2016.
 */
public class InstanceFactory {
    final static String GENERIC_IMAGE_AMI_ID   = "ami-c03b30aa";
    final static String INSTANCE_TYPE          = "t2.micro";
    final static String KEY_NAME               = "bansko";
    final static String SECURITY_GROUP         = "launch-wizard-1";
    final static String PACKAGE_FILE_NAME      = "package.zip";
    final static String S3_ADDRESS             = "https://s3.amazonaws.com/dsp-ass1/" + PACKAGE_FILE_NAME;

    protected void makeInstance(String job) throws IOException {
        AWSCredentials credentials = null;

        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                "Cannot load the credentials from the credential profiles file. " +
                        "Please make sure that your credentials file is at the correct " +
                        "location (~/.aws/credentials), and is in valid format.",
                e);
        }

        AmazonEC2Client amazonEC2Client = new AmazonEC2Client(credentials);
        amazonEC2Client.setEndpoint("ec2.us-east-1.amazonaws.com");

        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
        runInstancesRequest.setInstanceInitiatedShutdownBehavior(ShutdownBehavior.Terminate);
        runInstancesRequest
                .withImageId(GENERIC_IMAGE_AMI_ID)
                .withInstanceType(INSTANCE_TYPE)
                .withMinCount(1)
                .withMaxCount(1)
                .withKeyName(KEY_NAME)
                .withSecurityGroups(SECURITY_GROUP)
                .setUserData(getUserData(job));

        RunInstancesResult runInstancesResult = amazonEC2Client.runInstances(runInstancesRequest);
    }

    private static String getUserData(String job) {
        String base64UserData = null;
        StringBuilder userData = new StringBuilder();

        userData.append("#! /bin/bash").append("\n");
        userData.append("mv /home/ec2-user/ass1 ~/ass1").append("\n");
        userData.append("cd ~/ass1").append("\n");
        userData.append("wget ").append(S3_ADDRESS).append("\n");
        userData.append("unzip -P `cat zipcred.txt` ").append(PACKAGE_FILE_NAME).append("\n");
        userData.append("mkdir ~/.aws").append("\n");
        userData.append("mv ~/ass1/credentials ~/.aws/").append("\n");
        userData.append("java -jar ~/ass1/jars/").append(job).append(".jar > log.txt").append("\n");
        userData.append("curl -X POST -d \"`cat log.txt`\" http://requestb.in/18jmtli1").append("\n");
        userData.append("sudo shutdown -h now");

        try {
            base64UserData = new String(Base64.encodeBase64(userData.toString().getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return base64UserData;
    }
}
