package dsp.ass1.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by Ofer on 03/30/2016.
 */
public class InstanceFactory {
    final String GENERIC_IMAGE_AMI_ID   = "ami-9f363df5";
    final String INSTANCE_TYPE          = "t2.micro";
    final String KEY_NAME               = "bansko";
    final String SECURITY_GROUP         = "launch-wizard-1";
    final String PACKAGE_FILE_NAME      = "package.zip";
    final String S3_ADDRESS             = "https://s3.amazonaws.com/dsp-ass1/" + PACKAGE_FILE_NAME;

    AWSCredentials credentials = null;
    String jarFileName = null;

    protected InstanceFactory(String jarFileName, AWSCredentials credentials) {
        this.jarFileName = jarFileName;
        this.credentials = credentials;
    }

    //preping EC2 instance and running specified jarFile
    public void makeInstance() throws IOException {
        Region instanceRegion = Region.getRegion(Regions.US_EAST_1);
        AmazonEC2Client amazonEC2Client = new AmazonEC2Client(credentials);
        amazonEC2Client.setRegion(instanceRegion);

        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
        runInstancesRequest.setInstanceInitiatedShutdownBehavior(ShutdownBehavior.Terminate);
        runInstancesRequest
                .withImageId(GENERIC_IMAGE_AMI_ID)
                .withInstanceType(INSTANCE_TYPE)
                .withMinCount(1)
                .withMaxCount(1)
                .withKeyName(KEY_NAME)
                .withSecurityGroups(SECURITY_GROUP)
                .setUserData(getUserData(jarFileName));

        RunInstancesResult runInstancesResult = amazonEC2Client.runInstances(runInstancesRequest);


        // Adding Tags
        List<Instance> instances = runInstancesResult.getReservation().getInstances();

        for (Instance instance : instances) {
            CreateTagsRequest createTagsRequest = new CreateTagsRequest();
            createTagsRequest.withResources(instance.getInstanceId())
                    .withTags(new Tag("Type", jarFileName));
            amazonEC2Client.createTags(createTagsRequest);
        }
    }

    // returning user data in BASE64 format
    private String getUserData(String jarFile) {
        String base64UserData = null;
        StringBuilder userData = new StringBuilder();

        userData.append("#! /bin/bash").append("\n");
        userData.append("mv /home/ec2-user/ass1 ~/ass1").append("\n");
        userData.append("cd ~/ass1").append("\n");
        userData.append("wget ").append(S3_ADDRESS).append("\n");
        userData.append("unzip -P `cat zipcred.txt` ").append(PACKAGE_FILE_NAME).append("\n");
        userData.append("mkdir ~/.aws").append("\n");
        userData.append("mv ~/ass1/credentials ~/.aws/").append("\n");
        userData.append("java -jar ~/ass1/jars/").append(jarFile).append(".jar > log.txt").append("\n");
        userData.append("curl -X POST -d \"`cat log.txt`\" http://requestb.in/18jmtli1").append("\n");
        //userData.append("sudo shutdown -h now");

        try {
            base64UserData = new String(Base64.encodeBase64(userData.toString().getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return base64UserData;
    }

    public static AWSCredentials getCredentials() {
        AWSCredentials credentials = null;

        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException (
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        return credentials;
    }
}
