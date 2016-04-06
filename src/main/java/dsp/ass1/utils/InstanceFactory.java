package dsp.ass1.utils;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import org.apache.commons.codec.binary.Base64;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by Ofer Caspi on 03/30/2016.
 *
 */
public class InstanceFactory {
    final String GENERIC_IMAGE_AMI_ID   = "ami-80190eea";
    final String INSTANCE_TYPE          = "t2.micro";
    final String KEY_NAME               = "bansko";
    final String SECURITY_GROUP         = "launch-wizard-1";
    final String PACKAGE_FILE_NAME      = "package.zip";
    final String S3_ADDRESS             = "https://s3.amazonaws.com/dsp-ass1/" + PACKAGE_FILE_NAME;

    static int runningInstances = 0;
    String jarFileName = null;

    public InstanceFactory(String jarFileName) {
        this.jarFileName = jarFileName;
    }

    /**
     * Prepping EC2 instance and running specified jarFile.
     */
    public int makeInstance() {
        return makeInstances(1);
    }

    /**
     * Prepping EC2 instance and running specified jarFile.
     */
    public synchronized int makeInstances(int count) {
        int instancesCreated = 0;

        while (count > Constants.INSTANCES_PER_QUANTA) {
            count -= Constants.INSTANCES_PER_QUANTA;
            instancesCreated += createInstances(Constants.INSTANCES_PER_QUANTA);
        }

        instancesCreated += createInstances(count);
        return instancesCreated;
    }

    /**
     * Preping EC2 instance and running specified jarFile.
     * @param count
     *          How many instances to create.
     *          It is not advised to create too many at once.
     */
    private int createInstances(int count) {
        int newInstancesCount = Math.min(count, Constants.INSTANCE_LIMIT - runningInstances);
        if (newInstancesCount <= 0) {
            return 0;
        }

        AmazonEC2Client amazonEC2Client = new AmazonEC2Client();
        amazonEC2Client.setRegion(Constants.REGION);

        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
        runInstancesRequest.setInstanceInitiatedShutdownBehavior(ShutdownBehavior.Terminate);
        runInstancesRequest
                .withImageId(GENERIC_IMAGE_AMI_ID)
                .withInstanceType(INSTANCE_TYPE)
                .withMinCount(Math.min(1, newInstancesCount))
                .withMaxCount(newInstancesCount)
                .withKeyName(KEY_NAME)
                .withSecurityGroups(SECURITY_GROUP)
                .setUserData(getUserData(jarFileName));


        RunInstancesResult runInstancesResult;

        try {
            runInstancesResult = amazonEC2Client.runInstances(runInstancesRequest);
        }
        catch (Exception e) {
            return 0;
        }

        List<Instance> instances = runInstancesResult.getReservation().getInstances();
        tagInstances(amazonEC2Client, instances, jarFileName);

        // counting how instances were really created
        runningInstances += instances.size();
        return instances.size();
    }

    private void tagInstances(AmazonEC2Client client, List<Instance> instances, String tag) {
        for (Instance instance : instances) {
            CreateTagsRequest createTagsRequest = new CreateTagsRequest();
            createTagsRequest.withResources(instance.getInstanceId())
                    .withTags(new Tag("Type", tag));

            // sometimes instances creating too slow and while this code is running
            // the instance is not listed yet
            int tries = 10;
            while (tries-- > 0) {
                try {
                    client.createTags(createTagsRequest);
                    break;
                }
                catch (Exception e) {
                    System.err.println("can't tag instance " + instance.getInstanceId() + ", trying " + tries + " more times.");
                    e.printStackTrace();

                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        }
    }

    // returning user data in BASE64 format
    private String getUserData(String jarFile) {
        String base64UserData = null;
        StringBuilder userData = new StringBuilder();

        userData.append("#! /bin/bash").append("\n");
        userData.append("curl -X POST -d \"`echo running user-data script`\" http://requestb.in/1iom4uw1").append("\n");
        userData.append("cd ~/ass1").append("\n");
        userData.append("wget ").append(S3_ADDRESS).append("\n");
        userData.append("unzip -P `cat zipcred.txt` ").append(PACKAGE_FILE_NAME).append("\n");
        userData.append("mkdir ~/.aws").append("\n");
        userData.append("mv ~/ass1/credentials ~/.aws/").append("\n");
        userData.append("curl -X POST -d \"starting " + jarFile + "\" http://requestb.in/1iom4uw1").append("\n");
        userData.append("java -Xmx700m -jar ~/ass1/").append(jarFile).append(".jar > log.txt 2>&1").append("\n");
        userData.append("curl -X POST -d \"log for " + jarFile + "<br> `cat log.txt`\" http://requestb.in/1iom4uw1").append("\n");
        userData.append("sudo shutdown -h now");

        try {
            base64UserData = new String(Base64.encodeBase64(userData.toString().getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return base64UserData;
    }
}
