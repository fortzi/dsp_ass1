package dsp.ass1.utils;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Reservation;

/**
 * Created by Ofer Caspi on 04/07/2016.
 *
 */
public class EC2Helper {
    private AmazonEC2Client amazonEC2Client;

    public EC2Helper() {
        //creating ec2 object instance
        amazonEC2Client = new AmazonEC2Client();
        amazonEC2Client.setRegion(Settings.REGION);
    }

    public int countInstancesOfType(String type) {
        // defining filters:
        // first filter for tag "Type = manager"
        Filter filterType = new Filter("tag:Type");
        filterType.withValues(type);
        // second filter for status != terminated
        Filter filterState = new Filter("instance-state-name");
        filterState.withValues("pending", "running");

        // creating request for list of instances
        DescribeInstancesRequest describeReq = new DescribeInstancesRequest();
        describeReq.withFilters(filterType, filterState);

        // executing the request
        DescribeInstancesResult describeRes = amazonEC2Client.describeInstances(describeReq);

        int instanceCount = 0;

        for (Reservation reservation : describeRes.getReservations()) {
            instanceCount += reservation.getInstances().size();
        }

        return instanceCount;
    }

    /**
     * find out whether there is an instance with tag "Type" as "manager
     * @return true if the manager exists
     */
    public boolean isManagerAlive() {
        return countInstancesOfType(Settings.INSTANCE_MANAGER) != 0;
    }

    public void sleep(String caller) {
        try {
            Thread.sleep(Settings.SLEEP_INTERVAL * 2);
        } catch (InterruptedException e) {
            e.printStackTrace();
            new SQSHelper().debug(e, "sleep interapted in " + caller);
        }
    }
}
