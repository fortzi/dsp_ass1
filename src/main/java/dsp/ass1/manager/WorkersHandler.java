package dsp.ass1.manager;

import dsp.ass1.utils.Settings;
import dsp.ass1.utils.EC2Helper;
import dsp.ass1.utils.InstanceFactory;
import dsp.ass1.utils.SQSHelper;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Ofer Caspi on 04/07/2016.
 *
 */
public class WorkersHandler implements Runnable {
    private EC2Helper ec2;
    private SQSHelper sqs;
    private InstanceFactory workerFactory;
    private ConcurrentHashMap<String, Job> allJobs;

    public WorkersHandler(ConcurrentHashMap<String, Job> allJobs) {
        ec2 = new EC2Helper();
        sqs = new SQSHelper();
        workerFactory = new InstanceFactory(Settings.INSTANCE_WORKER);
        this.allJobs = allJobs;
    }

    public void run() {
        int neededWorkers;
        int createdWorkers;

        System.out.println("workers handler started");

        while (!ManagerMain.Auxiliary.terminate.get() || !allJobs.isEmpty()) {

            try {
                neededWorkers =
                        (sqs.getMsgCount(SQSHelper.Queues.PENDING_TWEETS) / ManagerMain.Auxiliary.ratio.get())
                                - ec2.countInstancesOfType(Settings.INSTANCE_WORKER);
            }
            catch (Exception e) {
                System.out.println("error in workers handler");
                e.printStackTrace();
                continue;
            }


            System.out.println("needed workers " + neededWorkers);

            if (neededWorkers > 0) {
                System.out.println("Trying to create " + neededWorkers + " new workers");
                createdWorkers = workerFactory.makeInstances(neededWorkers);
                System.out.println("Created " + createdWorkers + " new workers");
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("workers handler finished");

        //TODO keep this or delete ? since workers are killed anyway when manager died.
        /*
        createdWorkers = ec2.countInstancesOfType(Settings.INSTANCE_WORKER);
        System.out.println("Sending termination messages to " + createdWorkers + " workers");
        while (createdWorkers-- > 0) {
            sqs.sendMsgToQueue(SQSHelper.Queues.PENDING_TWEETS, "Termination message", Settings.TERMINATION_ATTRIBUTE);
        }*/
    }
}
