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
class WorkersHandler implements Runnable {
    private EC2Helper ec2;
    private SQSHelper sqs;
    private InstanceFactory workerFactory;
    private ConcurrentHashMap<String, Job> allJobs;

    WorkersHandler(ConcurrentHashMap<String, Job> allJobs) {
        ec2 = new EC2Helper();
        sqs = new SQSHelper();
        workerFactory = new InstanceFactory(Settings.INSTANCE_WORKER);
        this.allJobs = allJobs;
    }

    public void run () {
        try {
            runMethod();
        } catch (Exception e) {
            sqs.debug(e);
        }
    }

    private void runMethod() {
        double neededWorkers;
        int createdWorkers;
        System.out.println("workers handler started");

        while (!ManagerMain.Auxiliary.terminate.get() || !allJobs.isEmpty()) {

            try {
                neededWorkers  = sqs.getMsgCount(SQSHelper.Queues.PENDING_TWEETS);
                neededWorkers /= ManagerMain.Auxiliary.ratio.get();
                neededWorkers -= ec2.countInstancesOfType(Settings.INSTANCE_WORKER);

                neededWorkers = Math.ceil(neededWorkers);

            } catch (Exception e) {
                System.out.println("error in workers handler");
                e.printStackTrace();
                sqs.debug(e, "needed worker calculation");
                continue;
            }

            System.out.println("needed workers " + neededWorkers);

            if (neededWorkers > 0) {
                System.out.println("Trying to create " + (int)neededWorkers + " new workers");
                createdWorkers = workerFactory.makeInstances((int)neededWorkers);
                System.out.println("Created " + createdWorkers + " new workers");
                ec2.sleep("WorkersHandler");
                ec2.sleep("WorkersHandler");
                ec2.sleep("WorkersHandler");
                ec2.sleep("WorkersHandler");
            }
            else
                sqs.sleep("WorkersHandler");
        }

        System.out.println("workers handler finished");
    }
}
