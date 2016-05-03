package dsp.ass1.worker;

import dsp.ass1.utils.EC2Helper;
import dsp.ass1.utils.SQSHelper;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by doubled on 0003, 03, 5, 2016.
 */
class LivenessHandler implements Runnable {

    private EC2Helper ec2;
    private AtomicBoolean liveness;

    LivenessHandler() {
        ec2 = new EC2Helper();
        liveness = new AtomicBoolean(true);
    }

    public void run() {

        try {
            while(ec2.isManagerAlive()) {
                ec2.sleep("LivenessHandler");
                ec2.sleep("LivenessHandler");
                ec2.sleep("LivenessHandler");
                ec2.sleep("LivenessHandler");
            }
            /* if loop exited its only because manager is dead ! */
            liveness.set(false);
        }
        catch (Exception e) {
            new SQSHelper().debug(e);
            liveness.set(false);
        }
    }

    boolean isAlive() {
        return liveness.get();
    }
}
