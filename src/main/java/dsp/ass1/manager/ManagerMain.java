package dsp.ass1.manager;

/**
 * Created by Ofer Caspi on 03/28/2016.
 *
 */

import java.io.IOException;
import java.util.HashMap;

public class ManagerMain {
    public static void main(String[] args) throws IOException {
        System.out.println("Starting manager node");

        HashMap<String, Job> allJobs = new HashMap<String, Job>();
        Thread pendingJobsHandler = new Thread(new PendingJobsHandler(allJobs));
        Thread finishedJobsHandler = new Thread(new FinishedTweetsHandler(allJobs));

        pendingJobsHandler.start();
        finishedJobsHandler.start();

        try {
            finishedJobsHandler.join();
            pendingJobsHandler.interrupt();
        } catch (InterruptedException e) {
            System.err.println("Threads interrupted.");
            e.printStackTrace();
        }

        System.out.println("Terminating manager node");
    }

    protected static class Auxiliary {
        public static boolean terminate = false;
        public static int workerCount = 0;
    }
}