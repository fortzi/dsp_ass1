package dsp.ass1.manager;

/**
 * Created by Ofer Caspi on 03/28/2016.
 *
 */

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ManagerMain {
    public static void main(String[] args) throws IOException {
        System.out.println("Starting manager node");

        ConcurrentHashMap<String, Job> allJobs = new ConcurrentHashMap<String, Job>();
        Thread pendingJobsHandler = new Thread(new PendingJobsHandler(allJobs));
        Thread finishedJobsHandler = new Thread(new FinishedTweetsHandler(allJobs));
        Thread WorkersHandler = new Thread(new WorkersHandler());

        pendingJobsHandler.start();
        finishedJobsHandler.start();
        WorkersHandler.start();

        try {
            finishedJobsHandler.join();
            pendingJobsHandler.join();
            WorkersHandler.join();
        } catch (InterruptedException e) {
            System.err.println("Threads interrupted.");
            e.printStackTrace();
        }

        System.out.println("Terminating manager node");
    }

    protected static class Auxiliary {
        public static AtomicBoolean terminate = new AtomicBoolean(false);
    }
}