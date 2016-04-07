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
        Thread finishedTweetsHandler = new Thread(new FinishedTweetsHandler(allJobs));
        Thread finishedTweetsHandler1 = new Thread(new FinishedTweetsHandler(allJobs));
        Thread finishedTweetsHandler2 = new Thread(new FinishedTweetsHandler(allJobs));
        Thread finishedTweetsHandler3 = new Thread(new FinishedTweetsHandler(allJobs));
        Thread finishedTweetsHandler4 = new Thread(new FinishedTweetsHandler(allJobs));
        Thread finishedTweetsHandler5 = new Thread(new FinishedTweetsHandler(allJobs));
        Thread finishedTweetsHandler6 = new Thread(new FinishedTweetsHandler(allJobs));
        Thread finishedTweetsHandler7 = new Thread(new FinishedTweetsHandler(allJobs));
        Thread workersHandler = new Thread(new WorkersHandler());

        pendingJobsHandler.start();
        finishedTweetsHandler.start();
        finishedTweetsHandler1.start();
        finishedTweetsHandler2.start();
        finishedTweetsHandler3.start();
        finishedTweetsHandler4.start();
        finishedTweetsHandler5.start();
        finishedTweetsHandler6.start();
        finishedTweetsHandler7.start();
        workersHandler.start();

        try {
            finishedTweetsHandler.join();
            pendingJobsHandler.join();
            workersHandler.join();
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