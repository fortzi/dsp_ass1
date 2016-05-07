package dsp.ass1.manager;

/**
 * Created by Ofer Caspi on 03/28/2016.
 *
 */

import dsp.ass1.utils.SQSHelper;
import dsp.ass1.utils.Settings;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ManagerMain {
    public static void main(String[] args) throws IOException {
        System.out.println("Starting manager node");

        Jobs allJobs = new Jobs();

        ExecutorService executor = Executors.newFixedThreadPool(7);

        executor.submit(new FinishedTweetsHandler(allJobs));
        executor.submit(new FinishedTweetsHandler(allJobs));
        executor.submit(new FinishedTweetsHandler(allJobs));
        executor.submit(new FinishedTweetsHandler(allJobs));
        executor.submit(new FinishedTweetsHandler(allJobs));

        executor.submit(new PendingJobsHandler(allJobs));
        executor.submit(new WorkersHandler(allJobs));


        executor.shutdown();

        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            System.err.println("Threads interrupted.");
            e.printStackTrace();
            new SQSHelper().debug(e);
        }

        System.out.println("Terminating manager node");
    }

    protected static class Auxiliary {
        static AtomicBoolean terminate = new AtomicBoolean(false);
        static AtomicInteger ratio = new AtomicInteger(Settings.TWEETS_PER_WORKER);
    }
}