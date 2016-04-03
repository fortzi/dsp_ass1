package dsp.ass1.manager;

/**
 * Created by Ofer Caspi on 03/28/2016.
 *
 */

import java.io.IOException;
import java.util.HashMap;

public class ManagerMain {
    public static void main(String[] args) throws IOException {
        HashMap<String, Job> allJobs = new HashMap<String, Job>();

        (new Thread(new PendingJobsHandler(allJobs))).start();
        (new Thread(new FinishedTweetsHandler(allJobs))).start();
    }

    protected static class Auxiliary {
        public static boolean terminate = false;
        public static int workerCount = 0;
    }
}
