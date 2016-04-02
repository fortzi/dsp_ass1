package dsp.ass1.manager;

/**
 * Created by Ofer Caspi on 03/28/2016.
 */

import java.io.IOException;

public class ManagerMain {
    public static void main(String[] args) throws IOException {
        /*

        Forever:
            Download jobs from sqs
            For each job:


        */

        final TwitterWorkerFactory twitterWorkerFactory = new TwitterWorkerFactory();

        new Thread() {
            public void run() {
                try {
                    System.out.println("Creating worker instance");
                    twitterWorkerFactory.makeInstance();
                    System.out.println("Created worker instance");
                } catch (IOException e) {
                    System.out.println("TwitterWorkerFactory Error: " + e);
                }
            }
        }.start();
    }
}
