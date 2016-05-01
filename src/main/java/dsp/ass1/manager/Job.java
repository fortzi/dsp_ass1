package dsp.ass1.manager;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Ofer Caspi on 04/02/2016.
 *
 */
public class Job {
    PrintWriter results;
    File resultsFile;
    AtomicInteger remainingUrls;
    String id;
    boolean health;
    boolean initializing;

    public Job(String id) throws IOException {
        this.initializing = true;
        this.health = true;
        this.id = id;
        this.remainingUrls = new AtomicInteger(0);
        this.resultsFile = File.createTempFile("tmpfile", ".txt");
        resultsFile.deleteOnExit();
        this.results = new PrintWriter(new BufferedWriter(new FileWriter(resultsFile.getPath(), true)));
    }

    public void finsishInitiazling() {
        this.initializing = false;
    }

    public File getResultsFile() {
        return resultsFile;
    }

    public void setAsBroken() {
        this.health = false;
    }

    public void incRemainingUrls() {
        remainingUrls.incrementAndGet();
    }

    public void finalize() throws Throwable {
        results.close();
        results = null;
        super.finalize();
    }

    /**
     * Adds a result to the output file on disk
     * @param result The string to add to the file
     * @return Whether there are any more pending results
     */
    public synchronized boolean addResult(String result) {
        results.println(result);
        results.flush();
        return (remainingUrls.decrementAndGet() == 0) && !initializing;
    }

    public String getId() {
        return id;
    }

}
