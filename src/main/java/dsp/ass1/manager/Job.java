package dsp.ass1.manager;

import java.io.*;
import java.util.Arrays;

/**
 * Created by Ofer Caspi on 04/02/2016.
 *
 */
public class Job {
    String[] urls;
    PrintWriter results;
    File resultsFile;
    int remainingUrls;
    String id;

    public Job(String[] urls, String id) throws IOException {
        this.id = id;
        this.urls = urls;
        this.remainingUrls = urls.length;
        this.resultsFile = File.createTempFile(String.valueOf(Arrays.hashCode(urls)), ".txt");
        resultsFile.deleteOnExit();
        this.results = new PrintWriter(new BufferedWriter(new FileWriter(resultsFile.getPath(), true)));
    }

    public File getResultsFile() {
        return resultsFile;
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
        return --remainingUrls == 0;
    }

    public String getId() {
        return id;
    }

    public String[] getUrls() {
        return urls;
    }
}
