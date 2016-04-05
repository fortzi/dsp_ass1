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
        this.results = new PrintWriter(new BufferedWriter(new FileWriter(resultsFile.getPath(), true)));
    }

    public File getResultsFile() {
        return resultsFile;
    }

    public void close() {
        results.close();
        if(!resultsFile.delete()) {
            System.err.println("Error while deleting temp file for job " + getId());
        }
    }

    public int addResult(String result) {
        results.println(result);
        results.flush();
        return --remainingUrls;
    }

    public boolean isComplete() {
        return remainingUrls == 0;
    }

    public String getId() {
        return id;
    }

    public String[] getUrls() {
        return urls;
    }
}
