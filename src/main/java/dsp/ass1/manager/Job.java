package dsp.ass1.manager;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;

/**
 * Created by user1 on 04/02/2016.
 */
public class Job {
    String[] urls;
//    File results;
    PrintWriter results;
    int remainingUrls;

    public Job(String[] urls) throws IOException {
        this.urls = urls;
        this.remainingUrls = urls.length;
        File file = File.createTempFile(String.valueOf(urls.hashCode()), ".txt");
        this.results = new PrintWriter(new BufferedWriter(new FileWriter(file.getPath(), true)));
    }

    public int addResult(String tweetContent, int sentiment, Map<String, String> entities) {
        JSONObject result = new JSONObject();

        try {
            result.put("content", tweetContent);
            result.put("sentiment", sentiment);
            result.put("entities", new JSONObject(entities));
        } catch (JSONException e) {
            // Only happens when the key is null
            System.err.println(e.getMessage());
            return remainingUrls;
        }

        results.println(result.toString());
        return --remainingUrls;
    }

    public String getUrlAt(int i) {
        if (i < urls.length) {
            return urls[i];
        }

        throw new ArrayIndexOutOfBoundsException();
    }

    public String[] getUrls() {
        return urls;
    }
}
