package dsp.ass1.manager;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

import java.io.*;
import java.util.Map;

/**
 * Created by user1 on 04/02/2016.
 */
public class Job {
    String[] urls;
    File results;

    public Job(String[] urls) throws IOException {
        this.urls = urls;
        this.results = File.createTempFile(String.valueOf(urls.hashCode()), ".txt");
    }

    public void addResult(String tweetContent, int sentiment, Map<String, String> entities) throws IOException {
        JSONObject result = new JSONObject();

        try {
            result.put("content", tweetContent);
        } catch (JSONException e) {

        }

        Writer writer = new OutputStreamWriter(new FileOutputStream(results));

        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");

        writer.close();
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
