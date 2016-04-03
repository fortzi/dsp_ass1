package dsp.ass1.worker;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by doubled on 0002, 02, 4, 2016.
 */
public class WorkerMain {

    public static void main(String[] args) {
        HashMap<String,String> map = new HashMap<String, String>();
        map.put("kaki","brown");
        map.put("pipi","yellow");
        try {
            addResult("this is the tweet content \n second line",3,map);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void addResult(String tweetContent, int sentiment, Map<String, String> entities) throws Exception {
        JSONObject result = new JSONObject();

        try {
            result.put("content", tweetContent);
            result.put("sentiment", sentiment);
            result.put("entities", new JSONObject(entities));
        } catch (JSONException e) {
            throw new Exception("This should not happen.");
        }


        System.out.println(result.toString());
    }
}
