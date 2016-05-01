package dsp.ass1.manager;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by doubled on 0002, 02, 5, 2016.
 */
public class Jobs extends ConcurrentHashMap<String, Job> {

    public Jobs() {
        super();
    }

    /**
     * if there is a job that is not broken so the hashmap is not empty
     * @return empty or not
     */
    public boolean isEmpty() {
        for(Job job : this.values())
            if(job.health)
                return false;
        return true;
    }
}