package dsp.ass1.manager;

/**
 * Created by user1 on 03/28/2016.
 */

import com.amazonaws.auth.AWSCredentials;
import dsp.ass1.utils.InstanceFactory;

import java.io.IOException;

public class TwitterWorkerFactory extends InstanceFactory {
    public TwitterWorkerFactory() {
        super("hello-world");
    }
}
