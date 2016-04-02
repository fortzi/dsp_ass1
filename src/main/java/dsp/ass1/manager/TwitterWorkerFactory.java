package dsp.ass1.manager;

/**
 * Created by user1 on 03/28/2016.
 */

import dsp.ass1.utils.InstanceFactory;

import java.io.IOException;

public class TwitterWorkerFactory extends InstanceFactory {
    public void makeInstance() throws IOException {
        super.makeInstance("hello-world");
    }
}
