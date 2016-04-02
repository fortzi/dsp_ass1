package dsp.ass1.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * Created by user1 on 04/02/2016.
 */
public class S3Helper {
    AmazonS3 s3 = null;

    public S3Helper() {
        s3 = new AmazonS3Client();
    }
}
