package dsp.ass1.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;

/**
 * Created by user1 on 04/02/2016.
 */
public class S3Helper {
    final String BUCKET_NAME = "dsp-ass1";

    AmazonS3 s3 = null;

    public S3Helper() {
        Region region = Region.getRegion(Regions.US_EAST_1);

        s3 = new AmazonS3Client();
        s3.setRegion(region);
    }

    /**
     * Puts a file in the default bucket
     * @param folder The folder in which to put the file in.
     * @param objectName The key representing the object in S3.
     * @param file The file to put in S3.
     */
    public void putObject(Folders folder, String objectName, File file) {
        String objectKey = folder + "/" + objectName;
        s3.putObject(new PutObjectRequest(BUCKET_NAME, objectKey, file));
    }

    /**
     * Gets an object from the default bucket.
     * @param folder The folder in which to put the object.
     * @param objectName The key representing the object in S3.
     * @return The object's content as a string.
     * @throws AmazonClientException if the result input was unable to be processed
     */
    public String getObject(Folders folder, String objectName) throws AmazonClientException {
        String objectKey = folder + "/" + objectName;
        S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, objectKey));
        String result = "";

        try {
            result = getStringFromInputStream(object.getObjectContent());
        } catch (IOException e) {
            throw new AmazonClientException("Error while processing the result stream.", e);
        }

        return result;
    }

    private String getStringFromInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        StringBuilder result = new StringBuilder();

        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            result.append(line).append("\n");
        }

        return result.toString();
    }

    /**
     * The available folders in the bucket.
     */
    public static enum Folders {
        FINISHED_JOBS {
            public String toString() {
                return "finished-jobs";
            }
        },
        PENDING_JOBS {
            public String toString() {
                return "pending-jobs";
            }
        }
    }
}
