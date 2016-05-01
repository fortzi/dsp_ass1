package dsp.ass1.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.util.Random;

/**
 * Created by Ofer Caspi on 04/02/2016.
 *
 */
public class S3Helper {
    AmazonS3 s3 = null;
    Random randomizer;

    public S3Helper() {
        s3 = new AmazonS3Client();
        randomizer = new Random();
    }

    /**
     * Puts a file in the default bucket
     * @param folder The folder in which to put the file in.
     * @param file The file to put in S3.
     * @return The URL to the newly uploaded file.
     */
    public String putObject(Folders folder, File file) {
        String objectKey = folder + "/" + randomizer.nextInt();
        s3.putObject(new PutObjectRequest(Settings.BUCKET_NAME, objectKey, file));

        return objectKey;
    }

    /**
     * Gets an object from the default bucket.
     * @param objectKey The object's key in S3.
     * @return The object's content as a string.
     * @throws AmazonClientException if the result input was unable to be processed
     */
    public S3Object getObject(String objectKey) throws AmazonClientException {
        return s3.getObject(new GetObjectRequest(Settings.BUCKET_NAME, objectKey));
    }

    public static String getStringFromInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        StringBuilder result = new StringBuilder();

        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            result.append(line).append("\n");
        }

        return result.toString();
    }

    public void removeObject(String objectKey) {
        s3.deleteObject(Settings.BUCKET_NAME, objectKey);
    }

    public void removeObject(S3Object s3Object) {
        removeObject(s3Object.getKey());
    }

    /**
     * The available folders in the bucket.
     */
    public enum Folders {
        FINISHED_JOBS   { public String toString() { return "finished-jobs"; } },
        PENDING_JOBS    { public String toString() { return "pending-jobs"; } },
        STATISTICS    { public String toString() { return "statistics"; } }
    }
}
