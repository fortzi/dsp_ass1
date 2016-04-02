package dsp.ass1.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by user1 on 04/02/2016.
 */
public class S3Helper {
    final String BUCKET_NAME = "dsp-ass1";

    AmazonS3 s3 = null;

    public S3Helper() {
        Region region = Region.getRegion(Regions.US_EAST_1);
        s3 = new AmazonS3Client();
        //s3.setRegion(region);
    }

    /**
     * Puts a file in the default bucket
     * @param folder The folder in which to put the file in.
     * @param file The file to put in S3.
     * @return The URL to the newly uploaded file.
     */
    public String putObject(Folders folder, File file) {
        String objectKey = folder + "/" + file.hashCode();
        PutObjectResult result = s3.putObject(new PutObjectRequest(BUCKET_NAME, objectKey, file));

        return objectKey;
    }

    /**
     * Gets an object from the default bucket.
     * @param objectKey The object's key in S3.
     * @return The object's content as a string.
     * @throws AmazonClientException if the result input was unable to be processed
     */
    public S3Object getObject(String objectKey) throws AmazonClientException {
        return s3.getObject(new GetObjectRequest(BUCKET_NAME, objectKey));
    }

    /**
     * Gets an object from the default bucket.
     * @param folder The folder in which to put the object.
     * @param objectName The suffix of the object's key in S3.
     * @return The object's content as a string.
     * @throws AmazonClientException if the result input was unable to be processed
     */
    public S3Object getObject(Folders folder, String objectName) throws AmazonClientException {
        return getObject(folder + "/" + objectName);
    }

    public ArrayList<S3Object> getAllObjects(Folders folder) {
        ArrayList<S3Object> allObjects = new ArrayList<S3Object>();

        ListObjectsRequest listObjectRequest = new ListObjectsRequest().
            withBucketName(BUCKET_NAME).
            withPrefix(folder.toString()).
            withDelimiter("/");
        ObjectListing objectListing = s3.listObjects(listObjectRequest);

        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            allObjects.add(getObject(objectSummary.getKey()));
        }

        return allObjects;
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

    /**
     * The available folders in the bucket.
     */
    public static enum Folders {
        FINISHED_JOBS   { public String toString() { return "finished-jobs"; } },
        PENDING_JOBS    { public String toString() { return "pending-jobs"; } }
    }
}
