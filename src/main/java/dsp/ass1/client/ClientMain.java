package dsp.ass1.client;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import dsp.ass1.utils.Constants;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.SQSHelper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by doubled on 0002, 02, 4, 2016.
 */
public class ClientMain {

    private SQSHelper sqs;
    private S3Helper s3;

    public static void main(String[] args) {

        if(args.length < 1){
            System.out.println("arguments are missing");
            return;
        }

        ClientMain client = new ClientMain();
        boolean terminate = args.length > 1 && "terminate".equals(args[1]);

        client.clientRun(terminate, args[0]);
    }

    public ClientMain() {
        sqs = new SQSHelper();
        s3 = new S3Helper();
    }


    private void clientRun(boolean terminate, String fileName) {


        if(!isManagerAlive()) {
            System.out.println("Manager was not found !");
            if(!createManager()) {
                return;
            }
        }
        else {
            System.out.println("Manager was found !");
        }

        System.out.println("uploading " + fileName + " to S3");
        String inputFileKey = sendInputFile(fileName);
        System.out.println("file was uploaded successfully with key: " + inputFileKey);

        System.out.println("sending newJobSignal to SQS");
        String myId = sendNewJobSignal(inputFileKey);
        System.out.println("message sent successfully");

        System.out.println("my id: " + myId);

        if(terminate) {
            System.out.println("sending termination message");
            sendTeminateSignal();
            System.out.println("termination message sent successfully");
        }

        System.out.println("waiting for job to finish");
        String resultFileKey = waitAndGetResultsFile(myId);
        System.out.println("job finished !");



        System.out.println("parsing file");
        try {
            parseResultsFile(resultFileKey);
        } catch (IOException e) {
            System.out.println("problam with file opening");
            e.printStackTrace();
        } catch (JSONException e) {
            System.out.println("problam parsing Json");
            e.printStackTrace();
        }

        System.out.println("parsing completed, goodbye !");
    }

    /**
     *
     * @param fileKey the file id of the file that
     * @return
     */
    private String sendNewJobSignal(String fileKey) {
        return sqs.sendMsgToQueue(SQSHelper.Queues.PENDING_JOBS,fileKey);
    }

    /**
     * listening on the finished jobs queue waiting to see my name !
     * @param myId my id
     * @return the results file
     */
    private String waitAndGetResultsFile(String myId) {
        Message msg = sqs.getMsgFromQueue(SQSHelper.Queues.FINISHED_JOBS, myId);
        sqs.removeMsgFromQueue(SQSHelper.Queues.FINISHED_JOBS, msg);
        return msg.getBody();
    }

    private String sendInputFile(String fileName) {
        return s3.putObject(S3Helper.Folders.PENDING_JOBS, new File(fileName));
    }

    private void sendTeminateSignal() {
        Map<String,String> atts = new HashMap<String, String>();
        atts.put(Constants.TERMINATION_MESSAGE,"true");
        sqs.sendMsgToQueue(SQSHelper.Queues.PENDING_JOBS, "Termination messages !!", atts);
    }

    private boolean createManager() {

        System.out.println("Creating manager !");
        new ManagerFactory().makeInstance();
        System.out.println("Manager Created !");

        return true;
    }

    private boolean isManagerAlive() {

        //creating ec2 object instance
        AmazonEC2Client amazonEC2Client = new AmazonEC2Client();
        amazonEC2Client.setRegion(Constants.REGION);

        // defining filters:
        // first filter for tag "Type = manager"
        Filter filterType = new Filter("tag:Type");
        filterType.withValues("manager");
        // second filter for status != terminated
        Filter filterState = new Filter("instance-state-name");
        filterState.withValues("pending","running");

        // creating request for list of instances
        DescribeInstancesRequest describeReq = new DescribeInstancesRequest();
        describeReq.withFilters(filterType, filterState);

        // executing the request
        DescribeInstancesResult describeRes = amazonEC2Client.describeInstances(describeReq);

        return describeRes.getReservations().size() != 0;
    }

    private void parseResultsFile(String file) throws IOException, JSONException {

        String line;
        JSONObject tweet;

        PrintWriter writer = new PrintWriter("finalOutput.html");
        writer.println("<html>");
        writer.println("<body>");

        InputStream stream = s3.getObject(file).getObjectContent();

        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

        while((line = reader.readLine()) != null) {
            tweet = new JSONObject(line);

            writer.print("<p><b><font color=\"");
            switch (tweet.getInt("sentiment")) {
                case 0:
                    writer.print("DarkRed\">");
                    break;
                case 1:
                    writer.print("Red\">");
                    break;
                case 2:
                    writer.print("Black\">");
                    break;
                case 3:
                    writer.print("LightGreen\">");
                    break;
                case 4:
                    writer.print("DarkGreen\">");
                    break;
            }

            writer.print(tweet.getString("content"));
            writer.print("</font></b>");

            HashMap<String, String> result =
                    new ObjectMapper().readValue(tweet.getString("entities"), HashMap.class);

            for (Map.Entry<String, String> entry : result.entrySet()) {
                writer.print("<br>");
                writer.print(entry.getKey());
                writer.print(" = ");
                writer.print(entry.getValue());
            }

            writer.print("</p>\n");
        }

        writer.println("</body>");
        writer.println("</html>");

        writer.close();
    }

}
