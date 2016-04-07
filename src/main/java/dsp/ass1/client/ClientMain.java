package dsp.ass1.client;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import dsp.ass1.utils.Constants;
import dsp.ass1.utils.InstanceFactory;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.SQSHelper;

import java.io.*;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by doubled
 */
public class ClientMain {

    private SQSHelper sqs;
    private S3Helper s3;
    private final String[] sentiments = { "Very Negative", "Negative", "Neutral", "Positive", "Very Positive" };

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
        System.out.println(new Timestamp((new java.util.Date()).getTime()) + " Client started.");

        if(!isManagerAlive()) {
            System.out.println("Manager was not found !");
            if(!createManager()) {
                return;
            }
        }
        else {
            System.out.println( "Manager was found !");
        }

        System.out.println("uploading " + fileName + " to S3");
        String inputFileKey = sendInputFile(fileName);
        System.out.println("file was uploaded successfully with key: " + inputFileKey);

        System.out.println("sending newJobSignal to SQS");
        String myId = sendNewJobSignal(inputFileKey, terminate);
        System.out.println("message sent successfully");

        System.out.println("my id: " + myId);

        System.out.println("waiting for job to finish");
        String resultFileKey = waitAndGetResultsFile(myId);

        if(resultFileKey == null) {
            System.out.println("job refused ! manager not accepting any more jobs");
            System.out.println("please try again later.");
            return;
        }

        System.out.println("job finished ! parsing results file.");
        try {
            parseResultsFile(resultFileKey);
        } catch (IOException e) {
            System.out.println("problam with file opening");
            e.printStackTrace();
        } catch (JSONException e) {
            System.out.println("problam parsing Json");
            e.printStackTrace();
        }

        System.out.println("parsing completed, removing file");
        System.out.println();
        s3.removeObject(resultFileKey);
        System.out.println(new Timestamp((new java.util.Date()).getTime()) + "finished. goodbye !");
    }

    /**
     * sending message to the PENDING_JOBS queue stating that new job file
     * is now available in the PENDING_JOBS folder in s3
     * @param fileKey the s3 file key of the new input file
     * @param terminate indicating if this should also be termination message
     * @return message id (will be the client id for the rest of the run)
     */
    private String sendNewJobSignal(String fileKey, boolean terminate) {

        Map<String,String> atts = new HashMap<String, String>();

        if(terminate)
            atts.put(Constants.TERMINATION_ATTRIBUTE,"true");

        return sqs.sendMsgToQueue(SQSHelper.Queues.PENDING_JOBS, fileKey, atts);

    }

    /**
     * listening on the finished jobs queue waiting to see my name !
     * @param myId my id
     * @return the results file
     */
    private String waitAndGetResultsFile(String myId) {
        Message msg = sqs.getMsgFromQueue(SQSHelper.Queues.FINISHED_JOBS, myId);
        sqs.removeMsgFromQueue(SQSHelper.Queues.FINISHED_JOBS, msg);

        if(msg.getMessageAttributes().containsKey(Constants.REFUSE_ATTRIBUTE))
            return null;

        return msg.getBody();
    }

    private String sendInputFile(String fileName) {
        return s3.putObject(S3Helper.Folders.PENDING_JOBS, new File(fileName));
    }

    private boolean createManager() {

        System.out.println("Creating manager !");
        new InstanceFactory(Constants.INSTANCE_MANAGER).makeInstance();
        System.out.println("Manager Created !");

        return true;
    }

    /**
     * find out whether there is an instance with tag "Type" as "manager
     * @return true if the manager exists
     */
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

    /**
     * parsing final merged result file received from the manager into a pretty html file
     * @param file the result file's s3 key identifier
     * @throws IOException
     * @throws JSONException
     */
    private void parseResultsFile(String file) throws IOException, JSONException {
        String line;
        JSONObject tweet, entities;
        InputStream stream = s3.getObject(file).getObjectContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

        PrintWriter writer = new PrintWriter("finalOutput.html");
        writer.println(
                "<html>\n" +
                "<head>\n" +
                "<style>\n" +
                "\tbody {\n" +
                "\t\tfont-family: 'Open Sans', Arial;\n" +
                "\t\ttext-align: center;\n" +
                "\t\tbackground: #F5F8FA;\n" +
                "\t\tmargin: 0px;\n" +
                "\t}\n" +
                "\t\n" +
                "\t#header {\n" +
                "\t\tbackground: #1DA1F2;\n" +
                "\t\tcolor: white;\n" +
                "\t\tfont-size: 3em;\n" +
                "\t\tmargin: 30px 0px 0px 0px;\n" +
                "\t\tpadding: 20px;\n" +
                "\t}\n" +
                "\t\n" +
                "\t#tweetsListContainer {\n" +
                "\t\tmargin: auto;\n" +
                "\t\twidth: 600px;\n" +
                "\t}\n" +
                "\t\n" +
                "\t#tweetsList {\n" +
                "\t\tmargin: 0px;\n" +
                "\t\ttext-align: left;\n" +
                "\t}\n" +
                "\t\n" +
                "\t.tweet {\n" +
                "\t\tborder: 1px solid #E1E8ED;\n" +
                "\t\tbackground: #FFF;\n" +
                "\t\tmargin: 30px;\n" +
                "\t}\n" +
                "\t\n" +
                "\t.tweet div {\n" +
                "\t\tpadding: 10px;\n" +
                "\t\tborder-bottom: 1px solid #E1E8ED;\n" +
                "\t}\n" +
                "\t\n" +
                "\tul {\n" +
                "\t\tlist-style-type: none;\n" +
                "\t\tpadding: 0px;\n" +
                "\t}\n" +
                "\t\n" +
                "\t.title { vertical-align: center; }\n" +
                "\t.tweet div:last-child { border-bottom: 0px; }\n" +
                "\t.title ul { list-style-type: square; }\n" +
                "\t.entities li { display: inline; }\n" +
                "\t.entities .entity-value { font-weight: normal; }\n" +
                "\t.entities, .title {\n" +
                "\t\tfont-weight: bold;\n" +
                "\t\tfont-size: 0.8em;\n" +
                "\t\tvertical-align: center;\n" +
                "\t}\n" +
                "\t\n" +
                "\t.title .sentiment {\n" +
                "\t\tbackground: pink;\n" +
                "\t\twidth: 20px;\n" +
                "\t\theight: 20px;\n" +
                "\t\tdisplay: inline-block;\n" +
                "\t\tmargin-right: 10px;\n" +
                "\t}\n" +
                "\t.sentiment-0 .title .sentiment { background: darkred; }\n" +
                "\t.sentiment-1 .title .sentiment { background: red; }\n" +
                "\t.sentiment-2 .title .sentiment { background: black; }\n" +
                "\t.sentiment-3 .title .sentiment { background: lightgreen; }\n" +
                "\t.sentiment-4 .title .sentiment { background: darkgreen; }\n" +
                "\t\n" +
                "</style>\n" +
                "</head>\n" +
                "<body>\n" +
                "\t<div id=\"header\">\n" +
                "\t\tTwitter Sentiment Analysis & Entity Recognition\n" +
                "\t</div>\n" +
                "<div id=\"tweetsListContainer\">\n" +
                "\t<ul id=\"tweetsList\">");

        while((line = reader.readLine()) != null) {
            tweet = new JSONObject(line);
            int sentiment = tweet.getInt("sentiment");
            String sentimentDescription = "Bad Sentiment Value";
            if (sentiment < 0) {
                sentimentDescription = "Error processing tweet";
            } else if (sentiment < sentiments.length) {
                sentimentDescription = sentiments[sentiment];
            }

            writer.println(
                    "<li class=\"tweet sentiment-" + sentiment + "\">\n" +
                    "\t\t\t<div class=\"title\"><span class=\"sentiment\">&nbsp;</span>Sentiment: " + sentimentDescription + "</div>\n" +
                    "\t\t\t<div class=\"content\">" + tweet.getString("content") + "</div>\n" +
                    "\t\t\t<div class=\"entities\">\n");

            entities = tweet.getJSONObject("entities");
            if (entities.length() == 0) {
                writer.println("\t\t\t\t&nbsp;");
            } else {
                Iterator<?> keys = entities.keys();
                writer.println("\t\t\t\t<ul>");
                while( keys.hasNext() ){
                    String key = (String)keys.next();
                    String value = entities.getString(key);
                    writer.println("\t\t\t\t\t<li>" + key + ":<span class=\"entity-value\">" + value + "</span></li>");
                }
                writer.println("\t\t\t\t</ul>");
            }

            writer.println(
                    "\t\t\t</div>\n" +
                    "\t\t</li>");
        }

        writer.println(
                "\t</ul>\n" +
                "</div>\n" +
                "</body>\n" +
                "</html>");

        writer.close();
    }
}
