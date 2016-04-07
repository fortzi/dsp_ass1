package dsp.ass1.worker;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import dsp.ass1.utils.Settings;
import dsp.ass1.utils.S3Helper;
import dsp.ass1.utils.SQSHelper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.*;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by doubled
 */
public class WorkerMain {

    private SQSHelper sqs;
    private S3Helper s3;
    StanfordCoreNLP  sentimentPipeline;
    StanfordCoreNLP NERPipeline;
    private int tweetsOk;
    private int tweetsFaulty;

    public static void main(String[] args) {
        try {
            new WorkerMain().WorkerRun();
        } catch (JSONException e) {
            new SQSHelper().sendMsgToQueue(SQSHelper.Queues.DEBUGGING,e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            new SQSHelper().sendMsgToQueue(SQSHelper.Queues.DEBUGGING,e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            new SQSHelper().sendMsgToQueue(SQSHelper.Queues.DEBUGGING,e.getMessage());
            e.printStackTrace();
        }

    }

    public WorkerMain() {
        sqs = new SQSHelper();
        s3 = new S3Helper();
        tweetsOk = 0;
        tweetsFaulty = 0;

        Properties props1 = new Properties();
        props1.put("annotators", "tokenize, ssplit, parse, sentiment");
        sentimentPipeline =  new StanfordCoreNLP(props1);

        Properties props2 = new Properties();
        props2.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        NERPipeline =  new StanfordCoreNLP(props2);
    }

    private void WorkerRun() throws JSONException, IOException {
        Message msg;
        String tweetText;
        JSONObject result;


        while(true) {
            // look for new job from pending tweets queue
            System.out.println("Waiting for new message");
            msg = sqs.getMsgFromQueue(SQSHelper.Queues.PENDING_TWEETS, true);

            // check whether this is an termination message
            if (msg.getMessageAttributes().containsKey(Settings.TERMINATION_ATTRIBUTE)) {
                break;
            }

            // if not termination message that this is new tweet to work on !
            System.out.println("extracting tweet text from url");
            result = new JSONObject();
            tweetText = getTxtFromTweet(msg.getBody());

            System.out.println("analyzing tweet");
            if(tweetText == null) {
                // tweet parsing from web was failed
                result.put("content", Settings.HTTP_ERROR + msg.getBody());
                result.put("sentiment", -1);
                result.put("entities", new JSONObject());
            }
            else {
                // analyzing tweet and pushing results to Json object
                result.put("content", tweetText);
                result.put("sentiment", sentimentAnalysis(tweetText));
                result.put("entities", new JSONObject(extractEntities(tweetText)));
            }

            // pushing final results to the manager via FINISHED TWEETS queue
            System.out.println("sending results");
            Map<String, String> attributes = new HashMap<String, String>();
            String job_id = msg.getMessageAttributes().get(Settings.JOB_ID_ATTRIBUTE).getStringValue();
            attributes.put(Settings.JOB_ID_ATTRIBUTE, job_id);
            sqs.sendMsgToQueue(SQSHelper.Queues.FINISHED_TWEETS, result.toString(), attributes);

            //now that the tweet analysis has been sent, we can delete original tweet msg from queue
            System.out.println("removing message from queue");
            sqs.removeMsgFromQueue(SQSHelper.Queues.PENDING_TWEETS, msg);
        }

        // remove termination message from queue
        sqs.removeMsgFromQueue(SQSHelper.Queues.PENDING_TWEETS, msg);

        // upload statistics.
        System.out.println("uploading statistics");
        String id = getInstanceId();
        File statistics = createStatisticsFile(id);
        s3.putObject(S3Helper.Folders.STATISTICS, statistics);
    }

    private String getTxtFromTweet(String url)  {
        Document doc;
        try {
            doc = Jsoup.connect(url).get();
        } catch (IOException e) {
            e.printStackTrace();
            tweetsFaulty++;
            return null;
        }

        Elements elements = doc.getElementsByClass("tweet-text");

        if(elements.size() == 0) {
            tweetsFaulty++;
            return null;
        }
        else {
            tweetsOk++;
            return elements.get(0).text();
        }
    }

    private int sentimentAnalysis(String tweet) {
        int mainSentiment = 0;
        if (tweet != null && tweet.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(tweet);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        return mainSentiment;
    }

    private Map<String, String> extractEntities(String tweet) {

        HashMap<String,String> entities = new HashMap<String, String>();

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(tweet);

        // run all Annotators on this text
        NERPipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);

                if(ne == null) {
                    System.out.println("null ne for " + word);
                    continue;
                }
                if(ne.matches("PERSON|LOCATION|ORGANIZATION"))
                    entities.put(word, ne);
            }
        }

        return entities;
    }

    private File createStatisticsFile(String id) throws IOException {

        File statisticsFile = new File(id + ".txt");
        statisticsFile.deleteOnExit();

        if(!statisticsFile.createNewFile())
            throw new IOException("cannot create statistics file with name " + id);

        Writer writer = new OutputStreamWriter(new FileOutputStream(statisticsFile));

        writer.write("Worker Statistics (" + id + ")\n\n");
        writer.write("Total Tweets analyzed: " + (tweetsFaulty+tweetsOk) + "\n");
        writer.write("Faulty tweets: " + (tweetsFaulty) + "\n");
        writer.write("Good tweets: " + (tweetsOk) + "\n\n");
        writer.write("End Of File");
        writer.close();

        return statisticsFile;
    }

    private String getInstanceId() throws IOException {
        Document doc = Jsoup.connect("http://169.254.169.254/latest/meta-data/instance-id").get();
        return doc.body().text();
    }
}