package dsp.ass1.utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Created by doubled on 0002, 02, 4, 2016.
 */
public class SQSHelper {

    AmazonSQS sqs;

    public SQSHelper() {
        sqs = new AmazonSQSClient();
    }

    public String sendMsgToQueue(Queues queue, String msg) {
        return sendMsgToQueue(queue, msg, new HashMap<String,String>());
    }

    public String sendMsgToQueue(Queues queue, String msg, Map<String,String> att) {

        String queueUrl = sqs.getQueueUrl(queue.toString()).getQueueUrl();

        SendMessageRequest sendRequest = new SendMessageRequest(queueUrl, msg);

        for(String key : att.keySet()) {
            sendRequest.addMessageAttributesEntry(key, new MessageAttributeValue().withStringValue(att.get(key)));
        }

        SendMessageResult sendResult = sqs.sendMessage(sendRequest);

        return sendResult.getMessageId();
    }

    public Message getMsgFromQueue(Queues queue) {

        List<Message> messages;
        String queueUrl = sqs.getQueueUrl(queue.toString()).getQueueUrl();

        ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest(queueUrl);

        receiveRequest.withMaxNumberOfMessages(1);

        do {
            messages = sqs.receiveMessage(receiveRequest).getMessages();
        } while(messages.size() == 0);

        return messages.get(0);
    }


    public void removeMsgFromQueue(Queues queue, Message msg) {
        String queueUrl = sqs.getQueueUrl(queue.toString()).getQueueUrl();

        DeleteMessageRequest deleteReq = new DeleteMessageRequest();
        deleteReq.withQueueUrl(queueUrl);
        deleteReq.withReceiptHandle(msg.getReceiptHandle());

        sqs.deleteMessage(deleteReq);
    }




    public static enum Queues {
        PENDING_JOBS {
            public String toString() {
                return "dsp-ass1-pending-jobs";
            }
        },
        FINISHED_JOBS {
            public String toString() {
                return "dsp-ass1-finished-jobs";
            }
        },
        PENDING_TWEETS {
            public String toString() {
                return "dsp-ass1-pending-tweets";
            }
        },
        FINISHED_TWEETS {
            public String toString() {
                return "dsp-ass1-finished-tweets";
            }
        }


    }
}
