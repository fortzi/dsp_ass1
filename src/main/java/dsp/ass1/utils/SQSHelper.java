package dsp.ass1.utils;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by doubled on 0002, 02, 4, 2016.
 *
 */
public class SQSHelper {

    private AmazonSQSClient sqs;

    public SQSHelper() {
        sqs = new AmazonSQSClient();

    }

    private String sendMsgToQueue(Queues queue, String msg) {
        return sendMsgToQueue(queue, msg, new HashMap<String,String>());
    }

    public String sendMsgToQueue(Queues queue, String msg, Map<String,String> att) {
        String queueUrl = sqs.getQueueUrl(queue.toString()).getQueueUrl();
        SendMessageRequest sendRequest = new SendMessageRequest(queueUrl, msg);

        for(String key : att.keySet()) {
            sendRequest.addMessageAttributesEntry(key, new MessageAttributeValue()
                                                            .withStringValue(att.get(key))
                                                            .withDataType("String"));
        }

        SendMessageResult sendResult = sqs.sendMessage(sendRequest);
        return sendResult.getMessageId();
    }

    public String sendMsgToQueue(Queues queue, String msg, String attributeKey) {
        Map<String,String> atts = new HashMap<String, String>();
        atts.put(attributeKey,"true");
        return sendMsgToQueue(queue, msg, atts);
    }

    public int getMsgCount(Queues queue) {
        String queueUrl = sqs.getQueueUrl(queue.toString()).getQueueUrl();
        GetQueueAttributesRequest attributesRequest = new GetQueueAttributesRequest();
        attributesRequest.withQueueUrl(queueUrl);
        attributesRequest.withAttributeNames("ApproximateNumberOfMessages");

        GetQueueAttributesResult attributesResult = sqs.getQueueAttributes(attributesRequest);
        return Integer.parseInt(attributesResult.getAttributes().get("ApproximateNumberOfMessages"));
    }

    public Message getMsgFromQueue(Queues queue) {
        List<Message> messages;
        String queueUrl = sqs.getQueueUrl(queue.toString()).getQueueUrl();
        ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest(queueUrl);

        receiveRequest.withMessageAttributeNames("All");
        receiveRequest.withMaxNumberOfMessages(1);

        messages = sqs.receiveMessage(receiveRequest).getMessages();

        if(messages.size() == 0)
            return null;
        else
            return messages.get(0);
    }

    public void removeMsgFromQueue(Queues queue, Message msg) {
        String queueUrl = sqs.getQueueUrl(queue.toString()).getQueueUrl();

        DeleteMessageRequest deleteReq = new DeleteMessageRequest();
        deleteReq.withQueueUrl(queueUrl);
        deleteReq.withReceiptHandle(msg.getReceiptHandle());

        sqs.deleteMessage(deleteReq);
    }

    public void extendMessageVisibility(Queues queue, Message msg) {
        String queueUrl = sqs.getQueueUrl(queue.toString()).getQueueUrl();

        ChangeMessageVisibilityRequest visibilityRequest = new ChangeMessageVisibilityRequest();
        visibilityRequest.withQueueUrl(queueUrl);
        visibilityRequest.withVisibilityTimeout(90);
        visibilityRequest.withReceiptHandle(msg.getReceiptHandle());

        sqs.changeMessageVisibility(visibilityRequest);
    }

    public void sleep(String caller) {
        try {
            Thread.sleep(Settings.SLEEP_INTERVAL);
        } catch (InterruptedException e) {
            e.printStackTrace();
            debug(e,"sleep interrupted in " + caller);
        }
    }

    public void debug(Exception e, String msg) {
        String className, methodName;
        StringBuilder content = new StringBuilder();

        if(e.getStackTrace().length > 0) {
            className = e.getStackTrace()[0].getClassName();
            methodName = e.getStackTrace()[0].getMethodName();
        }
        else {
            className = "Unkown class";
            methodName = "Unkown method";
        }

        content.append("Exception in ").append(className).append(":").append(methodName).append("\n");
        content.append("Exception Message: ").append(e.getMessage()).append("\n");
        if(msg!=null)
            content.append("additional Message: ").append(msg).append("\n");
        content.append("Full stack trace: \n");

        for(StackTraceElement element : e.getStackTrace())
            content.append(element.toString()).append("\n");

        sendMsgToQueue(SQSHelper.Queues.DEBUGGING,  content.toString());
    }

    public void debug(Exception e) {
        debug(e,null);
    }

    public enum Queues {
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
        },
        DEBUGGING {
            public String toString() {
                return "dsp-ass1-debugging";
            }
        }
    }
}
