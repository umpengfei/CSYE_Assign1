package CSYE;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.util.*;
import java.util.Scanner;
import java.io.BufferedInputStream;

/**
 * Hello world!
 *
 */
public class App 
{

    public static void main( String[] args ) {
        final String QUEUE_NAME = "sqsDemo";
        final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

        try {
            CreateQueueResult create_result = sqs.createQueue(QUEUE_NAME);
        } catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }

        String queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();

        while (true) {
            System.out.println("Please input 'Offer' to input value or 'Poll' to get value or 'Exit' to exit");

            Scanner offerOrPoll = new Scanner(System.in);
            String check = offerOrPoll.nextLine();

            if (check.equals("Offer")) {
                System.out.println("Please enter the ip addresses, enter 'end' to end input");
                Scanner stdin = new Scanner(new BufferedInputStream(System.in));
                while (stdin.hasNext()) {
                    String next = stdin.nextLine();
                    if (next.equals("end")) {
                        break;
                    }
                    SendMessageRequest send_msg_request = new SendMessageRequest()
                            .withQueueUrl(queueUrl)
                            .withMessageBody(next)
                            .withDelaySeconds(5);
                    sqs.sendMessage(send_msg_request);
                }
            } else if (check.equals("Poll")) {
                List<Message> messages = sqs.receiveMessage(queueUrl).getMessages();
                if (messages.size() == 0) {
                    System.out.println("Queue is empty");
                } else {
                    System.out.println("The next value ip address is " + messages.get(0).getBody());
                    sqs.deleteMessage(queueUrl, messages.get(0).getReceiptHandle());
                }
            }  else if (check.equals("Exit")) {
                break;
            } else {
                System.out.println("Invalid input, Please try again");
            }
        }

    }
}
