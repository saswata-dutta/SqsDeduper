package org.saswata;

import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchRequestEntry;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;


public class SqsWorker implements Runnable {
    // reuse the placeholder dummy value in map to save space
    static final Object DUMMY = new Object();

    final SnsClient sns;
    final SqsClient sqs;
    final String queueUrl;
    final String topicArn;
    final ReceiveMessageRequest sqsReceiveReq;
    final ConcurrentMap<Long, Object> dedup;

    public SqsWorker(SqsClient sqs, String queueUrl,
                     SnsClient sns, String topicArn,
                     ConcurrentMap<Long, Object> dedup) {

        this.sns = sns;
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.topicArn = topicArn;
        this.sqsReceiveReq = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20)
                .build();
        this.dedup = dedup;
    }

    @Override
    public void run() {

        while (true) {
            try {
                // long poll, wait for atmost 20sec
                ReceiveMessageResponse response = sqs.receiveMessage(sqsReceiveReq);
                if (response == null || response.messages().isEmpty()) continue;

                List<DeleteMessageBatchRequestEntry> sqsAcks = new ArrayList<>();
                List<PublishBatchRequestEntry> snsMssgs = new ArrayList<>();
                for (Message m : response.messages()) {
                    if (!isEligible(m) || isDuplicate(m)) continue;

                    snsMssgs.add(
                            PublishBatchRequestEntry.builder()
                                    .id(m.messageId())
                                    .message(m.body())
                                    .build());
                    sqsAcks.add(
                            DeleteMessageBatchRequestEntry.builder()
                                    .receiptHandle(m.receiptHandle())
                                    .build());
                }

                PublishBatchRequest snsBatch = PublishBatchRequest.builder()
                        .topicArn(topicArn)
                        .publishBatchRequestEntries(snsMssgs)
                        .build();
                sns.publishBatch(snsBatch);

                DeleteMessageBatchRequest sqsBatchDel = DeleteMessageBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(sqsAcks)
                        .build();
                sqs.deleteMessageBatch(sqsBatchDel);

            } catch (Exception e) {
                // log and ignore errors in main loop, let visibility timeout and SQS retry handle it
                e.printStackTrace();
            }
        }
    }

    static boolean isEligible(Message m) {
        // TODO implement more rigorous check based on body
        // using string contains for market-id
        boolean valid = m != null && m.body() != null && !m.body().isBlank();
        return valid;
    }

    boolean isDuplicate(Message m) {
        // TODO extract oid from m.body()
        // oid is 17 digits can convert to long to avoid overhead of String (store and hash)
        long id = 123L;
        return dedup.putIfAbsent(id, DUMMY) != null;
    }
}
