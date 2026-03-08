package com.argus.producer.publisher;

import com.argus.producer.model.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wraps the Google Cloud Pub/Sub Publisher client.
 * TransactionGenerator and ProducerController respectively.
 */
@Component
public class PubSubPublisher {
    private static final Logger log = LoggerFactory.getLogger(PubSubPublisher.class);

    @Value("${pubsub.topic-path}")
    private String topicPath;

    private final ObjectMapper objectMapper;

    // Google cloud pubsub client
    private Publisher publisher;

    // Metrics
    private final AtomicLong publishedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);

    public PubSubPublisher() {
        this.objectMapper = new ObjectMapper();
    }

    // Lifecycle
    @PostConstruct
    public void init() throws IOException {
        // TopicName parses "projects/{project}/topics/{topic}" into a structured object
        TopicName topicName = TopicName.parse(topicPath);
        this.publisher = Publisher.newBuilder(topicName).build();

        log.info("Publisher initialized");
    }

    @PreDestroy
    public void shutdown() {
        if (publisher != null) {
            try {
                publisher.shutdown();
                publisher.awaitTermination(5, TimeUnit.SECONDS);
                log.info("PubSubPublisher shut down cleanly. Total published: {}", publishedCount.get());
            } catch (InterruptedException e) {
                log.warn("PubSubPublisher shutdown interrupted. Some messages may be lost.");
                Thread.currentThread().interrupt(); // restore interrupt flag
            }
        }
    }

    // Publishing
    public void publish(Transaction transaction) {
        try {
            // Serialize transaction
            String json = objectMapper.writeValueAsString(transaction);

            // JSON string to byte string
            ByteString data = ByteString.copyFromUtf8(json);

            // Build pubsub message
            PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();

            // Publish asynchronously
            ApiFuture<String> future = publisher.publish(message);

            // Register callbacks
            ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

                @Override
                public void onSuccess(String messageId) {
                    publishedCount.incrementAndGet();
                    log.debug("Published tx_id={} -> pubsub_id={}", transaction.getTxId(), messageId);
                }

                @Override
                public void onFailure(Throwable t) {
                    failedCount.incrementAndGet();
                    log.error("Failed to publish tx_id={}: {}", transaction.getTxId(), t.getMessage());
                }
            }, Executors.newSingleThreadExecutor());

        } catch (Exception e) {
            failedCount.incrementAndGet();
            log.error("Serialization failed for transaction: {}", e.getMessage());
        }
    }

    public long getPublishedCount() {
        return publishedCount.get();
    }

    public long getFailedCount() {
        return failedCount.get();
    }
}