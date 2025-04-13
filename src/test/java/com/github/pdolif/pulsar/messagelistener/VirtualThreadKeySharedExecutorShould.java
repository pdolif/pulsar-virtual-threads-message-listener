package com.github.pdolif.pulsar.messagelistener;

import org.apache.pulsar.client.api.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
public class VirtualThreadKeySharedExecutorShould {

    @Container
    public PulsarContainer pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:4.0.4"));

    private final String topicName = "topic1";
    private final String subscriptionName = "sub1";
    private final String message = "message1";

    private PulsarClient pulsarClient;
    private Producer<String> producer;
    private Consumer<String> consumer;

    private VirtualThreadKeySharedExecutor keySharedMessageListenerExecutor;
    private MessageRecorder messageListener;

    @BeforeEach
    public void setup() throws PulsarClientException {
        pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarContainer.getPulsarBrokerUrl())
                .build();

        producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        keySharedMessageListenerExecutor = new VirtualThreadKeySharedExecutor("executor");
        messageListener = new MessageRecorder();
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (producer != null) producer.close();
        if (consumer != null) consumer.close();
        if (keySharedMessageListenerExecutor != null) keySharedMessageListenerExecutor.close();
        if (pulsarClient != null) pulsarClient.close();
    }

    @Test
    public void executeListenerForMessageWithoutOrderingKey() throws PulsarClientException {
        // given a consumer with a message listener
        consumer = createConsumer(messageListener, keySharedMessageListenerExecutor);

        // when a message is produced without an ordering key
        producer.newMessage().value(message).send();

        // then the message listener receives the message
        awaitUntilMessagesReceived(messageListener, 1);
        var receivedMessage = messageListener.getMessages().get(0);
        assertThat(receivedMessage).isEqualTo(message);
    }

    @Test
    public void executeListenerForMessageWithOrderingKey() throws PulsarClientException {
        // given a consumer with a message listener
        consumer = createConsumer(messageListener, keySharedMessageListenerExecutor);

        // when a message is produced with an ordering key
        producer.newMessage()
                .value(message)
                .orderingKey("key".getBytes())
                .send();

        // then the message listener receives the message
        awaitUntilMessagesReceived(messageListener, 1);
        var receivedMessage = messageListener.getMessages().get(0);
        assertThat(receivedMessage).isEqualTo(message);
    }

    @Test
    public void executeListenerSequentiallyForMessagesWithSameOrderingKey() throws PulsarClientException {
        // given a message listener that sleeps for 10ms for each message
        var delay = 10;
        var delayedMessageListener = new MessageRecorder(delay);
        // given 100 messages that are produced with the same ordering key
        var messageCount = 100;
        var messages = IntStream.range(0, messageCount).mapToObj(i -> "message" + i).toList();
        for (String message : messages) {
            producer.newMessage()
                    .value(message)
                    .orderingKey("key".getBytes())
                    .send();
        }

        // when the messages are consumed with the delayed message listener
        consumer = createConsumer(delayedMessageListener, keySharedMessageListenerExecutor);

        // then the message listener should receive all messages sequentially
        awaitUntilMessagesReceived(delayedMessageListener, messageCount);
        assertThat(delayedMessageListener.consumeDuration()).isGreaterThanOrEqualTo(messageCount * delay);
    }

    @Test
    public void executeListenerConcurrentlyForMessagesWithDifferentOrderingKeys() throws PulsarClientException {
        // given a message listener that sleeps for 80ms for each message
        var delay = 80;
        var delayedMessageListener = new MessageRecorder(delay);
        // given 100 messages that are produced with unique ordering keys
        var messageCount = 100;
        var messages = IntStream.range(0, messageCount).mapToObj(i -> "message" + i).toList();
        for (String message : messages) {
            producer.newMessage()
                    .value(message)
                    .orderingKey(message.getBytes())
                    .send();
        }

        // when the messages are consumed with the delayed message listener
        consumer = createConsumer(delayedMessageListener, keySharedMessageListenerExecutor);

        // then the message listener should receive all messages concurrently
        awaitUntilMessagesReceived(delayedMessageListener, messageCount);
        var PERCENT_ERROR = 0.4;
        assertThat(delayedMessageListener.consumeDuration()).isLessThan((long) (delay * (1 + PERCENT_ERROR)));
    }

    @Test
    public void beLimitedByConsumerReceiverQueue() throws PulsarClientException {
        // given a message listener that sleeps for 10ms for each message
        var delay = 10;
        var delayedMessageListener = new MessageRecorder(delay);
        // given 100 messages that are produced with unique ordering keys
        var messageCount = 100;
        var messages = IntStream.range(0, messageCount).mapToObj(i -> "message" + i).toList();
        for (String message : messages) {
            producer.newMessage()
                    .value(message)
                    .orderingKey(message.getBytes())
                    .send();
        }

        // when the messages are consumed with the delayed message listener and a receiver queue size of 0
        consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionType(SubscriptionType.Key_Shared)
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .messageListener(delayedMessageListener)
                .messageListenerExecutor(keySharedMessageListenerExecutor)
                .receiverQueueSize(0) // disable message prefetching
                .subscribe();

        // then the message listener should receive all messages sequentially
        awaitUntilMessagesReceived(delayedMessageListener, messageCount);
        assertThat(delayedMessageListener.consumeDuration()).isGreaterThanOrEqualTo(messageCount * delay);
    }

    private Consumer<String> createConsumer(MessageRecorder listener, KeySharedExecutor executor)
            throws PulsarClientException {
        return pulsarClient.newConsumer(Schema.STRING)
                .subscriptionType(SubscriptionType.Key_Shared)
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .messageListener(listener)
                .messageListenerExecutor(executor)
                .subscribe();
    }

    private void awaitUntilMessagesReceived(MessageRecorder messageRecorder, int expectedMessagesCount) {
        await().pollInterval(10, MILLISECONDS)
                .until(() -> messageRecorder.receivedMessagesCount() == expectedMessagesCount);
    }

    public static class MessageRecorder implements MessageListener<String> {
        private final List<String> messages = Collections.synchronizedList(new ArrayList<>());
        private final int delay;
        private long firstReceivedTime = 0;
        private long lastReceivedTime = 0;

        public MessageRecorder(int delay) {
            this.delay = delay;
        }

        public MessageRecorder() {
            this(0);
        }

        @Override
        public void received(Consumer<String> consumer, Message<String> msg) {
            delay();

            long currentTime = System.currentTimeMillis();
            if (firstReceivedTime == 0) {
                firstReceivedTime = currentTime;
            }
            lastReceivedTime = currentTime;

            messages.add(msg.getValue());

            acknowledge(consumer, msg);
        }

        private void delay() {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private void acknowledge(Consumer<String> consumer, Message<String> msg) {
            try {
                consumer.acknowledge(msg);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }

        public List<String> getMessages() {
            return messages;
        }

        public int receivedMessagesCount() {
            return messages.size();
        }

        public long consumeDuration() {
            return lastReceivedTime - firstReceivedTime;
        }
    }

}
