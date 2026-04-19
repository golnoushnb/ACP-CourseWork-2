package uk.ac.ed.inf.acp_submission_2.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import uk.ac.ed.inf.acp_submission_2.config.KafkaConfig;
import uk.ac.ed.inf.acp_submission_2.exception.ResourceNotFoundException;
import uk.ac.ed.inf.acp_submission_2.model.CounterMessage;
import uk.ac.ed.inf.acp_submission_2.model.SortMessage;
import uk.ac.ed.inf.acp_submission_2.util.Constants;
import uk.ac.ed.inf.acp_submission_2.util.JsonUtil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Service
public class KafkaService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonUtil jsonUtil;
    private final KafkaConfig kafkaConfig;

    public KafkaService(KafkaTemplate<String, String> kafkaTemplate,
                        JsonUtil jsonUtil,
                        KafkaConfig kafkaConfig) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonUtil = jsonUtil;
        this.kafkaConfig = kafkaConfig;
    }

    public void writeCounterMessages(String topicName, int messageCount) {
        if (topicName == null || topicName.isBlank()) {
            throw new ResourceNotFoundException("Topic not found");
        }

        if (messageCount < 0) {
            throw new IllegalArgumentException("Message count is invalid");
        }

        KafkaConsumer<String, String> consumer = null;

        try {
            consumer = kafkaConfig.createConsumer();

            List<org.apache.kafka.common.PartitionInfo> partitionInfos =
                    consumer.partitionsFor(topicName, Duration.ofMillis(1000));

            if (partitionInfos == null || partitionInfos.isEmpty()) {
                throw new ResourceNotFoundException("Topic not found");
            }

            for (int i = 0; i < messageCount; i++) {
                CounterMessage message = new CounterMessage(Constants.STUDENT_ID, i);
                String jsonMessage = jsonUtil.toJson(message);
                kafkaTemplate.send(topicName, jsonMessage).get();
            }

            kafkaTemplate.flush();
        } catch (ResourceNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to write messages to Kafka", e);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public List<String> readUntilTimeout(String topicName, long timeoutInMsec) {
        if (topicName == null || topicName.isBlank()) {
            throw new ResourceNotFoundException("Topic not found");
        }

        if (timeoutInMsec < 0) {
            throw new IllegalArgumentException("Timeout is invalid");
        }

        List<String> messages = new ArrayList<>();
        KafkaConsumer<String, String> consumer = null;

        try {
            consumer = kafkaConfig.createConsumer();

            List<org.apache.kafka.common.PartitionInfo> partitionInfos =
                    consumer.partitionsFor(topicName, Duration.ofMillis(1000));

            if (partitionInfos == null || partitionInfos.isEmpty()) {
                throw new ResourceNotFoundException("Topic not found");
            }

            List<org.apache.kafka.common.TopicPartition> topicPartitions = new ArrayList<>();
            for (org.apache.kafka.common.PartitionInfo info : partitionInfos) {
                topicPartitions.add(new org.apache.kafka.common.TopicPartition(info.topic(), info.partition()));
            }

            consumer.assign(topicPartitions);
            consumer.seekToBeginning(topicPartitions);

            long deadline = System.currentTimeMillis() + timeoutInMsec;

            while (true) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    break;
                }

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(Math.min(remaining, 200)));

                for (ConsumerRecord<String, String> record : records) {
                    messages.add(record.value());
                }
            }

            return messages;
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public List<SortMessage> readSortedMessages(String topicName, int messagesToConsider) {
        if (topicName == null || topicName.isBlank()) {
            throw new ResourceNotFoundException("Topic not found");
        }

        if (messagesToConsider < 0) {
            throw new IllegalArgumentException("messagesToConsider is invalid");
        }

        KafkaConsumer<String, String> consumer = null;
        List<SortMessage> messages = new ArrayList<>();

        try {
            consumer = kafkaConfig.createConsumer();

            List<org.apache.kafka.common.PartitionInfo> partitionInfos =
                    consumer.partitionsFor(topicName, Duration.ofMillis(1000));

            if (partitionInfos == null || partitionInfos.isEmpty()) {
                throw new ResourceNotFoundException("Topic not found");
            }

            List<org.apache.kafka.common.TopicPartition> topicPartitions = new ArrayList<>();
            for (org.apache.kafka.common.PartitionInfo info : partitionInfos) {
                topicPartitions.add(new org.apache.kafka.common.TopicPartition(info.topic(), info.partition()));
            }

            consumer.assign(topicPartitions);
            consumer.seekToBeginning(topicPartitions);

            long deadline = System.currentTimeMillis() + 2000;

            while (messages.size() < messagesToConsider) {
                if (System.currentTimeMillis() > deadline) {
                    break;
                }
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(200));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        SortMessage sortMessage = jsonUtil.fromJson(record.value(), SortMessage.class);
                        messages.add(sortMessage);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse JSON", e);
                    }

                    if (messages.size() == messagesToConsider) {
                        break;
                    }
                }
            }

            messages.sort(Comparator.comparingInt(SortMessage::getId));
            return messages;

        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }
}