package uk.ac.ed.inf.acp_submission_2.service;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import uk.ac.ed.inf.acp_submission_2.exception.ResourceNotFoundException;
import uk.ac.ed.inf.acp_submission_2.model.TombstoneSummary;
import uk.ac.ed.inf.acp_submission_2.model.TransformMessage;
import uk.ac.ed.inf.acp_submission_2.model.TransformRequest;
import uk.ac.ed.inf.acp_submission_2.util.JsonUtil;

import java.nio.charset.StandardCharsets;

@Service
public class TransformMessagesService {

    private static final double ADDED_VALUE = 10.5;
    private static final String TOMBSTONE_KEY = "TOMBSTONE";

    private final RabbitTemplate rabbitTemplate;
    private final RedisService redisService;
    private final JsonUtil jsonUtil;

    public TransformMessagesService(RabbitTemplate rabbitTemplate,
                                    RedisService redisService,
                                    JsonUtil jsonUtil) {
        this.rabbitTemplate = rabbitTemplate;
        this.redisService = redisService;
        this.jsonUtil = jsonUtil;
    }

    public void process(TransformRequest request) {
        validateRequest(request);

        int totalMessagesWritten = 0;
        int totalMessagesProcessed = 0;
        int totalRedisUpdates = 0;
        double totalValueWritten = 0.0;
        double totalAdded = 0.0;

        for (int i = 0; i < request.getMessageCount(); i++) {
            Object rawMessage = waitForNextRabbitMessage(request.getReadQueue());
            String json = convertMessageToString(rawMessage);
            TransformMessage message = jsonUtil.fromJson(json, TransformMessage.class);

            if (TOMBSTONE_KEY.equals(message.getKey())) {
                // Spec says tombstone affects all keys in redis
                redisService.flushAll();

                // Continue counting; tombstone itself counts once as a redis update
                totalMessagesWritten++;
                totalRedisUpdates++;

                TombstoneSummary summary = new TombstoneSummary(
                        totalMessagesWritten,
                        totalMessagesProcessed,
                        totalRedisUpdates,
                        totalValueWritten,
                        totalAdded
                );

                rabbitTemplate.convertAndSend(request.getWriteQueue(), jsonUtil.toJson(summary));
                continue;
            }

            String key = message.getKey();
            Integer version = message.getVersion();
            Double value = message.getValue();

            if (key == null || version == null || value == null) {
                throw new RuntimeException("Invalid normal message");
            }

            String storedVersionValue = redisService.getValue(key);

            if (storedVersionValue == null || Integer.parseInt(storedVersionValue) < version) {
                redisService.setValue(key, String.valueOf(version));

                TransformMessage updatedMessage = new TransformMessage(
                        key,
                        version,
                        value + ADDED_VALUE
                );

                rabbitTemplate.convertAndSend(request.getWriteQueue(), jsonUtil.toJson(updatedMessage));

                totalMessagesWritten++;
                totalMessagesProcessed++;
                totalRedisUpdates++;
                totalAdded += ADDED_VALUE;
                totalValueWritten += value + ADDED_VALUE;
            } else {
                rabbitTemplate.convertAndSend(request.getWriteQueue(), json);

                totalMessagesWritten++;
                totalValueWritten += value;
            }
        }
    }

    private Object waitForNextRabbitMessage(String queueName) {
        while (true) {
            try {
                Object rawMessage = rabbitTemplate.receiveAndConvert(queueName);
                if (rawMessage != null) {
                    return rawMessage;
                }
            } catch (Exception e) {
                if (isRabbitNotFound(e)) {
                    throw new ResourceNotFoundException("Queue not found: " + queueName);
                }
                throw e;
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("RabbitMQ read interrupted", e);
            }
        }
    }

    private void validateRequest(TransformRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Request is invalid");
        }

        if (isBlank(request.getReadQueue())) {
            throw new ResourceNotFoundException("readQueue not found");
        }

        if (isBlank(request.getWriteQueue())) {
            throw new ResourceNotFoundException("writeQueue not found");
        }

        if (request.getMessageCount() < 1 || request.getMessageCount() > 500) {
            throw new IllegalArgumentException("messageCount is invalid");
        }
    }

    private String convertMessageToString(Object msg) {
        if (msg instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return msg.toString();
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private boolean isRabbitNotFound(Throwable e) {
        Throwable current = e;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains("NOT_FOUND")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}