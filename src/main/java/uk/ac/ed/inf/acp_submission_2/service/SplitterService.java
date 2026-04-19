package uk.ac.ed.inf.acp_submission_2.service;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import uk.ac.ed.inf.acp_submission_2.exception.ResourceNotFoundException;
import uk.ac.ed.inf.acp_submission_2.model.SplitterMessage;
import uk.ac.ed.inf.acp_submission_2.model.SplitterRequest;
import uk.ac.ed.inf.acp_submission_2.util.JsonUtil;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;

@Service
public class SplitterService {

    private static final String COUNT_EVEN = "count_even";
    private static final String COUNT_ODD = "count_odd";
    private static final String AVERAGE_EVEN = "average_even";
    private static final String AVERAGE_ODD = "average_odd";

    // Internal helper keys for accurate running averages
    private static final String SUM_EVEN = "sum_even";
    private static final String SUM_ODD = "sum_odd";

    private final RabbitTemplate rabbitTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RedisService redisService;
    private final JsonUtil jsonUtil;

    public SplitterService(RabbitTemplate rabbitTemplate,
                           KafkaTemplate<String, String> kafkaTemplate,
                           RedisService redisService,
                           JsonUtil jsonUtil) {
        this.rabbitTemplate = rabbitTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.redisService = redisService;
        this.jsonUtil = jsonUtil;
    }

    public void process(SplitterRequest request) {
        validateRequest(request);

        for (int i = 0; i < request.getMessageCount(); i++) {
            Object rawMessage = null;
            long deadline = System.currentTimeMillis() + 2000;

            while (rawMessage == null) {
                if (System.currentTimeMillis() > deadline) {
                    break;
                }
                try {
                    rawMessage = rabbitTemplate.receiveAndConvert(request.getReadQueue());
                } catch (Exception e) {
                    if (isRabbitNotFound(e)) {
                        throw new ResourceNotFoundException("Queue not found: " + request.getReadQueue());
                    }
                    throw e;
                }
                if (rawMessage == null) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("RabbitMQ read interrupted", e);
                    }
                }
            }

            if (rawMessage == null) {
                break;
            }

            String json = convertMessageToString(rawMessage);
            SplitterMessage message = jsonUtil.fromJson(json, SplitterMessage.class);

            if (message.getId() % 2 == 0) {
                processEvenMessage(message, json, request);
            } else {
                processOddMessage(message, json, request);
            }
        }
    }

    private void processEvenMessage(SplitterMessage message, String json, SplitterRequest request) {
        redisService.putHashValue(
                request.getRedisHashEven(),
                String.valueOf(message.getId()),
                json
        );

        int oldCount = readIntValue(COUNT_EVEN);
        double oldSum = readDoubleValue(SUM_EVEN);

        int newCount = oldCount + 1;
        double newSum = oldSum + message.getValue();
        double newAverage = newSum / newCount;

        redisService.setValue(COUNT_EVEN, String.valueOf(newCount));
        redisService.setValue(SUM_EVEN, doubleToRedisString(newSum));
        redisService.setValue(AVERAGE_EVEN, formatToTwoDecimals(newAverage));

        kafkaTemplate.send(request.getWriteTopicEven(), json);
        kafkaTemplate.flush();
    }

    private void processOddMessage(SplitterMessage message, String json, SplitterRequest request) {
        redisService.putHashValue(
                request.getRedisHashOdd(),
                String.valueOf(message.getId()),
                json
        );

        int oldCount = readIntValue(COUNT_ODD);
        double oldSum = readDoubleValue(SUM_ODD);

        int newCount = oldCount + 1;
        double newSum = oldSum + message.getValue();
        double newAverage = newSum / newCount;

        redisService.setValue(COUNT_ODD, String.valueOf(newCount));
        redisService.setValue(SUM_ODD, doubleToRedisString(newSum));
        redisService.setValue(AVERAGE_ODD, formatToTwoDecimals(newAverage));

        kafkaTemplate.send(request.getWriteTopicOdd(), json);
        kafkaTemplate.flush();
    }

    private void validateRequest(SplitterRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Request is invalid");
        }

        if (isBlank(request.getReadQueue())) {
            throw new ResourceNotFoundException("readQueue not found");
        }

        if (isBlank(request.getWriteTopicOdd())) {
            throw new ResourceNotFoundException("writeTopicOdd not found");
        }

        if (isBlank(request.getWriteTopicEven())) {
            throw new ResourceNotFoundException("writeTopicEven not found");
        }

        if (isBlank(request.getRedisHashOdd())) {
            throw new ResourceNotFoundException("redisHashOdd not found");
        }

        if (isBlank(request.getRedisHashEven())) {
            throw new ResourceNotFoundException("redisHashEven not found");
        }

        if (request.getMessageCount() < 1 || request.getMessageCount() > 500) {
            throw new IllegalArgumentException("messageCount is invalid");
        }
    }

    private int readIntValue(String key) {
        String value = redisService.getValue(key);
        if (value == null || value.isBlank()) {
            return 0;
        }
        return Integer.parseInt(value);
    }

    private double readDoubleValue(String key) {
        String value = redisService.getValue(key);
        if (value == null || value.isBlank()) {
            return 0.0;
        }
        return Double.parseDouble(value);
    }

    private String formatToTwoDecimals(double value) {
        return BigDecimal.valueOf(value)
                .setScale(2, RoundingMode.HALF_UP)
                .toPlainString();
    }

    private String doubleToRedisString(double value) {
        return BigDecimal.valueOf(value).toPlainString();
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