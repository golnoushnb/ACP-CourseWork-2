package uk.ac.ed.inf.acp_submission_2.service;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import uk.ac.ed.inf.acp_submission_2.exception.ResourceNotFoundException;
import uk.ac.ed.inf.acp_submission_2.model.CounterMessage;
import uk.ac.ed.inf.acp_submission_2.model.SortMessage;
import uk.ac.ed.inf.acp_submission_2.util.Constants;
import uk.ac.ed.inf.acp_submission_2.util.JsonUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Service
public class RabbitMqService {

    private final RabbitTemplate rabbitTemplate;
    private final JsonUtil jsonUtil;

    public RabbitMqService(RabbitTemplate rabbitTemplate, JsonUtil jsonUtil) {
        this.rabbitTemplate = rabbitTemplate;
        this.jsonUtil = jsonUtil;
    }

    public void writeCounterMessages(String queueName, int messageCount) {
        if (queueName == null || queueName.isBlank()) {
            throw new ResourceNotFoundException("Queue not found: " + queueName);
        }

        if (messageCount < 0) {
            throw new IllegalArgumentException("Message count is invalid");
        }

        for (int i = 0; i < messageCount; i++) {
            CounterMessage message = new CounterMessage(Constants.STUDENT_ID, i);
            String jsonMessage = jsonUtil.toJson(message);

            try {
                rabbitTemplate.convertAndSend(queueName, jsonMessage);
            } catch (Exception e) {
                if (isRabbitNotFound(e)) {
                    throw new ResourceNotFoundException("Queue not found: " + queueName);
                }
                throw e;
            }
        }
    }

    public List<String> readUntilTimeout(String queueName, long timeoutInMsec) {
        if (queueName == null || queueName.isBlank()) {
            throw new ResourceNotFoundException("Queue not found: " + queueName);
        }

        if (timeoutInMsec < 0) {
            throw new IllegalArgumentException("Timeout is invalid");
        }

        List<String> messages = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutInMsec;

        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }

            Object msg;
            try {
                msg = rabbitTemplate.receiveAndConvert(queueName);
            } catch (Exception e) {
                if (isRabbitNotFound(e)) {
                    throw new ResourceNotFoundException("Queue not found: " + queueName);
                }
                throw e;
            }

            if (msg != null) {
                messages.add(convertMessageToString(msg));
            } else {
                try {
                    Thread.sleep(Math.min(remaining, 10));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("RabbitMQ read interrupted", e);
                }
            }
        }

        return messages;
    }

    public List<SortMessage> readSortedMessages(String queueName, int messagesToConsider) {
        if (queueName == null || queueName.isBlank()) {
            throw new ResourceNotFoundException("Queue not found: " + queueName);
        }
        if (messagesToConsider < 0) {
            throw new IllegalArgumentException("messagesToConsider is invalid");
        }

        List<SortMessage> messages = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 2000;

        while (messages.size() < messagesToConsider) {
            if (System.currentTimeMillis() > deadline) {
                break;
            }
            Object msg;
            try {
                msg = rabbitTemplate.receiveAndConvert(queueName);
            } catch (Exception e) {
                if (isRabbitNotFound(e)) {
                    throw new ResourceNotFoundException("Queue not found: " + queueName);
                }
                throw e;
            }
            if (msg != null) {
                String json = convertMessageToString(msg);
                try {
                    SortMessage sortMessage = jsonUtil.fromJson(json, SortMessage.class);
                    messages.add(sortMessage);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse JSON", e);
                }
            } else {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("RabbitMQ read interrupted", e);
                }
            }
        }

        messages.sort(Comparator.comparingInt(SortMessage::getId));
        return messages;
    }
    private String convertMessageToString(Object msg) {
        if (msg instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return msg.toString();
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