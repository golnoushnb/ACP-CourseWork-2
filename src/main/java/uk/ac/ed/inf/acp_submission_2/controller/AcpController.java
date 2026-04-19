package uk.ac.ed.inf.acp_submission_2.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.inf.acp_submission_2.exception.ResourceNotFoundException;
import uk.ac.ed.inf.acp_submission_2.model.SplitterRequest;
import uk.ac.ed.inf.acp_submission_2.model.TransformRequest;
import uk.ac.ed.inf.acp_submission_2.service.KafkaService;
import uk.ac.ed.inf.acp_submission_2.service.RabbitMqService;
import uk.ac.ed.inf.acp_submission_2.service.SplitterService;
import uk.ac.ed.inf.acp_submission_2.service.TransformMessagesService;
import uk.ac.ed.inf.acp_submission_2.model.SortMessage;
import java.util.List;

@RestController
@RequestMapping("/api/v1/acp")
public class AcpController {

    private final RabbitMqService rabbitMqService;
    private final KafkaService kafkaService;
    private final SplitterService splitterService;
    private final TransformMessagesService transformMessagesService;

    public AcpController(RabbitMqService rabbitMqService,
                         KafkaService kafkaService,
                         SplitterService splitterService,
                         TransformMessagesService transformMessagesService) {
        this.rabbitMqService = rabbitMqService;
        this.kafkaService = kafkaService;
        this.splitterService = splitterService;
        this.transformMessagesService = transformMessagesService;
    }

    @PutMapping("/messages/rabbitmq/{queueName}/{messageCount}")
    public ResponseEntity<?> writeRabbitMessages(@PathVariable String queueName,
                                                 @PathVariable int messageCount) {
        try {
            rabbitMqService.writeCounterMessages(queueName, messageCount);
            return ResponseEntity.ok().build();
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
            return ResponseEntity.status(404).body(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Internal server error");
        }
    }

    @GetMapping("/messages/rabbitmq/{queueName}/{timeoutInMsec}")
    public ResponseEntity<?> readRabbitMessages(@PathVariable String queueName,
                                                @PathVariable long timeoutInMsec) {
        try {
            List<String> messages = rabbitMqService.readUntilTimeout(queueName, timeoutInMsec);
            return ResponseEntity.ok(messages);
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
            return ResponseEntity.status(404).body(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Internal server error");
        }
    }

    @GetMapping("/messages/sorted/rabbitmq/{queueName}/{messagesToConsider}")
    public ResponseEntity<?> readSortedRabbitMessages(@PathVariable String queueName,
                                                      @PathVariable int messagesToConsider) {
        try {
            List<SortMessage> messages = rabbitMqService.readSortedMessages(queueName, messagesToConsider);
            return ResponseEntity.ok(messages);
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
            return ResponseEntity.status(404).body(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Internal server error");
        }
    }

    @PutMapping("/messages/kafka/{writeTopic}/{messageCount}")
    public ResponseEntity<?> writeKafkaMessages(@PathVariable String writeTopic,
                                                @PathVariable int messageCount) {
        try {
            kafkaService.writeCounterMessages(writeTopic, messageCount);
            return ResponseEntity.ok().build();
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
            return ResponseEntity.status(404).body(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Internal server error");
        }
    }

    @GetMapping("/messages/kafka/{readTopic}/{timeoutInMsec}")
    public ResponseEntity<?> readKafkaMessages(@PathVariable String readTopic,
                                               @PathVariable long timeoutInMsec) {
        try {
            List<String> messages = kafkaService.readUntilTimeout(readTopic, timeoutInMsec);
            return ResponseEntity.ok(messages);
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
            return ResponseEntity.status(404).body(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Internal server error");
        }
    }

    @GetMapping("/messages/sorted/kafka/{topic}/{messagesToConsider}")
    public ResponseEntity<?> readSortedKafkaMessages(@PathVariable String topic,
                                                     @PathVariable int messagesToConsider) {
        try {
            List<SortMessage> messages = kafkaService.readSortedMessages(topic, messagesToConsider);
            return ResponseEntity.ok(messages);
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
            return ResponseEntity.status(404).body(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Internal server error");
        }
    }

    @PostMapping("/splitter")
    public ResponseEntity<?> splitter(@RequestBody SplitterRequest request) {
        try {
            splitterService.process(request);
            return ResponseEntity.ok().build();
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
            return ResponseEntity.status(404).body(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Internal server error");
        }
    }

    @PostMapping("/transformMessages")
    public ResponseEntity<?> transformMessages(@RequestBody TransformRequest request) {
        try {
            transformMessagesService.process(request);
            return ResponseEntity.ok().build();
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
            return ResponseEntity.status(404).body(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Internal server error");
        }
    }
}