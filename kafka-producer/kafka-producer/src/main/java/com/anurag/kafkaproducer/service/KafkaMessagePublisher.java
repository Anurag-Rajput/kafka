package com.anurag.kafkaproducer.service;

import java.util.concurrent.ExecutionException;

import com.anurag.kafkaproducer.dto.MessageResponse;
import com.anurag.kafkaproducer.exception.MessagePublishException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessagePublisher {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final String topic;

	public KafkaMessagePublisher(KafkaTemplate<String, String> kafkaTemplate,
			@Value("${app.kafka.topic}") String topic) {
		this.kafkaTemplate = kafkaTemplate;
		this.topic = topic;
	}

	public MessageResponse publish(String message) {
		try {
			SendResult<String, String> result = kafkaTemplate.send(topic, message).get();
			RecordMetadata metadata = result.getRecordMetadata();
			return new MessageResponse(metadata.topic(), metadata.partition(), metadata.offset());
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new MessagePublishException("Interrupted while publishing Kafka message", exception);
		}
		catch (ExecutionException | RuntimeException exception) {
			Throwable cause = exception instanceof ExecutionException && exception.getCause() != null
					? exception.getCause() : exception;
			throw new MessagePublishException("Failed to publish Kafka message", cause);
		}
	}
}
