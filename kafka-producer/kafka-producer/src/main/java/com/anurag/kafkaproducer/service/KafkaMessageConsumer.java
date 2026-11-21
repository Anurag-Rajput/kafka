package com.anurag.kafkaproducer.service;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaMessageConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageConsumer.class);

	private final List<String> receivedMessages = new CopyOnWriteArrayList<>();

	@KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
	public void consume(String message) {
		receivedMessages.add(message);
		LOGGER.info("Received Kafka message: {}", message);
	}

	public List<String> getReceivedMessages() {
		return List.copyOf(receivedMessages);
	}
}
