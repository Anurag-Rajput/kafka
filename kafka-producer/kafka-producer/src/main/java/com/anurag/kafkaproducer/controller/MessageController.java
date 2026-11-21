package com.anurag.kafkaproducer.controller;

import com.anurag.kafkaproducer.dto.MessageRequest;
import com.anurag.kafkaproducer.dto.MessageResponse;
import com.anurag.kafkaproducer.service.KafkaMessagePublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/messages")
public class MessageController {

	private final KafkaMessagePublisher publisher;

	public MessageController(KafkaMessagePublisher publisher) {
		this.publisher = publisher;
	}

	@PostMapping
	public ResponseEntity<MessageResponse> publish(@RequestBody MessageRequest request) {
		if (request == null || request.message() == null || request.message().isBlank()) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "message must not be blank");
		}
		return ResponseEntity.ok(publisher.publish(request.message()));
	}
}
