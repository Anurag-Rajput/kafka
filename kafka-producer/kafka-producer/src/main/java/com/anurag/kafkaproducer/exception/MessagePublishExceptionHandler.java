package com.anurag.kafkaproducer.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class MessagePublishExceptionHandler {

	@ExceptionHandler(MessagePublishException.class)
	public ResponseEntity<PublishError> handlePublishFailure(MessagePublishException exception) {
		return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
			.body(new PublishError("Failed to publish message"));
	}

	public record PublishError(String error) {
	}
}
