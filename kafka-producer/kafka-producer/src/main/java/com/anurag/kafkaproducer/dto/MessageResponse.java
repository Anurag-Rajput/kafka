package com.anurag.kafkaproducer.dto;

public record MessageResponse(String topic, int partition, long offset) {
}
