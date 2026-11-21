package com.anurag.kafkaproducer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import com.anurag.kafkaproducer.service.KafkaMessageConsumer;

@SpringBootTest
@AutoConfigureMockMvc
class KafkaProducerApplicationTests {

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private KafkaMessageConsumer consumer;

	@Test
	void consumerRecordsReceivedMessages() {
		consumer.consume("Consumed message");

		org.assertj.core.api.Assertions.assertThat(consumer.getReceivedMessages())
			.contains("Consumed message");
	}

	@Test
	void publishesMessageAndReturnsAcknowledgementMetadata() throws Exception {
		RecordMetadata metadata = mock(RecordMetadata.class);
		when(metadata.topic()).thenReturn("javatechie-topic");
		when(metadata.partition()).thenReturn(2);
		when(metadata.offset()).thenReturn(17L);
		SendResult<String, String> sendResult = mock(SendResult.class);
		when(sendResult.getRecordMetadata()).thenReturn(metadata);
		when(kafkaTemplate.send("javatechie-topic", "Hello"))
			.thenReturn(CompletableFuture.completedFuture(sendResult));

		mockMvc.perform(post("/api/messages")
				.contentType("application/json")
				.content("{\"message\":\"Hello\"}"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.topic").value("javatechie-topic"))
			.andExpect(jsonPath("$.partition").value(2))
			.andExpect(jsonPath("$.offset").value(17));

		verify(kafkaTemplate).send("javatechie-topic", "Hello");
	}

	@ParameterizedTest
	@ValueSource(strings = { "", "null", "{}", "{\"message\":null}", "{\"message\":\"   \"}", "{invalid" })
	void rejectsMissingBlankOrInvalidMessageBodies(String body) throws Exception {
		mockMvc.perform(post("/api/messages")
				.contentType("application/json")
				.content(body))
			.andExpect(status().isBadRequest());

		verifyNoInteractions(kafkaTemplate);
	}

	@Test
	void returnsGatewayErrorWhenKafkaSendFails() throws Exception {
		when(kafkaTemplate.send("javatechie-topic", "Hello"))
			.thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));

		mockMvc.perform(post("/api/messages")
				.contentType("application/json")
				.content("{\"message\":\"Hello\"}"))
			.andExpect(status().isBadGateway())
			.andExpect(jsonPath("$.error").value("Failed to publish message"));

		verify(kafkaTemplate).send("javatechie-topic", "Hello");
	}
}
