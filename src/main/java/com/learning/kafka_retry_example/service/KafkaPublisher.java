package com.learning.kafka_retry_example.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.learning.kafka_retry_example.dto.Student;

@Service
public class KafkaPublisher {

	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	@Value("${kafka.topic.name}")
	String studentTopic;

	public void publish(Student student) {
		CompletableFuture<SendResult<String, Object>> completableFuture = kafkaTemplate.send(studentTopic, student);
		completableFuture.whenComplete((res, ex) -> {
			if (ex == null) {
				System.out.println(
						"Sent message: " + studentTopic + " partition: " + res.getRecordMetadata().partition());
				System.out.println("Sent message: " + studentTopic + " offset: " + res.getRecordMetadata().offset());
			} else {
				System.out.println("Unable to sent message - Exception e=[" + ex.getMessage() + "]");
			}
		});
	}

}
