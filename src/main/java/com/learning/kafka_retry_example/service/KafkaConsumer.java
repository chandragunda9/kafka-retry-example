package com.learning.kafka_retry_example.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.learning.kafka_retry_example.dto.Student;

@Service
public class KafkaConsumer {

	Logger logger = LoggerFactory.getLogger(getClass());

//	@KafkaListener(topics = "${kafka.topic.name}", containerFactory = "containerFactory")
//	public void receiveStudent(Student student) {
//		logger.info("consumer consume event: "+student);
//	}

	@RetryableTopic(attempts = "4")
	@KafkaListener(topics = "${kafka.topic.name}", containerFactory = "containerFactory")
	public void receiveStudent(Student student, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.RECEIVED_PARTITION) String partition, @Header(KafkaHeaders.OFFSET) String offset)
			throws RuntimeException {
		logger.info("consumer consuming event: " + student + " topic: " + topic + " offset: " + offset);
		System.out.println(student.getName());
		if (!student.getName().equalsIgnoreCase("chandra") && !student.getName().equalsIgnoreCase("venkata")) {
			throw new RuntimeException("Invalid Data!");
		}
	}

	@DltHandler
	void dltConsume(Student student, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.RECEIVED_PARTITION) String partition, @Header(KafkaHeaders.OFFSET) String offset) {
		logger.info("dlt event: " + student + " topic: " + topic + " offset: " + offset);
	}

}
