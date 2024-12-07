package com.learning.kafka_retry_example.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.learning.kafka_retry_example.dto.Student;

@Service
public class KafkaConsumer {

	Logger logger = LoggerFactory.getLogger(getClass());

	@KafkaListener(topics = "${kafka.topic.name}", containerFactory = "containerFactory")
	public void receiveStudent(Student student) {
		logger.info("consumer consume event: "+student);
	}

}
