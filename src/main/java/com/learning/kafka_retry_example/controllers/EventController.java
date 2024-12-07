package com.learning.kafka_retry_example.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.learning.kafka_retry_example.dto.Student;
import com.learning.kafka_retry_example.service.KafkaPublisher;

@RestController
public class EventController {

	@Autowired
	KafkaPublisher kafkaPublisher;

	@PostMapping("/publish/event")
	public ResponseEntity<Object> publishEvent(@RequestBody Student student) {
		try {
			kafkaPublisher.publish(student);
			return ResponseEntity.ok("Event sent");
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.internalServerError().build();
		}
	}

}
