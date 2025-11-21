/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.solace.demo.spring.faa.stdds;

import java.time.Instant;
import java.util.Date;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;

@SpringBootApplication
public class STDDSPosition {
	private final MongoTemplate mongoTemplate;
	String collectionName = "STDDSPosition";

	@Value("${spring.cloud.stream.bindings.sink-in-0.destination}")
	private String destination;

	@Value("${spring.cloud.stream.bindings.sink-in-0.group}")
	private String group;

	@Value( "${spring.cloud.stream.binders.local-solace.environment.solace.java.host}")
	private String host;

	@Value( "${spring.cloud.stream.binders.local-solace.environment.solace.java.msgVpn}")
	private String msgvpn;

	@Value( "${spring.cloud.stream.binders.local-solace.environment.solace.java.clientUsername}")
	private String username;

	@Value( "${spring.cloud.stream.binders.local-solace.environment.solace.java.clientPassword}")
	private String password;

	public STDDSPosition(MongoTemplate mongoTemplate) {

		this.mongoTemplate = mongoTemplate;
	}

	@PostConstruct
	public void init() {


		// 1. Create collection if it doesn't exist
		if (!mongoTemplate.collectionExists(collectionName)) {
			mongoTemplate.createCollection(collectionName);
		}

		// 2. Create indexes
		IndexOperations indexOps = mongoTemplate.indexOps(collectionName);

		indexOps.ensureIndex(
				new Index()
						.on("time", Sort.Direction.ASC)
						.expire(28800)   // time-to-live in seconds
						.named("time_1")
		);
		indexOps.ensureIndex(new Index()
				.on("SurfaceMovementEventMessage.status", Sort.Direction.ASC)
				.on("time", Sort.Direction.DESC).named("idx_status_time")
		);
		indexOps.ensureIndex(new Index().on("SurfaceMovementEventMessage.callsign", Sort.Direction.ASC).named("idx_aircraft_id"));
		indexOps.ensureIndex(
				new Index()
						.on("SurfaceMovementEventMessage.enhancedData.departureAirport", Sort.Direction.ASC)
						.on("SurfaceMovementEventMessage.enhancedData.destinationAirport", Sort.Direction.ASC)
						.on("time", Sort.Direction.DESC).named("idx_airport_pair_time")
		);
		indexOps.ensureIndex(new Index()
				.on("SurfaceMovementEventMessage.enhancedData.destinationAirport", Sort.Direction.ASC)
				.on("time", Sort.Direction.DESC).named("idx_arrival_time")
		);
		indexOps.ensureIndex(new Index()
				.on("SurfaceMovementEventMessage.enhancedData.departureAirport", Sort.Direction.ASC)
				.on("time", Sort.Direction.DESC).named("idx_departure_time")
		);
		indexOps.ensureIndex(new Index().on("SurfaceMovementEventMessage.airport", Sort.Direction.ASC).named("idx_airport"));
		indexOps.ensureIndex(new Index().on("SurfaceMovementEventMessage.aircraftType", Sort.Direction.ASC).named("idx_aircraft_type"));
		indexOps.ensureIndex(new Index().on("SurfaceMovementEventMessage.acAddress", Sort.Direction.ASC).named("idx_aircraft_registration"));
	}

	@PreDestroy
	public void cleanup() {
		String queueName = "scst/wk/" + group + "/plain/" + destination.replace('>', '_').replace('*', '_');

		try {
			JCSMPProperties props = new JCSMPProperties();
			props.setProperty(JCSMPProperties.HOST, host);
			props.setProperty(JCSMPProperties.VPN_NAME, msgvpn);
			props.setProperty(JCSMPProperties.USERNAME, username);
			props.setProperty(JCSMPProperties.PASSWORD, password);
			JCSMPSession session = JCSMPFactory.onlyInstance().createSession(props);
			session.connect();

			System.out.println("Deleting auto-provisioned queue: " + queueName);
			Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
			session.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
			session.closeSession();

			System.out.println("Queue: " + queueName + " deleted successfully");
		} catch (Exception e) {
			System.out.println("Failed to delete queue " + queueName);
			e.printStackTrace();

		}
	}

	public static void main(String[] args) {
		SpringApplication.run(STDDSPosition.class, args);
	}

	/*
	 *  Check out application.yml to see how to
	 *  1. Use `concurrency` for multi-threaded consumption
	 *  2. Use wildcard subscriptions
	 */
	@Bean
	public Consumer<String> sink() {
		return message -> {
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode jsonNode = null;
			ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
			Document doc = null;
			try {
				jsonNode = objectMapper.readTree(message);
				if (jsonNode instanceof ObjectNode objectNode) {
					Instant time = Instant.parse(objectNode.get("SurfaceMovementEventMessage").get("time").textValue());
					doc = Document.parse(writer.writeValueAsString(objectNode));
					doc.append("time", Date.from(time));
					mongoTemplate.save(doc, collectionName);
					System.out.println("Wrote Message!");
				}
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		};
	}
}
