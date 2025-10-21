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
import org.bson.Document;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import org.springframework.data.mongodb.core.MongoTemplate;

@SpringBootApplication
public class STDDSPosition {
	private final MongoTemplate mongoTemplate;

	public STDDSPosition(MongoTemplate mongoTemplate) {

		this.mongoTemplate = mongoTemplate;
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
	public Consumer<String> sink(){
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
					mongoTemplate.save(doc, "STDDSPosition");
					System.out.println("Wrote Message!");
				}
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
		};
	}
}
