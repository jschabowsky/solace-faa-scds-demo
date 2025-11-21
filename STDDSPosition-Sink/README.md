# STDDSPosition-Sink

The `STDDSPosition-Sink` application is a Spring Boot application that leverages Spring Cloud Stream to consume events about aircraft on the ground from the FAA from a message broker.

## Requirements

To run this sample, you will need to have installed:

- Java 17 or Above

## Code Tour

In the `STDDSPosition-Sink` application, review the source code which consumes flight plans published on the broker and performs the following steps:
- Gets the actual timestamp thats nested deep in the document, and adds it to a `time` attribute at the document root. This attribute is used by the database in order to expire documents after a certain amount of time.
- Uses the injected mongoTemplate to save the document into the defined collection

```java
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
```

The data for STDDSPosition is in the JSON format and after written to the database looks like:
```json lines
{
  _id: ObjectId('691f8906c4c0ac46c59d719f'),
  SurfaceMovementEventMessage: {
    callsign: 'N565CM',
    manualCallsign: 'N565CM',
    track: '1149',
    stid: '9452565',
    airport: 'KSLC',
    mode3ACode: '0336',
    manualMode3ACode: '0336',
    aircraftType: 'C206',
    manualAircraftType: 'C206',
    acAddress: 'A739BB',
    time: '2025-11-20T21:32:54.645Z',
    event: 'spotout',
    position: { latitude: '40.77829', longitude: '-111.96005' },
    altitude: '4225.0',
    status: 'onsurface',
    events: ''
  },
  time: ISODate('2025-11-20T21:32:54.645Z')
}
```

## Running the application

Make sure to update:
- Solace Broker connection details with the appropriate host, msgVpn, client username, and password in `application.yml`.
- MongoDB/DocumentDB connection details

The application is expected to be deployed within AWS, in our case Elastic Beanstalk and so the following secrets must be created in AWS Secrets Manager:
```yaml
      - aws-secretsmanager:solace/scds/broker
      - aws-secretsmanager:faa/documentdb/password
      - aws-secretsmanager:aws/accountid
```
The account id is specifically used in order to inject a unique group for each user, in a multiuser environment.

```sh
cd STDDSPosition-Sink
mvn clean spring-boot:run
```

🚀 Leverage the power of Spring Cloud Stream to build robust and scalable data production pipelines with ease! 🚀

