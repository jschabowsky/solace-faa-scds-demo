# FDPSPosition-Sink

The `FDPSPosition-Sink` application is a Spring Boot application that leverages Spring Cloud Stream to consume events about airbone aircraft from the FAA from a message broker.

## Requirements

To run this sample, you will need to have installed:

- Java 17 or Above

## Code Tour

In the `FDPSPosition-Sink` application, review the source code which consumes flight plans published on the broker and performs the following steps:
- Gets the actual timestamp thats nested deep in the document, and adds it to a `time` attribute at the document root. This attribute is used by the database in order to expire documents after a certain amount of time.
- Uses the injected mongoTemplate to save the document into the defined collection

```java
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
                Instant time = Instant.parse(objectNode.get("message").get("flight").get("timestamp").textValue());
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

The data for FDPSPosition is in the JSON format and after written to the database looks like: 
```json
{
  _id: ObjectId('691f76b2b2b5493085a3ec1e'),
  message: {
    flight: {
      centre: 'ZNY',
      source: 'TH',
      system: 'SLC',
      timestamp: '2025-11-20T20:14:05.760Z',
      arrival: {
        arrivalPoint: 'KPHL',
        runwayPositionAndTime: { runwayTime: { estimated: { time: '2025-11-20T20:42:00Z' } } }
      },
      controllingUnit: { sectorIdentifier: '50', unitIdentifier: 'ZNY' },
      departure: {
        departurePoint: 'KART',
        runwayPositionAndTime: { runwayTime: { actual: { time: '2025-11-20T20:01:00Z' } } }
      },
      enRoute: {
        position: {
          positionTime: '2025-11-20T20:14:03Z',
          reportSource: 'SURVEILLANCE',
          targetPositionTime: '2025-11-20T20:14:03Z',
          actualSpeed: { surveillance: { uom: 'KNOTS', text: '427.0' } },
          altitude: { uom: 'FEET', text: '19400.0' },
          position: {
            location: {
              srsName: 'urn:ogc:def:crs:EPSG::4326',
              pos: '42.789444 -76.178611'
            }
          },
          targetAltitude: { uom: 'FEET', text: '19400.0' },
          targetPosition: {
            srsName: 'urn:ogc:def:crs:EPSG::4326',
            pos: '42.788889 -76.178611'
          },
          trackVelocity: {
            x: { uom: 'KNOTS', text: '18.0' },
            y: { uom: 'KNOTS', text: '-426.0' }
          }
        }
      },
      flightIdentification: {
        aircraftIdentification: 'PDT5974',
        computerId: '274',
        siteSpecificPlanId: '345'
      },
      flightStatus: { fdpsFlightStatus: 'ACTIVE' },
      gufi: {
        codeSpace: 'urn:uuid',
        text: 'fa67d5b0-a907-43e9-a11e-1a025aecba91'
      },
      operator: { operatingOrganization: { organization: { name: 'PDT' } } },
      supplementalData: {
        additionalFlightInformation: {
          nameValue: [
            { name: 'MSG_SEQ_NO', value: '23396811' },
            {
              name: 'FDPS_GUFI',
              value: 'us.fdps.2025-11-20T18:34:04Z.000/01/500'
            },
            { name: 'FLIGHT_PLAN_SEQ_NO', value: '4' },
            { name: 'SOURCE_TIME_AND_SEQ', value: '2014056733' },
            { name: 'SOURCE_TIME', value: '20_14_05' },
            { name: 'ADSB_02M_52B', value: '-ACC954' }
          ]
        }
      },
      assignedAltitude: { simple: { uom: 'FEET', text: '19000.0' } },
      flightPlan: { identifier: 'KB66844500' }
    }
  },
  time: ISODate('2025-11-20T20:14:05.760Z')
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
cd FDPSPosition-Sink
mvn clean spring-boot:run
```

🚀 Leverage the power of Spring Cloud Stream to build robust and scalable data production pipelines with ease! 🚀

