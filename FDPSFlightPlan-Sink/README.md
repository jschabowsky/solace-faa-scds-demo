# FDPSFlightPlan-Sink 

The `FDPSFlightPlan-Sink` application is a Spring Boot application that leverages Spring Cloud Stream to consume flight plans from the FAA from a message broker.

## Requirements

To run this sample, you will need to have installed:

- Java 17 or Above

## Code Tour

In the `FDPSFlightPlan-Sink` application, review the source code which consumes flight plans published on the broker and performs the following steps:
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
The data for FDPSFlightPlan is in the JSON format and after written to the database looks like:
```json lines
{
  _id: ObjectId('691f72f945221179c34a889b'),
  message: {
    flight: {
      centre: 'ZME',
      flightType: 'SCHEDULED',
      source: 'AH',
      system: 'SLC',
      timestamp: '2025-11-20T19:58:49.819Z',
      agreed: {
        route: {
          initialFlightRules: 'IFR',
          nasRouteText: 'KMKL./.CNG242060..FTZ.TRTLL6.KORD/2123'
        }
      },
      aircraftDescription: {
        aircraftAddress: 'AD87AA',
        equipmentQualifier: 'L',
        registration: 'N971DC',
        wakeTurbulence: 'M',
        aircraftType: { icaoModelIdentifier: 'E145' },
        capabilities: {
          standardCapabilities: 'STANDARD',
          navigation: {
            navigationCode: 'D F G W',
            performanceBasedCode: 'B2 B3 B4 C2 D2'
          },
          surveillance: {
            otherSurveillanceCapabilities: '260B',
            surveillanceCode: 'S B1'
          }
        },
        accuracy: {
          cmsFieldType: [
            {
              phase: 'ARRIVAL',
              type: 'RNV',
              uom: 'NAUTICAL_MILES',
              text: '1.0'
            },
            {
              phase: 'ENROUTE',
              type: 'RNV',
              uom: 'NAUTICAL_MILES',
              text: '2.0'
            },
            {
              phase: 'DEPARTURE',
              type: 'RNV',
              uom: 'NAUTICAL_MILES',
              text: '1.0'
            }
          ]
        }
      },
      arrival: {
        arrivalPoint: 'KORD',
        arrivalAerodromeAlternate: { code: 'KMKE' },
        runwayPositionAndTime: { runwayTime: { estimated: { time: '2025-11-20T21:23:00Z' } } }
      },
      departure: {
        departurePoint: 'KMKL',
        runwayPositionAndTime: { runwayTime: { actual: { time: '2025-11-20T19:43:00Z' } } }
      },
      enRoute: { beaconCodeAssignment: { currentBeaconCode: '1346' } },
      flightIdentification: {
        aircraftIdentification: 'LYM5880',
        computerId: '700',
        siteSpecificPlanId: '363'
      },
      flightStatus: { fdpsFlightStatus: 'ACTIVE' },
      gufi: {
        codeSpace: 'urn:uuid',
        text: '43debe1f-d7b3-4bd7-8b06-3b7f66968d62'
      },
      operator: { operatingOrganization: { organization: { name: 'KEY LIME' } } },
      originator: { aftnAddress: 'KMKLYFYX' },
      supplementalData: {
        additionalFlightInformation: {
          nameValue: [
            { name: 'MSG_SEQ_NO', value: '61241239' },
            {
              name: 'FDPS_GUFI',
              value: 'us.fdps.2025-11-20T18:53:49Z.000/12/500'
            },
            { name: 'FLIGHT_PLAN_SEQ_NO', value: '5' },
            { name: 'SOURCE_TIME_AND_SEQ', value: '1958495429' },
            { name: 'SOURCE_TIME', value: '19_58_49' },
            { name: 'FLIGHT_PLAN_REV_NO', value: '04' }
          ]
        }
      },
      assignedAltitude: { simple: { uom: 'FEET', text: '36000.0' } },
      coordination: {
        coordinationTime: '2025-11-20T19:58:00Z',
        coordinationTimeHandling: 'E',
        coordinationFix: {
          fix: 'CNG',
          distance: { uom: 'NAUTICAL_MILES', text: '60.0' },
          radial: { uom: 'DEGREES', text: '242.0' }
        }
      },
      flightPlan: { identifier: 'KM68029500' },
      requestedAirspeed: { nasAirspeed: { uom: 'KNOTS', text: '447.0' } }
    }
  },
  time: ISODate('2025-11-20T19:58:49.819Z')
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
cd FDPSFlightPlan-Sink
mvn clean spring-boot:run
```

🚀 Leverage the power of Spring Cloud Stream to build robust and scalable data production pipelines with ease! 🚀

