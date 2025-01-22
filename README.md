```shell
mvn clean generate-resources
mvn generate-sources
```

## Architecture

### Components
1. Kafka
2. Apache Flink
3. REST API

##JVM ARGS
```
--add-opens=java.base/java.util=ALL-UNNAMED 
```

### Data flow

1. Kafka producer sends messages to Kafka topic
2. Flink reads messages from Kafka topics:
    - `rccar-body`
    - `rccar-engine`
    - `rccar-remote-control`
    - `rccar-wheel`
3. Every topic is captured by a different Flink operator with keyed process function. Datastreams produces:
   - `bodyStream`
   - `engineStream`
   - `remoteControlStream`
   - `wheelsStream`
   
4. All 4 streams are connected with each other with a maximum wait of 5000ms, producing a new `RCCarStream`
   - If a message regarding the part is not received in 5000ms, a new RCCar is built and forwarded with one of the Situations: `MISSING_BODY`, `MISSING_ENGINE`, `MISSING_REMOTE_CONTROL`, `MISSING_WHEELS`
   - If a message is received, the RCCar is built with all parts assembled and forwarded with the Situation `AWAITING_PRICE`

5. All the RCCars of `RCCarStream` with situation `AWAITING_PRICE` are sent to the REST API using ASYNC/IO with approach and sent no a new stream called `pricedStream`
   - If there is Timeout or error on API async call, the situation of the RCCar is changed to `FAILED_GET_PRICE`
   - otherwise the situation is changed to `COMPLETED`
6. All `COMPLETED` from pricedStream are sent to a new Kafka topic `rccar-completed`
7. All other incomplete (failed to get price and missing some part) inputs from `rcarStream` and `pricedStream` are sent to a new Kafka topic `rccar-incomplete`

### Strategy details

1. A couple of KeyeCoProcessFunction's are used to connect respective parts streams with a maximum timeout
2. An Async/IO strategy was used to fetch the current price of the part, based on id (park sku), from the REST API with capacity of 5 requests at a time
3. The REST API has a random delay between 0-800ms to simulate the real world
4. After async enrichment, all the parts are collected again and a new RCCar is built with a RichCoFlatMapFunction. In this case there is no timeout because the parts were already captured and exists in previous streams


## Tests

### 1 - 5k messages
1. Async I/O with retry
2. Timeout 30s
3. External API random delay 0-800ms
4. 
#### Result: no messages were lost

### 2 - Simulate crash and recovery
1. Simulate 100 of each input parts on kafka source topics
2. Simulate a crash during processing, random crash after (20-70) inputs
3. Observe the recovery process


## References

1. [Asynchronous I/O for External Data Access](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/asyncio/)
2. [Process Function](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/process_function/)
3. [Connected Streams](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/etl/#connected-streams)
4. [Data Pipelines](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/learn-flink/etl/#example)
---
## Todo

1. simulate a failure and recovering from the point it crashed, reading the remaining messages of kakfa topics.