## DataFaker Gen RabbitMQ Sink

The RabbitMQ Sink allows for the generation and sending of data to RabbitMQ.
This sink can generate and send documents either individually or in batches in one message. It depends on `batchSize` property specified in `output.yaml` configuration.

## How to run 

### Configure RabbitMQ Sink

All configuration is in `output.yaml` file.
```bash
sinks:
  rabbitmq:
    batchsize: 1 # when 1 message contains 1 document, when > 1 message contains a batch of documents
    host: localhost
    port: 5672
    username: guest
    password: guest
    exchange: test.direct.exchange
    routingkey: products.key
````

### Run

```bash
 bin/datafaker_gen -f json -n 10000 -sink rabbitmq 
```