# kafka-producer
This simulate the streaming of data. To use it, upload your CSV into data directory and deploy the python app.

## Running on OpenShift
```
$ oc new-app python:3.6~https://github.com/jiajunngjj/kafka-producer \
    -e BROKER_URL=my-cluster-kafka-brokers:9092 \
    -e KAFKA_TOPIC=my-topic
```

