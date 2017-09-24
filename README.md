# HERMES

*POC for Kafka stream demo*

This code give code sample of 2 use cases for real time processing using kafka streaming platform.
## Real time anomaly detection for time series ##
This test can be launched via `RealTimeAnomalyDetector` main class

* To work you'll need to configure kafka-connect to be able to received statsd data as avro 
this can be done by using the [statsd kafka-connect extension](https://github.com/jcustenborder/kafka-connect-statsd)

* So you'll start receiving statsd metrics as avro message the `MetricStarter` class provide a sample based 
on [Kamon](www.kamon.io) monitoring library to send statds metrics.

* The `AnomalyDetectorPOrocessor` will start to get records from kafka and store them in a local store
at specified interval it will check the number of item in the store and start the anomaly detection 
on the time series

* The anomaly detection is done by using the yahoo [EGADS](https://github.com/yahoo/egads) library

* The anomalies detected are resent to kafka topic

## Real join between streams ##
This test can be started by `StreamJoinProcess` 
* you will need to prepare all the required topics to be able to run this
* Be sure to partition your topics the same way to prevent missing match

```$xslt
./kafka-topics --zookeeper 127.0.0.1:2181 --create --topic join1 --replication-factor 1 --partitions 4
./kafka-topics --zookeeper 127.0.0.1:2181 --create --topic join2 --replication-factor 1 --partitions 4
./kafka-topics --zookeeper 127.0.0.1:2181 --create --topic AggEvents --replication-factor 1 --partitions 4
./kafka-topics --zookeeper 127.0.0.1:2181 --create --topic joinstream2 --replication-factor 1 --partitions 4
./kafka-topics --zookeeper 127.0.0.1:2181 --create --topic unifiedEvent --replication-factor 1 --partitions 4
```

* Be sure to have the kafka schema-registry up and running as we only use Avro for message format.
* `ScalaAvroProducer` is a provider generating the event for you
* All the events are resent to kafka topic
  

 
    

