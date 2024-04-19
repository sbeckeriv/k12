# k12 

A Kafka tool for tailing, reading and list topics.

## list
```
k12 list -b 'localhost:9092' -vvvvvvvvvvvvvvvvvvvvvv`
rd_kafka_version: 0x020300ff, 2.3.0
Cluster information:
  Broker count: 1
  Topics count: 4
  Metadata broker name: localhost:29092/1
  Metadata broker id: 1


Topics:
  two
    Partition: 0  Leader: 1  Replicas: [1]  ISR: [1]  Err: None
```

## read
### offset
```
k12 read -b 'localhost:9092' --topic one --offset 4`
{"message":"message 1","timestamp":1713505424670,"topic":"one"}
{"message":"message 1","timestamp":1713506974124,"topic":"one"}
```

### times offset
using https://docs.rs/chrono-english/latest/chrono_english/
```
k12 read -b 'localhost:9092' --topic one --offset 4`
{"message":"message 1","timestamp":1713505424670,"topic":"one"}
{"message":"message 1","timestamp":1713506974124,"topic":"one"}
```

### timestamps
```
k12 read -b 'localhost:9092' --topic one --start '2024-04-19T05:30:00Z' --end '2024-04-19T05:55:00Z'`
{"message":"message 1","timestamp":1713505096129,"topic":"one"}
{"message":"message 1","timestamp":1713505420424,"topic":"one"}
```

## tail

```
k12 tail -b 'localhost:9092' --topic one`
{"message":"message 1","timestamp":1713508350144,"topic":"one"}
```

## run kafka via podman

port 29092

```
podman-compose up
```

## Test data
For now I use ruby

```
gem install kafka

ruby produce.rb	
```
