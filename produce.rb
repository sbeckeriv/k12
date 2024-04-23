require 'kafka'

# Create a Kafka producer instance
kafka = Kafka.new(['0.0.0.0:29092'])

# Create a producer for the desired topic
producer = kafka.producer

# Send messages to the topic
producer.produce('message 1', topic: "one")
producer.produce('message 2', topic: "two")
producer.produce('message 3', topic: "three")
producer.produce('{"a":3, "b":"c", "d":["a"], "e":{"a":"b"}}', topic: "json")

# Deliver the messages
producer.deliver_messages

# Close the producer
producer.shutdown
