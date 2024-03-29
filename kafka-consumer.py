from confluent_kafka import Producer, Consumer
from confluent_kafka import TopicPartition

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def main():
    # sets the consumer group ID and offset  
    config = read_config()
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"
    #config["default.offset"] = 0

    topic = "first-topic"

    # creates a new consumer and subscribes to your topic
    consumer = Consumer(config)
    consumer.subscribe([topic])
    consumer.assign([TopicPartition(topic, 1, 5)]) #if we want to specify the partition to consume from, and the offset value
    try:
        while True:
        # consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0) #1.0 is the timeout
            if msg is not None and msg.error() is None:
                key = msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")
                print(f"Consumed message from topic {topic}: key = {key:12} value = {value:12}")

                # PARTITIONS = []
                # for partition in consumer (topic):
                #     PARTITIONS.append(TopicPartition(topic, partition))

                # end_offsets = consumer.end_offsets(PARTITIONS)
                # print(end_offsets)
            # else:
            #     print("No message found")
    except KeyboardInterrupt:
        pass
    finally:
        # Retrieve the latest committed offsets for the consumer group
        committed_offsets = consumer.list_consumer_group_offsets('my_consumer_group')

        # Print the latest committed offsets
        for topic_partition, offset in committed_offsets.items():
            print(f"Partition {topic_partition.topic}-{topic_partition.partition}: Committed Offset {offset.offset}")
        # closes the consumer connection
        consumer.close()

main()