## python3 -m venv env
## source env/bin/activate
## pip3 install confluent-kafka

from confluent_kafka import Producer, Consumer


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
  # producer and consumer code here
  config = read_config()
  topic = "first-topic"
  
  # creates a new producer instance
  producer = Producer(config)

  # produces a sample message
  key = "key"
  value = "value"
  producer.produce(topic, key=key, value=value)
  print(f"Produced message to topic {topic}: key = {key:12} value = {value:12}")
  
  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()

main()