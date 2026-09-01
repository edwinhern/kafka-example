import { KafkaJS } from "@confluentinc/kafka-javascript";

export class KafkaManager {
  private kafka: KafkaJS.Kafka;

  private producer: KafkaJS.Producer | null;
  private consumer: KafkaJS.Consumer | null;

  constructor(config: KafkaJS.KafkaConfig) {
    if (!config.brokers || config.brokers.length === 0) {
      throw new Error("Brokers list is required");
    }

    this.kafka = new KafkaJS.Kafka({ kafkaJS: config });
    this.producer = null;
    this.consumer = null;
  }

  async createProducer(producerConfig: KafkaJS.ProducerConstructorConfig) {
    if (this.producer) return this.producer;

    this.producer = this.kafka.producer(producerConfig);

    console.log("Connecting Kafka Producer");
    await this.producer.connect();

    return this.producer;
  }

  async createConsumer(consumerConfig: KafkaJS.ConsumerConstructorConfig) {
    if (!consumerConfig || !consumerConfig["group.id"]) {
      throw new Error("GroupId is required");
    }

    if (this.consumer) return this.consumer;

    this.consumer = this.kafka.consumer(consumerConfig);

    console.log("Connecting Kafka Consumer");
    await this.consumer.connect();
    console.log("Kafka Consumer connected sucessfully");

    return this.consumer;
  }
}
