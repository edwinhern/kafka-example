import { KafkaJS } from "@confluentinc/kafka-javascript";

export class KafkaManager {
  private kafka: KafkaJS.Kafka;
  private producer: KafkaJS.Producer | null = null;
  private consumer: KafkaJS.Consumer | null = null;

  constructor(config: KafkaJS.KafkaConfig) {
    if (!config.brokers || config.brokers.length === 0) {
      throw new Error("Brokers list is required");
    }

    this.kafka = new KafkaJS.Kafka({ kafkaJS: config });
  }

  async createProducer(producerConfig?: KafkaJS.ProducerConstructorConfig) {
    if (this.producer) return this.producer;

    this.producer = this.kafka.producer(producerConfig);

    console.log("Connecting Kafka Producer...");
    await this.producer.connect();
    console.log("Kafka Producer connected successfully");

    return this.producer;
  }

  async createConsumer(consumerConfig: KafkaJS.ConsumerConstructorConfig) {
    const groupId = consumerConfig?.["group.id"] || consumerConfig?.kafkaJS?.groupId;
    if (!consumerConfig || !groupId) {
      throw new Error("group.id is required in consumerConfig");
    }

    if (this.consumer) return this.consumer;

    this.consumer = this.kafka.consumer(consumerConfig);
    console.log("Connecting Kafka Consumer...");
    await this.consumer.connect();
    console.log("Kafka Consumer connected successfully");

    return this.consumer;
  }

  async disconnect() {
    if (this.producer) {
      await this.producer.disconnect();
      this.producer = null;
    }

    if (this.consumer) {
      await this.consumer.disconnect();
      this.consumer = null;
    }

    console.log("All Kafka connections closed");
  }
}
