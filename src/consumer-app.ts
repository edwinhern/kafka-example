import { KafkaManager } from "./client";

async function consumerStart() {
  const kafkaManager = new KafkaManager({
    brokers: ["localhost:9092"],
    clientId: "inventory-consumer-client",
  });

  try {
    const consumer = await kafkaManager.createConsumer({
      "group.id": "inventory-consumer-group",
      "allow.auto.create.topics": true,
      "topic.metadata.refresh.interval.ms": 1000,
      kafkaJS: {
        groupId: "inventory-consumer-group",
        fromBeginning: true,
      },
    });

    const topic = "test-topic";
    await consumer.subscribe({ topics: [topic] });

    console.log(`Consumer subscribed to '${topic}'. Waiting for messages...`);

    // Graceful shutdown on Ctrl+C or kill signal
    const shutdown = async () => {
      console.log("\nInitiating graceful shutdown...");
      await kafkaManager.disconnect();
      process.exit(0);
    };

    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const offset = message.offset;
        const key = message.key ? message.key.toString() : null;
        const value = message.value ? message.value.toString() : null;
        console.log(
          `[Received] Topic: ${topic} | Partition: ${partition} | Offset: ${offset} | Key: ${key} | Value: ${value}`,
        );
      },
    });
  } catch (error) {
    console.error("Consumer execution failure:", error);
    await kafkaManager.disconnect();
    process.exit(1);
  }
}

consumerStart();
