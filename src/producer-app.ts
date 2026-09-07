import { KafkaManager } from "./client";

async function producerStart() {
  const kafkaManager = new KafkaManager({
    brokers: ["localhost:9092"],
    clientId: "inventory-producer-client",
  });

  try {
    const producer = await kafkaManager.createProducer();

    const res = [];
    for (let i = 0; i < 50; i++) {
      res.push(
        producer.send({
          topic: "test-topic",
          messages: [{ value: `v-${i + 1}`, partition: 0, key: "x" }],
        }),
      );
    }

    await Promise.all(res);
  } catch (error) {
    console.error("Producer execution failure:", error);
  } finally {
    await kafkaManager.disconnect();
  }
}

producerStart();
