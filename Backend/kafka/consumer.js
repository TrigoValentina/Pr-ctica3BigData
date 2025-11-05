import { Kafka } from "kafkajs";
import dotenv from "dotenv";

dotenv.config();

const kafka = new Kafka({
  clientId: "emergentes-consumer",
  brokers: [process.env.KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: "grupo-gamc" });

export const startKafkaConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "sensores", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      console.log("📥 Mensaje recibido de Kafka:", data);
    },
  });
};
