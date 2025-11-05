import { Kafka, Partitioners } from "kafkajs";
import dotenv from "dotenv";

dotenv.config();

const kafka = new Kafka({
  clientId: "emergentes-producer",
  brokers: [process.env.KAFKA_BROKER],
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

export const sendKafkaMessage = async (topic, message) => {
  await producer.connect();
  await producer.send({
    topic,
    messages: [{ value: JSON.stringify(message) }],
  });
  console.log(`📤 Mensaje enviado a Kafka topic "${topic}":`, message);
  await producer.disconnect();
};
