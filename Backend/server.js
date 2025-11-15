import express from "express";
import dotenv from "dotenv";
import { connectMongo } from "./db/mongo.js";
import { sendKafkaMessage } from "./kafka/producer.js";
import { startKafkaConsumer } from "./kafka/consumer.js";

dotenv.config();
const app = express();
app.use(express.json());

// 🔌 Conexiones iniciales
connectMongo();
startKafkaConsumer();

// 📡 Endpoint de prueba para enviar mensajes a Kafka
app.post("/api/sensores", async (req, res) => {
  const data = req.body;
  await sendKafkaMessage("sensores", data);
  res.json({ message: "Dato enviado a Kafka", data });
});

app.listen(process.env.PORT, () => {
  console.log(`🚀 Servidor backend corriendo en puerto ${process.env.PORT}`);
});
