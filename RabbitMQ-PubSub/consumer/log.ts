import amqplib from "amqplib";

const rabbitUrl = "amqp://localhost:5672";

(async () => {
  const connection = await amqplib.connect(rabbitUrl);
  const channel = await connection.createChannel();

  channel.prefetch(1);

  const logQueue = "LOG_QUEUE";
  const exchange = "TASK_EXCHANGE";

  process.once("SIGINT", async () => {
    console.log("Closing connection...");

    await channel.close();
    await connection.close();

    console.log("Connection closed");

    process.exit(0);
  });

  await channel.assertExchange(exchange, "fanout", { durable: true, autoDelete: false })

  await channel.assertQueue(logQueue, { autoDelete: false });
  await channel.bindQueue(logQueue, exchange, "");
  await channel.consume(logQueue, async (message) => {
    console.log(`[${new Date().toLocaleString()}] Log Received message`);

    if (!message) return;

    channel.ack(message);

    const { task } = JSON.parse(message.content.toString());

    console.log(`[${new Date().toLocaleString()}] Processing LOG ${task}...`);
  });

  console.log("Waiting for messages...");
})();
