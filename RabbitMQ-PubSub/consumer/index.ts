import amqplib from "amqplib";

const rabbitUrl = "amqp://localhost:5672";

(async () => {
  const connection = await amqplib.connect(rabbitUrl);
  const channel = await connection.createChannel();

  channel.prefetch(1);

  const taskQueue = "TASK_QUEUE";
  const logQueue = "LOG_QUEUE";
  const exchange = "TASK_EXCHANGE";

  process.once("SIGINT", async () => {
    console.log("Closing connection...");

    await channel.close();
    await connection.close();

    console.log("Connection closed");

    process.exit(0);
  });

  await channel.assertExchange(exchange, "fanout", { durable: false });

  await channel.assertQueue(taskQueue, { exclusive: true });
  await channel.bindQueue(taskQueue, exchange, "");
  await channel.consume(
    taskQueue,
    async (message) => {
      console.log(`[${new Date().toLocaleString()}] Task Received message`);

      if (!message) return;

      channel.ack(message);

      const { task } = JSON.parse(message.content.toString());

      console.log(
        `[${new Date().toLocaleString()}] Processing TASK ${task}...`
      );
    },
    {
      noAck: false,
      consumerTag: "task-consumer",
    }
  );

  await channel.assertQueue(logQueue, { exclusive: true });
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
