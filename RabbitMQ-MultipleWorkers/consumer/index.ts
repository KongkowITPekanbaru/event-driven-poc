import amqplib from "amqplib";

const rabbitUrl = "amqp://localhost:5672";

(async () => {
  const connection = await amqplib.connect(rabbitUrl);
  const channel = await connection.createChannel();

  channel.prefetch(1);

  const queue = "TASK_QUEUE";

  process.once("SIGINT", async () => {
    console.log("Closing connection...");

    await channel.close();
    await connection.close();

    console.log("Connection closed");

    process.exit(0);
  });

  await channel.assertQueue(queue, { durable: true });

  await channel.consume(
    queue,
    async (message) => {
      console.log(`[${new Date().toLocaleString()}] Received message`);

      if (!message) return;

      channel.ack(message);

      const { task } = JSON.parse(message.content.toString());

      console.log(`[${new Date().toLocaleString()}] Processing ${task}...`);
    },
    {
      noAck: false,
      consumerTag: "task-consumer",
    }
  );

  console.log("Waiting for messages...");
})();
