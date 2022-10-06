import amqplib from "amqplib";

const rabbitUrl = "amqp://localhost:5672";

(async () => {
  const connection = await amqplib.connect(rabbitUrl);
  const channel = await connection.createChannel();

  channel.prefetch(1);

  const taskQueue = "TASK_QUEUE";
  const delayQueue = "DELAY_QUEUE";
  const taskExchange = "TASK_EXCHANGE";
  const delayExchange = "DELAY_EXCHANGE";

  await channel.assertExchange(taskExchange, "topic", { durable: true });
  await channel.assertExchange(delayExchange, "fanout", { durable: true });

  await channel.assertQueue(taskQueue, { durable: true });
  await channel.assertQueue(delayQueue, {
    durable: true,
    deadLetterExchange: taskExchange,
    messageTtl: 10000,
  }); // Message TTL set the delay time for the message

  await channel.bindQueue(taskQueue, taskExchange, "#");

  await channel.bindQueue(delayQueue, delayExchange, "#");

  process.once("SIGINT", async () => {
    console.log("Closing connection...");

    await channel.close();
    await connection.close();

    console.log("Connection closed");

    process.exit(0);
  });

  await channel.consume(
    taskQueue,
    async (message) => {
      console.log(`[${new Date().toLocaleString()}] Received message`);

      if (!message) return;

      channel.ack(message);

      let { task, retry = 0 } = JSON.parse(message.content.toString());

      const taskDate = new Date(task);
      const passed20SecondsFromTaskDate =
        taskDate.getTime() + 20000 < new Date().getTime();

      if (!passed20SecondsFromTaskDate) {
        retry += 1;

        console.log(
          `[${new Date().toLocaleString()}] Task ${task} failed, retrying (attempt ${retry})...`
        );

        if (retry > 3) {
          console.log(
            `[${new Date().toLocaleString()}] Task ${task} failed too many times, discarding...`
          );

          return;
        }

        channel.publish(
          delayExchange,
          "",
          Buffer.from(JSON.stringify({ task, retry }))
        );
      }

      console.log(`[${new Date().toLocaleString()}] Processing ${task}...`);
    },
    {
      noAck: false,
      consumerTag: "task-consumer",
    }
  );

  console.log("Waiting for messages...");
})();
