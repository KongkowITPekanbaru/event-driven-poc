import amqplib from "amqplib";

const rabbitUrl = "amqp://localhost:5672";
const quantityOfMessages = 2;

(async () => {
  console.log(`Publishing ${quantityOfMessages} messages...`);

  const connection = await amqplib.connect(rabbitUrl);
  const channel = await connection.createChannel();

  const exchange = "TASK_EXCHANGE";

  await channel.assertExchange(exchange, "fanout", { durable: true });

  for (let i = 0; i < quantityOfMessages; i++) {
    try {
      console.log(
        `[${new Date().toLocaleString()}] Publishing message ${i}...`
      );

      const message = {
        task: `Message ${i}`,
      };

      channel.publish(exchange, "", Buffer.from(JSON.stringify(message)));

      console.log(`Message ${i} published`);
    } catch (error) {
      console.error(`Error publishing message ${i}`, error);
    }
  }

  console.log("Closing connection...");

  await channel.close();
  await connection.close();

  console.log("Connection closed");
})();
