import fs from "fs";
import path from "path";
import amqplib from "amqplib";

const rabbitUrl = "amqp://localhost:5672";
const receiveDir = path.resolve(__dirname, "files-received");

(async () => {
  const connection = await amqplib.connect(rabbitUrl);
  const channel = await connection.createChannel();

  channel.prefetch(10); // Get 10 messages at once

  const queue = "FILE_UPLOAD_QUEUE";
  const exchange = "FILE_UPLOAD_EXCHANGE";

  process.once("SIGINT", async () => {
    console.log("Closing connection...");

    await channel.close();
    await connection.close();

    console.log("Connection closed");

    process.exit(0);
  });

  await channel.assertQueue(queue, { durable: true });
  await channel.assertExchange(exchange, "x-delayed-message", {
    durable: true,
    arguments: { "x-delayed-type": "direct" },
  });

  await channel.consume(
    queue,
    async (message) => {
      console.log(`[${new Date().toLocaleString()}] Received file`);

      if (!message) return;

      channel.ack(message);

      const { fileBuffer, attempt = 1 } = JSON.parse(
        message.content.toString()
      );
      const filename = message.fields.routingKey;

      if (filename.endsWith(".pdf")) {
        fs.writeFileSync(
          path.resolve(receiveDir, filename),
          Buffer.from(fileBuffer)
        );

        console.log(`File ${filename} saved`);

        return;
      }

      if (attempt > 3) {
        console.log(`File ${filename} failed to save`);

        return;
      }

      const newAttempt = attempt + 1;
      console.log(
        `File ${filename} failed to save, retrying (attempt ${newAttempt})...`
      );

      const newMessage = {
        fileBuffer,
        attempt: newAttempt,
      };

      channel.publish(
        "FILE_UPLOAD_EXCHANGE",
        filename,
        Buffer.from(JSON.stringify(newMessage)),
        { headers: { "x-delay": 10000 } }
      );
    },
    {
      noAck: false,
      consumerTag: "file-upload-consumer",
    }
  );

  console.log("Waiting for files...");
})();
