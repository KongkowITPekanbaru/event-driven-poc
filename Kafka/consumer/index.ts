import fs from "fs";
import path from "path";
import { Kafka, Consumer, Producer } from "kafkajs";

const receiveDir = path.resolve(__dirname, "files-received");

const kafka = new Kafka({
  clientId: "consumer",
  brokers: ["localhost:9092"],
  logLevel: 1,
});

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

(async () => {
  const consumer = kafka.consumer({ groupId: "file-receiver" }); // Each message from consumer group will be handled once, if there are multiple consumers, messages will be handled by the first one
  await consumer.connect();

  const retryConsumer = kafka.consumer({ groupId: "file-receiver-retry" });
  await retryConsumer.connect();

  const producer = kafka.producer();
  await producer.connect();

  process.once("SIGINT", async () => {
    console.log("Closing connection...");

    await consumer.disconnect();
    await producer.disconnect();

    console.log("Connection closed");

    process.exit(0);
  });

  await consumer.subscribe({ topic: "files" });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`[${new Date().toLocaleString()}] Received file`);

      if (!message || !message.value) return;

      const {
        fileBuffer,
        filename,
        retry = 0,
      } = JSON.parse(message.value.toString());

      if (filename.endsWith(".pdf")) {
        fs.writeFileSync(
          path.resolve(receiveDir, filename),
          Buffer.from(fileBuffer)
        );

        console.log(`File ${filename} saved`);

        return;
      }

      if (retry >= 3) {
        console.log(`File ${filename} could not be saved`);

        return;
      }

      console.log(
        `File ${filename} could not be saved, retrying [${retry}]...`
      );

      await producer.send({
        topic: `files-delay-10`,
        messages: [
          {
            value: Buffer.from(
              JSON.stringify({
                fileBuffer,
                filename,
                retry: retry + 1,
              })
            ),
          },
        ],
      });
    },
  });

  await retryConsumer.subscribe({ topic: "files-delay-10" });
  await consumer.run({
    eachBatch: async ({ batch }) => {
      const { topic, messages } = batch;

      console.log(
        `[${new Date().toLocaleString()}] Processing ${
          messages.length
        } messages from ${topic}`
      );

      await producer.send({
        topic: "files",
        messages,
      });
    },
  });

  do {
    consumer.pause([{ topic: "files-delay-10" }]);

    console.log(
      `Waiting 10 seconds before resuming consumption of files-delay-10 topic...`
    );
    await sleep(10 * 1000);

    console.log(`Resuming consumption of files-delay-10 topic...`);
    consumer.resume([{ topic: "files-delay-10" }]);
    await sleep(1 * 1000); // Need to wait for the consumer to resume before polling again
  } while (true);
})();
