import fs from "fs";
import path from "path";
import { Kafka } from "kafkajs";

const watchDir = path.resolve(__dirname, "files-to-send");
const processedDir = path.resolve(__dirname, "files-sent");

let watchedFiles: string[] = [];

const kafka = new Kafka({
  clientId: "producer",
  brokers: ["localhost:9092"],
});

fs.watch(watchDir, async (event, filename) => {
  if (event !== "change") return;

  if (watchedFiles.includes(filename)) return;
  watchedFiles.push(filename);

  console.log(`New file ${filename} detected on ${watchDir}`);

  const producer = kafka.producer();
  await producer.connect();

  try {
    console.log(`[${new Date().toLocaleString()}] Publishing file...`);

    const fileBuffer = fs.readFileSync(path.resolve(watchDir, filename));

    await producer.send({
      topic: "files",
      messages: [
        {
          value: Buffer.from(
            JSON.stringify({
              fileBuffer,
              filename,
            })
          ),
        },
      ],
    });

    console.log(`File ${filename} published`);

    fs.renameSync(
      path.resolve(watchDir, filename),
      path.resolve(processedDir, filename)
    );

    console.log("File moved to processed directory");
  } catch (error) {
    console.error("Error publishing file", error);
  } finally {
    console.log("Closing connection...");

    await producer.disconnect();

    console.log("Connection closed");
  }
});
