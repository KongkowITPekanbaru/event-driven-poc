import fs from "fs";
import path from "path";
import amqplib from "amqplib";

const rabbitUrl = "amqp://localhost:5672";
const watchDir = path.resolve(__dirname, "files-to-send");
const processedDir = path.resolve(__dirname, "files-sent");

let watchedFiles: string[] = [];

fs.watch(watchDir, async (event, filename) => {
  if (event !== "change") return;

  if (watchedFiles.includes(filename)) return;
  watchedFiles.push(filename);

  console.log(`New file ${filename} detected on ${watchDir}`);

  const connection = await amqplib.connect(rabbitUrl);
  const channel = await connection.createChannel();

  const exchange = "FILE_UPLOAD_EXCHANGE";
  const queue = "FILE_UPLOAD_QUEUE";

  await channel.assertExchange(exchange, "x-delayed-message", {
    durable: true,
    arguments: { "x-delayed-type": "direct" },
  });
  await channel.assertQueue(queue, { durable: true });
  await channel.bindQueue(queue, exchange, filename);

  try {
    console.log(`[${new Date().toLocaleString()}] Publishing file...`);

    const fileBuffer = fs.readFileSync(path.resolve(watchDir, filename));

    const message = {
      fileBuffer,
    };

    channel.publish(exchange, filename, Buffer.from(JSON.stringify(message)));

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

    await channel.close();
    await connection.close();

    console.log("Connection closed");
  }
});
