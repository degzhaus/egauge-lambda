import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { Readable } from "stream";
import { Buffer } from "buffer";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";

const BUCKET_NAME = "degzhaus-egauge-data";

export function generateDayIntervals(date: Date): { f: number; n: number; s: number }[] {
  const s = 299;
  const n = 13; // 13 intervals = 1 hour @ 5-min intervals
  const intervals: { f: number; n: number; s: number }[] = [];

  const midnight = new Date(date);
  midnight.setHours(0, 0, 0, 0);

  for (let hour = 0; hour < 24; hour++) {
    const f = Math.floor(midnight.getTime() / 1000) + hour * 3600;
    intervals.push({ f, n, s });
  }

  return intervals;
}

export async function spawnLambda(
  lambdaName: string,
  payload: any,
  maxRetries = 3,
  retryDelayMs = 1000
): Promise<any[]> {
  const lambda = new LambdaClient({});

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const command = new InvokeCommand({
        FunctionName: lambdaName,
        Payload: Buffer.from(JSON.stringify(payload)),
      });

      const response = await lambda.send(command);
      const payloadBuffer = response.Payload;

      if (!payloadBuffer) throw new Error("Empty response from Lambda");

      const parsed = JSON.parse(Buffer.from(payloadBuffer).toString());

      return parsed;
    } catch (err) {
      console.warn(
        `Attempt ${attempt} for ${lambdaName} with f=${payload.f} failed:`,
        err
      );

      if (attempt === maxRetries) {
        throw new Error(
          `All ${maxRetries} retries failed for interval starting at ${payload.f}`
        );
      }

      await delay(retryDelayMs);
    }
  }

  throw new Error("Unreachable"); // for type-safety
}

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function appendCsvToMonthlyFile(rows: any[], date: Date) {
  const s3 = new S3Client({});
  const monthKey = `${BUCKET_NAME}/${date.getFullYear()}-${String(
    date.getMonth() + 1
  ).padStart(2, "0")}.csv`;

  let existingCsv = "";
  const exists = await s3
    .send(new HeadObjectCommand({ Bucket: BUCKET_NAME, Key: monthKey }))
    .then(() => true)
    .catch((err) => (err.$metadata?.httpStatusCode === 404 ? false : Promise.reject(err)));

    if (exists) {
    const getRes = await s3.send(
      new GetObjectCommand({ Bucket: BUCKET_NAME, Key: monthKey })
    );
    const stream = getRes.Body as Readable;
    existingCsv = await streamToString(stream);
  }
  
  const header = Object.keys(rows[0]);
  const newRows = rows.map((r) => header.map((h) => r[h]).join(",")).join("\n");

  const combinedCsv = exists
    ? existingCsv.trimEnd() + "\n" + newRows
    : header.join(",") + "\n" + newRows;

    await s3.send(
    new PutObjectCommand({
      Bucket: BUCKET_NAME,
      Key: monthKey,
      Body: combinedCsv,
      ContentType: "text/csv",
    })
  );

  console.log(`Updated monthly CSV: s3://${BUCKET_NAME}/${monthKey}`);
}

function streamToString(stream: Readable): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    stream.on("error", reject);
  });
}
