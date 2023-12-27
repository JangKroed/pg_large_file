const express = require("express");
const pgp = require("pg-promise")();
// const { Pool } = require("pg");
const { LargeObjectManager } = require("pg-large-object");
const multer = require("multer");
const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");
require("dotenv").config();

const { DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_PORT } = process.env;

const config = {
  user: DB_USER,
  password: DB_PASSWORD,
  host: DB_HOST,
  database: DB_NAME,
  port: Number(DB_PORT),
};
// const client = new Client(config);
const db = pgp(config);

const upload = multer({ storage: multer.memoryStorage() });

const app = express();
app.use(express.json());

app.post("/stream", upload.single("file"), async (req, res, next) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "No file uploaded" });
    }

    const { originalname, buffer: fileBuffer, size } = req.file;
    const fileId = uuidv4();

    console.time("Upload");
    await db.tx(async (t) => {
      const largeObjectManager = new LargeObjectManager({ pgPromise: t });

      const oid = await largeObjectManager.createAsync();

      const largeObject = await largeObjectManager.openAsync(
        oid,
        LargeObjectManager.WRITE,
      );

      const bufferSize = 16384;
      let offset = 0;

      while (offset < fileBuffer.length) {
        const chunk = fileBuffer.slice(offset, offset + bufferSize);
        await largeObject.writeAsync(chunk);
        offset += bufferSize;
      }

      await largeObject.closeAsync();

      const query = {
        text: "INSERT INTO mm_notice_file (file_id, file_name, file_size, file_oid) VALUES ($1, $2, $3, $4)",
        values: [fileId, originalname, size, oid],
      };
      await db.query(query);

      res.status(200).json({ oid, fileId });
    });
    console.timeEnd("Upload");
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get("/stream", async (req, res, next) => {
  try {
    const { fileId } = req.query;

    const query = {
      text: "SELECT * FROM mm_notice_file WHERE file_id=$1",
      values: [fileId],
    };
    const [result] = await db.query(query);

    // 찾으려는 데이터가 없으면 에러
    if (!result) {
      throw new Error("empty data!");
    }

    const { file_oid: oid, file_name } = result;
    res.set("Content-disposition", "attachment; filename=" + file_name);
    res.set("Content-Type", "text/plain");

    console.time("Download");
    const test = await db.tx((tx) => {
      const man = new LargeObjectManager({ pgPromise: tx });

      const bufferSize = 16384;

      return man
        .openAndReadableStreamAsync(oid, bufferSize)
        .then(([size, stream]) => {
          const _buf = [];

          return new Promise((resolve, reject) => {
            stream.on("data", (chunk) => _buf.push(chunk));
            stream.on("end", () => resolve(Buffer.concat(_buf)));
            stream.on("error", reject);
          });
        });
    });

    console.timeEnd("Download");
    res.status(200).end(test);
  } catch (err) {
    console.error(err);
    res.status(400).json({ error: err.message });
  }
});

app.delete("/stream", async (req, res) => {
  try {
    console.time("Delete");
    const { fileId } = req.query;

    const query = {
      text: "SELECT * FROM mm_notice_file WHERE file_id=$1",
      values: [fileId],
    };
    const [result] = await db.query(query);

    // 찾으려는 데이터가 없으면 에러
    if (!result) {
      throw new Error("empty data!");
    }

    const { file_oid: oid } = result;

    const largeObjectManager = new LargeObjectManager({ pgPromise: db });

    await largeObjectManager.unlinkAsync(Number(oid));

    const deleteQuery = {
      text: "DELETE FROM mm_notice_file WHERE file_id=$1",
      values: [fileId],
    };
    await db.query(deleteQuery);

    console.timeEnd("Delete");
    res.status(200).json({ message: "Large Object deleted successfully" });
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
});

app.listen(4000, () => {
  console.log("Server Start!");
});
