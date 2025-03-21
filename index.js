import express from 'express';
import multer from 'multer';
import dotenv from 'dotenv';
import bodyParser from 'body-parser';
import fs from 'fs';
import fastCsv from 'fast-csv';
import pkg from 'pg';

const { Pool } = pkg;

try {
    dotenv.config();
} catch (error) {
    console.error('Error loading environment variables:', error);
}

const pool = new Pool({
    user: process.env.DBUSER,
    host: process.env.DBHOST,
    database: process.env.DBNAME,
    password: process.env.DBPASSWORD,
    port: process.env.DBPORT || 5432,
  });
  
const app = express();

const PORT = process.env.PORT || 3000;

app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.json());

app.get('/', (req, res) => {
    res.send('Hello, World!');
});

const upload = multer({ dest: "uploads/" });

// Function to create a table dynamically based on CSV headers
const createTableFromCsv = async (filePath, tableName) => {
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(filePath);
        let headers = [];

        const csvStream = fastCsv
            .parse({ headers: true })
            .on("headers", async (cols) => {
                headers = cols.map(col => col.replace(/\s+/g, "_").toLowerCase()); // Normalize column names
                csvStream.pause(); // Pause stream while we create the table

                const client = await pool.connect();
                try {
                    // Drop table if exists (optional, for testing purposes)
                    await client.query(`DROP TABLE IF EXISTS ${tableName};`);

                    // Create table dynamically
                    const columns = headers.map(header => `${header} TEXT`).join(", ");
                    const createTableQuery = `CREATE TABLE ${tableName} (id SERIAL PRIMARY KEY, ${columns});`;
                    await client.query(createTableQuery);

                    console.log(`✅ Table '${tableName}' created with columns: ${headers.join(", ")}`);
                    resolve(headers);
                } catch (error) {
                    reject(error);
                } finally {
                    client.release();
                    csvStream.resume(); // Resume processing
                }
            })
            .on("error", (error) => reject(error))
            .on("end", () => console.log("CSV Headers Processed"));

        stream.pipe(csvStream);
    });
};

// Function to import CSV data into the dynamically created table
const importCsvToDb = async (filePath, tableName, headers) => {
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(filePath);
        const csvData = [];

        const csvStream = fastCsv
            .parse({ headers: true })
            .on("data", (row) => {
                csvData.push(Object.values(row)); // Extract values only
            })
            .on("end", async () => {
                if (csvData.length === 0) {
                    console.log("❌ No data found in CSV");
                    return reject(new Error("No data found in CSV"));
                }

                const client = await pool.connect();
                try {
                    await client.query("BEGIN");

                    // Construct dynamic insert query
                    const columns = headers.join(", ");
                    const placeholders = headers.map((_, i) => `$${i + 1}`).join(", ");
                    const insertQuery = `INSERT INTO ${tableName} (${columns}) VALUES (${placeholders});`;

                    for (const row of csvData) {
                        await client.query(insertQuery, row);
                    }

                    await client.query("COMMIT");
                    console.log(`✅ CSV Data Imported into '${tableName}' Successfully`);
                    resolve();
                } catch (err) {
                    await client.query("ROLLBACK");
                    console.error("❌ Error inserting data:", err);
                    reject(err);
                } finally {
                    client.release();
                    fs.unlinkSync(filePath); // Remove file after processing
                }
            })
            .on("error", (error) => reject(error));

        stream.pipe(csvStream);
    });
};

// File upload API
app.post("/upload", upload.single("file"), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ message: "No file uploaded" });
    }
    console.log(req.file)
  
    const {tableName} = req.body; // Change this as needed
    
    try {
        const headers = await createTableFromCsv(req.file.path, tableName);
        await importCsvToDb(req.file.path, tableName, headers);
        res.json({ message: "CSV imported successfully" });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});
try {
    app.listen(PORT, () => {
        console.log(`Server is running on http://localhost:${PORT}`);
    });
} catch (error) {
    console.error('Error starting the server:', error);
}
