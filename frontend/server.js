import express from 'express';
import pkg from 'pg';
const { Pool } = pkg;
import cors from 'cors';

const app = express();
app.use(cors());
app.use(express.json());

const pool = new Pool({
  host: "localhost",
  port: 5432,
  database: "DataLake",
  user: "tomy.berrios",
  password: "Yucaquemada1"
});

// Test connection
pool.connect((err, client, done) => {
  if (err) {
    console.error('Error connecting to the database:', err);
  } else {
    console.log('Successfully connected to PostgreSQL');
    done();
  }
});

// Obtener catálogo
app.get("/api/catalog", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM metadata.catalog");
    console.log('Catalog data retrieved:', result.rows);
    res.json(result.rows);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Actualizar estado
app.patch("/api/catalog/status", async (req, res) => {
  const { schema_name, table_name, db_engine, active } = req.body;
  try {
    const result = await pool.query(
      `UPDATE metadata.catalog 
       SET active = $1, updated_at = NOW()
       WHERE schema_name = $2 AND table_name = $3 AND db_engine = $4
       RETURNING *`,
      [active, schema_name, table_name, db_engine]
    );
    res.json(result.rows[0]);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Forzar sincronización
app.post("/api/catalog/sync", async (req, res) => {
  const { schema_name, table_name, db_engine } = req.body;
  try {
    const result = await pool.query(
      `UPDATE metadata.catalog 
       SET status = 'full_load', updated_at = NOW()
       WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3
       RETURNING *`,
      [schema_name, table_name, db_engine]
    );
    res.json(result.rows[0]);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: err.message });
  }
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});