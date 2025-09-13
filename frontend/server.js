import express from "express";
import pkg from "pg";
const { Pool } = pkg;
import cors from "cors";
import { spawn } from "child_process";

const app = express();
app.use(cors());
app.use(express.json());

const pool = new Pool({
  host: "localhost",
  port: 5432,
  database: "DataLake",
  user: "tomy.berrios",
  password: "Yucaquemada1",
});

// Test connection
pool.connect((err, client, done) => {
  if (err) {
    console.error("Error connecting to the database:", err);
  } else {
    console.log("Successfully connected to PostgreSQL");
    done();
  }
});

// Obtener catálogo
app.get("/api/catalog", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM metadata.catalog");
    console.log("Catalog data retrieved:", result.rows);
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
// Obtener estadísticas del dashboard
app.get("/api/dashboard/stats", async (req, res) => {
  try {
    console.log("Fetching dashboard stats...");

    // 1. SYNCHRONIZATION STATUS
    const syncStatus = await pool.query(`
      SELECT 
        COUNT(*) FILTER (WHERE status = 'PERFECT_MATCH') as perfect_match,
        COUNT(*) FILTER (WHERE status = 'LISTENING_CHANGES') as listening_changes,
        COUNT(*) FILTER (WHERE status = 'FULL_LOAD' AND active = true) as full_load_active,
        COUNT(*) FILTER (WHERE status = 'FULL_LOAD' AND active = false) as full_load_inactive,
        COUNT(*) FILTER (WHERE status = 'NO_DATA') as no_data,
        COUNT(*) FILTER (WHERE status = 'ERROR') as errors,
        STRING_AGG(CASE WHEN status = 'PROCESSING' 
          THEN schema_name || '.' || table_name || ' (' || status || ')'
          ELSE NULL END, ', ') as current_process
      FROM metadata.catalog
    `);

    // 2. TRANSFER PERFORMANCE BY ENGINE
    const transferPerformance = await pool.query(`
      SELECT 
        db_engine,
        COUNT(*) FILTER (WHERE status = 'PROCESSING' AND completed_at IS NULL) as active_transfers,
        ROUND(AVG(transfer_rate_per_second)::numeric, 2) as avg_transfer_rate,
        ROUND(AVG(memory_used_mb)::numeric, 2) as avg_memory_used,
        ROUND(AVG(cpu_usage_percent)::numeric, 2) as avg_cpu_usage,
        ROUND(AVG(io_operations_per_second)::numeric, 2) as avg_iops,
        ROUND(AVG(avg_latency_ms)::numeric, 2) as avg_latency,
        SUM(bytes_transferred) as total_bytes
      FROM metadata.transfer_metrics
      WHERE created_at > NOW() - INTERVAL '5 minutes'
      GROUP BY db_engine
    `);

    // 3. SYSTEM RESOURCES (from transfer_metrics)
    const systemResources = await pool.query(`
      SELECT 
        ROUND(AVG(cpu_usage_percent)::numeric, 2) as cpu_usage,
        ROUND(AVG(memory_used_mb)::numeric, 2) as memory_used,
        MAX(memory_used_mb) as memory_peak,
        ROUND(AVG(io_operations_per_second)::numeric, 2) as io_ops
      FROM metadata.transfer_metrics
      WHERE created_at > NOW() - INTERVAL '1 minute'
    `);

    // 4. DATABASE HEALTH
    const dbHealth = await pool.query(`
      SELECT 
        (SELECT count(*) FROM pg_stat_activity) as active_connections,
        (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections,
        EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time())) as uptime_seconds,
        (
          SELECT json_build_object(
            'buffer_hit_ratio', ROUND(COALESCE((sum(heap_blks_hit) * 100.0 / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0)), 100)::numeric, 1),
            'cache_hit_ratio', ROUND(COALESCE((sum(idx_blks_hit) * 100.0 / NULLIF(sum(idx_blks_hit) + sum(idx_blks_read), 0)), 100)::numeric, 1)
          )
          FROM pg_statio_user_tables
        ) as cache_stats
    `);

    // 5. CONNECTION POOLING
    const connectionPool = await pool.query(`
      SELECT 
        COUNT(DISTINCT db_engine) as total_pools,
        COUNT(*) FILTER (WHERE status = 'PROCESSING') as active_connections,
        COUNT(*) FILTER (WHERE status != 'PROCESSING' AND active = true) as idle_connections,
        COUNT(*) FILTER (WHERE status = 'ERROR') as failed_connections,
        MAX(updated_at) as last_update
      FROM metadata.catalog
    `);

    // 6. RECENT ACTIVITY
    const recentActivity = await pool.query(`
      SELECT 
        COUNT(*) as transfers_last_hour,
        COUNT(*) FILTER (WHERE status = 'FAILED') as errors_last_hour,
        MIN(created_at) as first_transfer,
        MAX(created_at) as last_transfer,
        SUM(records_transferred) as total_records,
        SUM(bytes_transferred) as total_bytes
      FROM metadata.transfer_metrics
      WHERE created_at > NOW() - INTERVAL '1 hour'
    `);

    // Construir el objeto de respuesta
    const stats = {
      syncStatus: {
        progress: 0,
        perfectMatch: parseInt(syncStatus.rows[0]?.perfect_match || 0),
        listeningChanges: parseInt(syncStatus.rows[0]?.listening_changes || 0),
        fullLoadActive: parseInt(syncStatus.rows[0]?.full_load_active || 0),
        fullLoadInactive: parseInt(syncStatus.rows[0]?.full_load_inactive || 0),
        noData: parseInt(syncStatus.rows[0]?.no_data || 0),
        errors: parseInt(syncStatus.rows[0]?.errors || 0),
        currentProcess: syncStatus.rows[0]?.current_process || "",
      },
      systemResources: {
        cpuUsage: (systemResources.rows[0]?.cpu_usage || 0).toString(),
        memoryUsed: (systemResources.rows[0]?.memory_used || 0).toString(),
        memoryTotal: (systemResources.rows[0]?.memory_peak || 0).toString(),
        memoryPercentage:
          systemResources.rows[0]?.memory_used &&
          systemResources.rows[0]?.memory_peak
            ? (
                (systemResources.rows[0].memory_used /
                  systemResources.rows[0].memory_peak) *
                100
              ).toFixed(1)
            : "0",
        rss: (systemResources.rows[0]?.memory_used || 0).toString(),
        virtual: (systemResources.rows[0]?.memory_peak || 0).toString(),
      },
      dbHealth: {
        activeConnections: dbHealth.rows[0]
          ? dbHealth.rows[0].active_connections +
            "/" +
            dbHealth.rows[0].max_connections
          : "0/0",
        responseTime: "< 1ms",
        bufferHitRate: (
          dbHealth.rows[0]?.cache_stats?.buffer_hit_ratio || 0
        ).toFixed(1),
        cacheHitRate: (
          dbHealth.rows[0]?.cache_stats?.cache_hit_ratio || 0
        ).toFixed(1),
        status: dbHealth.rows[0] ? "Healthy" : "Unknown",
      },
      connectionPool: {
        totalPools: parseInt(connectionPool.rows[0]?.total_pools || 0),
        activeConnections: parseInt(
          connectionPool.rows[0]?.active_connections || 0
        ),
        idleConnections: parseInt(
          connectionPool.rows[0]?.idle_connections || 0
        ),
        failedConnections: parseInt(
          connectionPool.rows[0]?.failed_connections || 0
        ),
        lastCleanup: connectionPool.rows[0]?.last_update
          ? formatUptime(
              (Date.now() -
                new Date(connectionPool.rows[0].last_update).getTime()) /
                1000
            )
          : "0m",
      },
    };

    // Calcular progreso total
    const total =
      stats.syncStatus.perfectMatch +
      stats.syncStatus.listeningChanges +
      stats.syncStatus.fullLoadActive +
      stats.syncStatus.fullLoadInactive +
      stats.syncStatus.noData;

    stats.syncStatus.progress =
      total > 0
        ? Math.round(
            ((stats.syncStatus.perfectMatch +
              stats.syncStatus.listeningChanges) /
              total) *
              100
          )
        : 0;

    // Agregar métricas por motor
    stats.engineMetrics = {};
    transferPerformance.rows.forEach((metric) => {
      stats.engineMetrics[metric.db_engine] = {
        recordsPerSecond: parseFloat(metric.avg_transfer_rate),
        bytesTransferred: parseFloat(metric.total_bytes),
        avgLatencyMs: parseFloat(metric.avg_latency),
        cpuUsage: parseFloat(metric.avg_cpu_usage),
        memoryUsed: parseFloat(metric.avg_memory_used),
        iops: parseFloat(metric.avg_iops),
        activeTransfers: parseInt(metric.active_transfers),
      };
    });

    // Agregar actividad reciente
    stats.recentActivity = {
      transfersLastHour: parseInt(
        recentActivity.rows[0]?.transfers_last_hour || 0
      ),
      errorsLastHour: parseInt(recentActivity.rows[0]?.errors_last_hour || 0),
      totalRecords: parseInt(recentActivity.rows[0]?.total_records || 0),
      totalBytes: parseInt(recentActivity.rows[0]?.total_bytes || 0),
      firstTransfer: recentActivity.rows[0]?.first_transfer || null,
      lastTransfer: recentActivity.rows[0]?.last_transfer || null,
      uptime: formatUptime(dbHealth.rows[0]?.uptime_seconds || 0),
    };

    console.log("Sending dashboard stats");
    res.json(stats);
  } catch (err) {
    console.error("Error getting dashboard stats:", err);
    res.status(500).json({
      error: "Error al obtener estadísticas",
      details: err.message,
    });
  }
});

// Función para formatear el tiempo de uptime
function formatUptime(seconds) {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);

  if (days > 0) {
    return `${days}d ${hours.toString().padStart(2, "0")}h ${minutes
      .toString()
      .padStart(2, "0")}m`;
  } else if (hours > 0) {
    return `${hours.toString().padStart(2, "0")}h ${minutes
      .toString()
      .padStart(2, "0")}m`;
  } else {
    return `${minutes.toString().padStart(2, "0")}m`;
  }
}
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
