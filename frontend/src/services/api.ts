import axios from "axios";

const api = axios.create({
  baseURL: "/api", // Esto usará el proxy de Vite
  headers: {
    "Content-Type": "application/json",
  },
  timeout: 60000, // 60 segundos timeout
});

export interface CatalogEntry {
  schema_name: string;
  table_name: string;
  db_engine: string;
  connection_string: string;
  active: boolean;
  status: string;
  last_sync_time: string;
  last_sync_column: string;
  last_offset: number;
  cluster_name: string;
  updated_at: string;
}

export interface DashboardStats {
  syncStatus: {
    progress: number;
    perfectMatch: number;
    listeningChanges: number;
    fullLoadActive: number;
    fullLoadInactive: number;
    noData: number;
    errors: number;
    currentProcess: string;
  };
  systemResources: {
    cpuUsage: string;
    memoryUsed: string;
    memoryTotal: string;
    memoryPercentage: string;
    rss: string;
    virtual: string;
  };
  dbHealth: {
    activeConnections: string;
    responseTime: string;
    bufferHitRate: string;
    cacheHitRate: string;
    status: string;
  };
  connectionPool: {
    totalPools: number;
    activeConnections: number;
    idleConnections: number;
    failedConnections: number;
    lastCleanup: string;
  };
}

export const dashboardApi = {
  getDashboardStats: async () => {
    try {
      const response = await api.get<DashboardStats>("/dashboard/stats");
      return response.data;
    } catch (error) {
      console.error("Error fetching dashboard stats:", error);
      if (axios.isAxiosError(error) && error.response) {
        console.error("Server error details:", error.response.data);
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const monitorApi = {
  getActiveQueries: async () => {
    try {
      const response = await api.get("/monitor/queries");
      return response.data;
    } catch (error) {
      console.error("Error fetching active queries:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const catalogApi = {
  // Obtener todas las entradas del catálogo
  getCatalogEntries: async () => {
    try {
      console.log("Fetching catalog entries...");
      const response = await api.get<CatalogEntry[]>("/catalog");
      console.log("Received catalog data:", response.data);
      return response.data;
    } catch (error) {
      console.error("Error fetching catalog:", error);
      throw error;
    }
  },

  // Actualizar el estado activo de una entrada
  updateEntryStatus: async (
    schema_name: string,
    table_name: string,
    db_engine: string,
    active: boolean
  ) => {
    try {
      const response = await api.patch<CatalogEntry>("/catalog/status", {
        schema_name,
        table_name,
        db_engine,
        active,
      });
      return response.data;
    } catch (error) {
      console.error("Error updating status:", error);
      throw error;
    }
  },

  // Forzar una sincronización completa
  triggerFullSync: async (
    schema_name: string,
    table_name: string,
    db_engine: string
  ) => {
    try {
      const response = await api.post<CatalogEntry>("/catalog/sync", {
        schema_name,
        table_name,
        db_engine,
      });
      return response.data;
    } catch (error) {
      console.error("Error triggering sync:", error);
      throw error;
    }
  },

  // Actualizar una entrada del catálogo
  updateEntry: async (entry: CatalogEntry) => {
    try {
      const response = await api.put<CatalogEntry>("/catalog", entry);
      return response.data;
    } catch (error) {
      console.error("Error updating entry:", error);
      throw error;
    }
  },
};
