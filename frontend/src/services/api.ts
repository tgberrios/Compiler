import axios from "axios";

const api = axios.create({
  baseURL: "/api", // Esto usará el proxy de Vite
  headers: {
    "Content-Type": "application/json",
  },
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
