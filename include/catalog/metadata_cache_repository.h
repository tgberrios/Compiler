#ifndef METADATA_CACHE_REPOSITORY_H
#define METADATA_CACHE_REPOSITORY_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <chrono>
#include <pqxx/pqxx>

using json = nlohmann::json;

// MetadataCacheRepository: Persistencia de metadata cache en base de datos
class MetadataCacheRepository {
public:
  explicit MetadataCacheRepository(pqxx::connection& conn);
  ~MetadataCacheRepository() = default;

  // Guardar metadata cache
  void saveCacheEntry(const std::string& key,
                      const json& value,
                      const std::chrono::system_clock::time_point& expiresAt);

  // Obtener metadata cache
  std::optional<json> getCacheEntry(const std::string& key);

  // Eliminar entrada expirada
  void removeExpiredEntries();

  // Eliminar entrada específica
  void removeEntry(const std::string& key);

  // Limpiar todo el cache
  void clearCache();

  // Obtener estadísticas de cache
  json getCacheStats();

  // Inicializar tablas si no existen
  static void initializeTables(pqxx::connection& conn);

  // Hash for cache key (shared with MetadataRepository for invalidation)
  static std::string hashKey(const std::string& key);

private:
  pqxx::connection& conn_;
};

#endif // METADATA_CACHE_REPOSITORY_H
