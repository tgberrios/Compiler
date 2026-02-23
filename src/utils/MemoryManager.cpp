#include "utils/MemoryManager.h"
#include <algorithm>
#include <fstream>
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;

MemoryManager::MemoryManager(const MemoryLimit& limit)
    : limit_(limit) {
  if (limit_.warningThreshold == 0) {
    limit_.warningThreshold = 75;  // Default 75%
  }
  if (limit_.criticalThreshold == 0) {
    limit_.criticalThreshold = 90;  // Default 90%
  }

  // Crear directorio de spill si no existe
  if (limit_.enableSpill && !limit_.spillDirectory.empty()) {
    fs::create_directories(limit_.spillDirectory);
  }

  Logger::info(LogCategory::SYSTEM, "MemoryManager",
               "Initialized with max memory: " + std::to_string(limit_.maxMemory) + 
               " bytes, spill: " + (limit_.enableSpill ? "enabled" : "disabled"));
}

MemoryManager::~MemoryManager() {
  cleanupSpillFiles();
  pools_.clear();
}

void* MemoryManager::allocate(size_t size, const std::string& context) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Verificar límites
  if (limit_.maxMemory > 0 && stats_.currentUsage + size > limit_.maxMemory) {
    if (limit_.enableSpill) {
      // Log only; allocation still attempted. Caller is responsible for spilling.
      Logger::warning(LogCategory::SYSTEM, "MemoryManager",
                      "Memory limit reached, spill to disk required");
    } else {
      Logger::error(LogCategory::SYSTEM, "MemoryManager",
                    "Memory limit exceeded: " + std::to_string(stats_.currentUsage + size) +
                    " > " + std::to_string(limit_.maxMemory));
      return nullptr;
    }
  }

  // Intentar usar memory pool si el tamaño es común
  if (size <= 4096) {  // Bloques pequeños
    auto pool = getPool(size);
    void* ptr = pool->acquire();
    if (ptr) {
      updateStats(size, 0);
      if (!context.empty()) {
        contextUsage_[context] += size;
      }
      return ptr;
    }
  }

  // Allocación normal
  void* ptr = std::malloc(size);
  if (!ptr) {
    Logger::error(LogCategory::SYSTEM, "MemoryManager",
                  "Failed to allocate " + std::to_string(size) + " bytes");
    return nullptr;
  }

  updateStats(size, 0);
  if (!context.empty()) {
    contextUsage_[context] += size;
  }

  checkLimitsAndAlert();

  return ptr;
}

void MemoryManager::deallocate(void* ptr, size_t size) {
  if (!ptr) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  // Intentar devolver a pool si es posible
  bool returnedToPool = false;
  for (auto& [blockSize, pool] : pools_) {
    if (size == blockSize) {
      pool->release(ptr);
      returnedToPool = true;
      break;
    }
  }

  if (!returnedToPool) {
    std::free(ptr);
  }

  updateStats(0, size);
}

bool MemoryManager::hasAvailableMemory(size_t requiredSize) const {
  std::lock_guard<std::mutex> lock(mutex_);

  if (limit_.maxMemory == 0) {
    return true;  // Sin límite
  }

  return (stats_.currentUsage + requiredSize) <= limit_.maxMemory;
}

size_t MemoryManager::getCurrentUsage() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stats_.currentUsage;
}

MemoryManager::MemoryStats MemoryManager::getStats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stats_;
}

void MemoryManager::setLimit(const MemoryLimit& limit) {
  std::lock_guard<std::mutex> lock(mutex_);
  limit_ = limit;

  if (limit_.enableSpill && !limit_.spillDirectory.empty()) {
    fs::create_directories(limit_.spillDirectory);
  }

  Logger::info(LogCategory::SYSTEM, "MemoryManager",
               "Memory limit updated: " + std::to_string(limit_.maxMemory) + " bytes");
}

std::string MemoryManager::spillToDisk(const void* data, size_t size, const std::string& prefix) {
  if (!limit_.enableSpill || limit_.spillDirectory.empty()) {
    Logger::error(LogCategory::SYSTEM, "MemoryManager",
                  "Spill to disk is not enabled");
    return "";
  }

  // Generar nombre de archivo único
  auto now = std::chrono::system_clock::now();
  auto timeT = std::chrono::system_clock::to_time_t(now);
  std::stringstream ss;
  ss << limit_.spillDirectory << "/" << prefix << "_" << timeT << "_" 
     << std::hex << std::hash<const void*>{}(data) << ".spill";

  std::string filePath = ss.str();

  try {
    std::ofstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
      Logger::error(LogCategory::SYSTEM, "MemoryManager",
                    "Failed to open spill file: " + filePath);
      return "";
    }

    file.write(static_cast<const char*>(data), size);
    file.close();

    {
      std::lock_guard<std::mutex> lock(mutex_);
      spillFiles_.push_back(filePath);
      stats_.spillCount++;
      stats_.spillBytes += size;
    }

    Logger::info(LogCategory::SYSTEM, "MemoryManager",
                 "Spilled " + std::to_string(size) + " bytes to: " + filePath);
    
    return filePath;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "MemoryManager",
                  "Error spilling to disk: " + std::string(e.what()));
    return "";
  }
}

std::unique_ptr<char[]> MemoryManager::loadFromDisk(const std::string& filePath, size_t& size) {
  try {
    std::ifstream file(filePath, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
      Logger::error(LogCategory::SYSTEM, "MemoryManager",
                    "Failed to open spill file: " + filePath);
      size = 0;
      return nullptr;
    }

    std::streamsize sizeVal = file.tellg();
    if (sizeVal <= 0) {
      Logger::error(LogCategory::SYSTEM, "MemoryManager",
                    "Invalid or empty spill file: " + filePath);
      size = 0;
      return nullptr;
    }
    size = static_cast<size_t>(sizeVal);
    file.seekg(0, std::ios::beg);

    auto buffer = std::make_unique<char[]>(size);
    file.read(buffer.get(), size);
    file.close();

    Logger::debug(LogCategory::SYSTEM, "MemoryManager",
                  "Loaded " + std::to_string(size) + " bytes from: " + filePath);
    
    return buffer;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "MemoryManager",
                  "Error loading from disk: " + std::string(e.what()));
    size = 0;
    return nullptr;
  }
}

void MemoryManager::cleanupSpillFiles() {
  std::lock_guard<std::mutex> lock(mutex_);

  for (const auto& filePath : spillFiles_) {
    try {
      if (fs::exists(filePath)) {
        fs::remove(filePath);
      }
    } catch (const std::exception& e) {
      Logger::warning(LogCategory::SYSTEM, "MemoryManager",
                      "Failed to remove spill file: " + filePath + " - " + e.what());
    }
  }

  spillFiles_.clear();
  Logger::info(LogCategory::SYSTEM, "MemoryManager", "Cleaned up spill files");
}

std::shared_ptr<MemoryManager::MemoryPool> MemoryManager::getPool(size_t blockSize) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = pools_.find(blockSize);
  if (it != pools_.end()) {
    return it->second;
  }

  // Crear nuevo pool
  auto pool = std::make_shared<MemoryPool>(blockSize);
  pools_[blockSize] = pool;
  return pool;
}

void MemoryManager::performGC() {
  std::lock_guard<std::mutex> lock(mutex_);

  // Limpiar pools vacíos
  auto it = pools_.begin();
  while (it != pools_.end()) {
    if (it->second->getAvailableBlocks() == 0) {
      it = pools_.erase(it);
    } else {
      ++it;
    }
  }

  // Limpiar contextos sin uso
  contextUsage_.clear();

  Logger::debug(LogCategory::SYSTEM, "MemoryManager", "Garbage collection performed");
}

void MemoryManager::checkLimitsAndAlert() {
  if (limit_.maxMemory == 0) {
    return;
  }

  size_t usagePercent = (stats_.currentUsage * 100) / limit_.maxMemory;
  size_t warningThreshold = calculateWarningThreshold();
  size_t criticalThreshold = calculateCriticalThreshold();

  if (usagePercent >= criticalThreshold) {
    triggerCritical();
  } else if (usagePercent >= warningThreshold) {
    triggerWarning();
  }
}

void MemoryManager::updateStats(size_t allocated, size_t freed) {
  if (allocated > 0) {
    stats_.currentUsage += allocated;
    stats_.totalAllocated += allocated;
    stats_.allocationCount++;

    if (stats_.currentUsage > stats_.peakUsage) {
      stats_.peakUsage = stats_.currentUsage;
    }

    if (stats_.allocationCount > 0) {
      stats_.averageAllocationSize = 
          static_cast<double>(stats_.totalAllocated) / 
          static_cast<double>(stats_.allocationCount);
    }
  }

  if (freed > 0) {
    if (stats_.currentUsage >= freed) {
      stats_.currentUsage -= freed;
    } else {
      stats_.currentUsage = 0;
    }
    stats_.totalFreed += freed;
    stats_.freeCount++;
  }
}

size_t MemoryManager::calculateWarningThreshold() const {
  return limit_.warningThreshold;
}

size_t MemoryManager::calculateCriticalThreshold() const {
  return limit_.criticalThreshold;
}

void MemoryManager::triggerWarning() {
  size_t usagePercent = (stats_.currentUsage * 100) / limit_.maxMemory;
  Logger::warning(LogCategory::SYSTEM, "MemoryManager",
                  "Memory usage warning: " + std::to_string(usagePercent) + 
                  "% (" + std::to_string(stats_.currentUsage) + "/" + 
                  std::to_string(limit_.maxMemory) + " bytes)");

  if (warningCallback_) {
    warningCallback_(stats_.currentUsage, limit_.maxMemory);
  }
}

void MemoryManager::triggerCritical() {
  size_t usagePercent = (stats_.currentUsage * 100) / limit_.maxMemory;
  Logger::error(LogCategory::SYSTEM, "MemoryManager",
                "Memory usage critical: " + std::to_string(usagePercent) + 
                "% (" + std::to_string(stats_.currentUsage) + "/" + 
                std::to_string(limit_.maxMemory) + " bytes)");

  if (criticalCallback_) {
    criticalCallback_(stats_.currentUsage, limit_.maxMemory);
  }
}

// MemoryPool implementation
MemoryManager::MemoryPool::MemoryPool(size_t blockSize, size_t initialBlocks)
    : blockSize_(blockSize) {
  for (size_t i = 0; i < initialBlocks; ++i) {
    void* block = std::malloc(blockSize_);
    if (block) {
      pool_.push_back(block);
      availableBlocks_++;
    }
  }
}

MemoryManager::MemoryPool::~MemoryPool() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (void* block : pool_) {
    std::free(block);
  }
  pool_.clear();
}

void* MemoryManager::MemoryPool::acquire() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (pool_.empty()) {
    // Crear nuevo bloque (no se añade al pool; se entrega al caller)
    void* block = std::malloc(blockSize_);
    if (block) {
      return block;
    }
    return nullptr;
  }

  void* block = pool_.back();
  pool_.pop_back();
  availableBlocks_--;
  return block;
}

void MemoryManager::MemoryPool::release(void* ptr) {
  if (!ptr) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  pool_.push_back(ptr);
  availableBlocks_++;
}
