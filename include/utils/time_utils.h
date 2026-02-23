#ifndef TIME_UTILS_H
#define TIME_UTILS_H

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>
#ifdef _WIN32
#include <errno.h>
#endif

namespace TimeUtils {

/** Returns current local time as "YYYY-MM-DD HH:MM:SS.mmm". Returns "" on localtime failure; callers should check for empty. */
inline std::string getCurrentTimestamp() {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) %
            1000;

  std::stringstream ss;
  struct tm tm_buf;
#ifdef _WIN32
  errno_t err = localtime_s(&tm_buf, &time_t);
  if (err != 0) {
    return "";
  }
  ss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
#else
  std::tm *tm_ptr = localtime_r(&time_t, &tm_buf);
  if (!tm_ptr) {
    return "";
  }
  ss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
#endif
  ss << "." << std::setfill('0') << std::setw(3) << ms.count();
  return ss.str();
}

} // namespace TimeUtils

#endif
