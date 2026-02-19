// Meridian VCS - Platform Compatibility Helpers
//
// Provides portable wrappers for platform-specific functions.
// Include this header instead of using platform-specific APIs directly.

#pragma once

#include <ctime>

namespace meridian {

// Thread-safe gmtime wrapper.
// On POSIX systems, delegates to gmtime_r.
// On Windows, delegates to gmtime_s (which has reversed parameter order).
// Returns result on success, nullptr on failure.
inline struct tm* portable_gmtime(const time_t* timer, struct tm* result) {
#ifdef _WIN32
    return gmtime_s(result, timer) == 0 ? result : nullptr;
#else
    return gmtime_r(timer, result);
#endif
}

// Thread-safe localtime wrapper.
// On POSIX systems, delegates to localtime_r.
// On Windows, delegates to localtime_s.
// Returns result on success, nullptr on failure.
inline struct tm* portable_localtime(const time_t* timer, struct tm* result) {
#ifdef _WIN32
    return localtime_s(result, timer) == 0 ? result : nullptr;
#else
    return localtime_r(timer, result);
#endif
}

} // namespace meridian
