// Compile this launcher for the baseline ISA, independently of either engine.
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits.h>
#include <unistd.h>

#if defined(__linux__) && defined(__x86_64__)
#include <cpuid.h>

static bool fast_cpu() {
    unsigned a, b, c, d;
    if (__get_cpuid_max(0, nullptr) < 0xD ||
        __get_cpuid_max(0x80000000, nullptr) < 0x80000001)
        return false;

    // Cascade Lake's scalar/SIMD features, not only the explicit AVX-512
    // intrinsics: LLVM may use any feature enabled by target-cpu=cascadelake.
    __cpuid_count(1, 0, a, b, c, d);
    constexpr unsigned leaf1 =
        (1u << 0) | (1u << 1) | (1u << 9) | (1u << 12) | (1u << 13) |
        (1u << 19) | (1u << 20) | (1u << 22) | (1u << 23) | (1u << 25) |
        (1u << 26) | (1u << 27) | (1u << 28) | (1u << 29) | (1u << 30);
    // SSE3, PCLMUL, SSSE3, FMA, CX16, SSE4.1/4.2, MOVBE, POPCNT,
    // AES, XSAVE, OSXSAVE, AVX, F16C, RDRAND.
    if ((c & leaf1) != leaf1)
        return false;
    unsigned xcr0_lo, xcr0_hi;
    // XGETBV itself is safe only after checking XSAVE and OSXSAVE above.
    __asm__ volatile("xgetbv" : "=a"(xcr0_lo), "=d"(xcr0_hi) : "c"(0));
    // The OS must save XMM, YMM, opmask, and both ZMM state components.
    if ((xcr0_lo & 0xE6u) != 0xE6u)
        return false;

    __cpuid_count(7, 0, a, b, c, d);
    constexpr unsigned leaf7b =
        (1u << 0) | (1u << 3) | (1u << 5) | (1u << 8) | (1u << 10) |
        (1u << 16) | (1u << 17) | (1u << 18) | (1u << 19) |
        (1u << 23) | (1u << 24) | (1u << 28) | (1u << 30) | (1u << 31);
    // FSGSBASE, BMI1, AVX2, BMI2, INVPCID, AVX512F/DQ, RDSEED, ADX,
    // CLFLUSHOPT, CLWB, AVX512CD/BW/VL.
    constexpr unsigned leaf7c = (1u << 3) | (1u << 4) | (1u << 11);
    // PKU, OS-enabled PKU, AVX512VNNI.
    if ((b & leaf7b) != leaf7b || (c & leaf7c) != leaf7c)
        return false;
    __cpuid_count(0x80000001, 0, a, b, c, d);
    constexpr unsigned extended = (1u << 0) | (1u << 5) | (1u << 8);
    // LAHF/SAHF, LZCNT, PREFETCHW.
    if ((c & extended) != extended)
        return false;
    __cpuid_count(0xD, 1, a, b, c, d);
    constexpr unsigned xsave = (1u << 0) | (1u << 1) | (1u << 3);
    return (a & xsave) == xsave; // XSAVEOPT, XSAVEC, XSAVES.
}
#else
static bool fast_cpu() { return false; }
#endif

static bool executable_path(char *path, size_t capacity, const char *argv0) {
#if defined(__linux__)
    const ssize_t n = readlink("/proc/self/exe", path, capacity - 1);
    if (n >= 0 && static_cast<size_t>(n) < capacity - 1) {
        path[n] = '\0';
        return true;
    }
#endif
    // Local non-Linux builds use run.sh's explicit path to this launcher.
    if (std::strlen(argv0) >= capacity)
        return false;
    std::strcpy(path, argv0);
    return true;
}

int main(int, char **argv) {
    const char *mode = std::getenv("SS_ENGINE");
    if (!mode || !*mode)
        mode = "auto";
    const bool baseline = std::strcmp(mode, "baseline") == 0;
    const bool forced_fast = std::strcmp(mode, "fast") == 0;
    if (!baseline && !forced_fast && std::strcmp(mode, "auto") != 0) {
        std::fprintf(stderr, "SS_ENGINE must be auto, baseline, or fast (got '%s')\n", mode);
        return 64;
    }
    const bool supported = !baseline && fast_cpu();
    if (forced_fast && !supported) {
        std::fputs("SS_ENGINE=fast requires Linux amd64 with Cascade Lake CPU features and OS AVX-512 state support\n", stderr);
        return 126;
    }
    char path[PATH_MAX];
    if (!executable_path(path, sizeof(path), argv[0])) {
        std::fputs("cannot locate the shopping-sessions engines\n", stderr);
        return 126;
    }
    char *name = std::strrchr(path, '/');
    name = name ? name + 1 : path;
    const size_t room = sizeof(path) - static_cast<size_t>(name - path);
    if (room < sizeof("shopping-sessions-fast")) {
        std::fputs("engine path is too long\n", stderr);
        return 126;
    }
    std::strcpy(name, "shopping-sessions-fast");
    const bool fast = supported && (forced_fast || access(path, X_OK) == 0);
    if (!fast)
        std::strcpy(name, "shopping-sessions");
    const char *trace = std::getenv("SS_ENGINE_TRACE");
    if (trace && std::strcmp(trace, "1") == 0)
        std::fprintf(stderr, "[engine] %s\n", fast ? "fast" : "baseline");
    argv[0] = path;
    execv(path, argv);
    const int error = errno;
    std::fprintf(stderr, "cannot execute %s: %s\n", path, std::strerror(error));
    return error == ENOENT ? 127 : 126;
}
