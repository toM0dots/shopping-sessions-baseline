// Native Shopping Sessions pipeline. Input-backed string views remain alive
// until the Parquet writers finish; no reference answers or cached results.
#include <algorithm>
#include <array>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#if defined(__SSE2__)
#include <emmintrin.h>
#endif
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <parquet/arrow/writer.h>
#include <stdexcept>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using SV = std::string_view;
using I64 = int64_t;
constexpr I64 DAY = 86400000, REACH = 7 * DAY;
constexpr I64 BAD = std::numeric_limits<I64>::min();
constexpr size_t SHARDS = 64;

// wyhash-style string hash: one 64x64->128-bit multiply per 16 bytes, with
// overlapping loads for short keys. Values only choose shards and table slots;
// every table still compares full keys, so outputs never depend on them.
static inline uint64_t mum(uint64_t a, uint64_t b) {
    __uint128_t r = static_cast<__uint128_t>(a) * b;
    return uint64_t(r) ^ uint64_t(r >> 64);
}
static inline uint64_t load8(const char *p) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    return v;
}
static inline uint64_t load4(const char *p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return v;
}
struct Hash {
    size_t operator()(SV s) const {
        constexpr uint64_t k0 = 0xa0761d6478bd642fULL, k1 = 0xe7037ed1a0b428dbULL,
                           k2 = 0x8ebc6af09c88c6e3ULL;
        const char *p = s.data();
        size_t n = s.size();
        uint64_t seed = k0 ^ n, a, b;
        if (n <= 16) {
            if (n >= 8) {
                a = load8(p);
                b = load8(p + n - 8);
            } else if (n >= 4) {
                a = load4(p);
                b = load4(p + n - 4);
            } else if (n) {
                a = (uint64_t(uint8_t(p[0])) << 16) | (uint64_t(uint8_t(p[n >> 1])) << 8) |
                    uint8_t(p[n - 1]);
                b = 0;
            } else
                a = b = 0;
        } else {
            size_t left = n;
            while (left > 16) {
                seed = mum(load8(p) ^ k1, load8(p + 8) ^ seed);
                p += 16;
                left -= 16;
            }
            a = load8(p + left - 16);
            b = load8(p + left - 8);
        }
        return mum(k1 ^ n, mum(a ^ k1, b ^ seed) ^ k2);
    }
};
template <class T> using Map = std::unordered_map<SV, T, Hash>;

// Process-lifetime copies of the IDs that Key cannot pack, length-prefixed.
// Each thread fills its own 1 MiB chunk, so copying needs no lock except to
// register a new chunk. Generated data almost never needs one.
class FallbackIds {
    std::mutex lock;
    std::vector<std::unique_ptr<char[]>> chunks;
    static thread_local char *cursor;
    static thread_local size_t left;

  public:
    const char *copy(SV s) {
        size_t need = (4 + s.size() + 7) & ~size_t(7);
        if (need > left) {
            size_t size = std::max<size_t>(size_t(1) << 20, need);
            std::unique_ptr<char[]> chunk(new char[size]);
            cursor = chunk.get();
            left = size;
            std::lock_guard<std::mutex> guard(lock);
            chunks.push_back(std::move(chunk));
        }
        char *out = cursor;
        uint32_t n = uint32_t(s.size());
        std::memcpy(out, &n, 4);
        std::memcpy(out + 4, s.data(), s.size());
        cursor += need;
        left -= need;
        return out;
    }
    static SV text(const char *copy) {
        uint32_t n;
        std::memcpy(&n, copy, 4);
        return SV(copy + 4, n);
    }
};
thread_local char *FallbackIds::cursor = nullptr;
thread_local size_t FallbackIds::left = 0;
static FallbackIds fallback_ids;

// An ID in 8 bytes. Every generated ID is one byte followed by 1 to 8
// characters of [0-9a-z] (u..., e..., k...); such an ID is packed exactly:
// bit 63 set, the first byte in bits 45-52, the digit count minus one in bits
// 42-44 and the base-36 value in bits 0-41. Equality is one integer compare.
// Any other ID is a pointer to a length-prefixed copy (bit 63 clear). A string
// either always packs or never does, so a packed key never equals a copied one.
// Zero is the empty key: no stored ID is empty.
struct Key {
    uint64_t v = 0;
    static constexpr uint64_t PACKED = uint64_t(1) << 63, VALUE = (uint64_t(1) << 42) - 1;
    static const std::array<uint8_t, 256> &digits() {
        static const std::array<uint8_t, 256> table = [] {
            std::array<uint8_t, 256> t{};
            t.fill(0xFF);
            for (int c = 0; c < 10; ++c)
                t['0' + c] = uint8_t(c);
            for (int c = 0; c < 26; ++c)
                t['a' + c] = uint8_t(10 + c);
            return t;
        }();
        return table;
    }
    static Key of(SV s) {
        if (s.size() >= 2 && s.size() <= 9) {
            auto &d = digits();
            uint64_t value = 0;
            bool packable = true;
            for (size_t i = 1; i < s.size(); ++i) {
                uint8_t digit = d[uint8_t(s[i])];
                packable &= digit != 0xFF;
                value = value * 36 + (digit & 63);
            }
            if (packable)
                return Key{PACKED | uint64_t(uint8_t(s[0])) << 45 | uint64_t(s.size() - 2) << 42 | value};
        }
        return Key{reinterpret_cast<uintptr_t>(fallback_ids.copy(s))};
    }
    bool empty() const { return !v; }
    bool packed() const { return v & PACKED; }
    // The ID's bytes: written into buf when packed, else the stored copy.
    SV text(char (&buf)[16]) const {
        if (!packed())
            return v ? FallbackIds::text(reinterpret_cast<const char *>(uintptr_t(v))) : SV();
        static const char alphabet[] = "0123456789abcdefghijklmnopqrstuvwxyz";
        size_t count = ((v >> 42) & 7) + 1;
        uint64_t value = v & VALUE;
        buf[0] = char((v >> 45) & 0xFF);
        for (size_t i = count; i > 0; --i) {
            buf[i] = alphabet[value % 36];
            value /= 36;
        }
        return SV(buf, count + 1);
    }
    size_t size() const {
        char buf[16];
        return text(buf).size();
    }
    bool operator==(const Key &o) const {
        if (v == o.v)
            return true;
        if (packed() || o.packed() || !v || !o.v)
            return false;
        char a[16], b[16];
        return text(a) == o.text(b);
    }
    bool operator!=(const Key &o) const { return !(*this == o); }
    // Byte-wise order. Packed keys compare by first byte, then by the digits
    // left-aligned to eight places (base-36 digits sort like their ASCII
    // bytes), then by length, so a prefix sorts first.
    bool operator<(const Key &o) const {
        if (packed() && o.packed())
            return order(v) < order(o.v);
        char a[16], b[16];
        return text(a) < o.text(b);
    }
    static uint64_t order(uint64_t v) {
        static const uint64_t scale[9] = {1, 36, 1296, 46656, 1679616, 60466176, 2176782336ULL,
                                          78364164096ULL, 2821109907456ULL};
        uint64_t count = ((v >> 42) & 7) + 1;
        return ((v >> 45) & 0xFF) << 48 | ((v & VALUE) * scale[8 - count]) << 3 | (count - 1);
    }
    uint64_t hash() const {
        if (!packed()) {
            char buf[16];
            return Hash{}(text(buf));
        }
        uint64_t h = mum(v ^ 0xe7037ed1a0b428dbULL, 0x8ebc6af09c88c6e3ULL);
        return mum(h ^ 0xa0761d6478bd642fULL, 0x589965cc75374cc3ULL);
    }
};
static_assert(sizeof(Key) == 8);

// Read-only copy of a small string map for per-row lookups: one contiguous,
// cache-resident probe sequence instead of a modulo and a node chain. An
// occupied flag (not an empty key) marks slots, since an empty ID is valid.
class FlatIds {
    struct Entry {
        SV key;
        uint32_t tag = 0, value = UINT32_MAX;
    };
    std::vector<Entry> entries;
    size_t mask = 0;

  public:
    explicit FlatIds(const Map<uint32_t> &map) {
        size_t cap = 16;
        while (cap / 2 < map.size())
            cap *= 2;
        entries.resize(cap);
        mask = cap - 1;
        for (auto &[key, value] : map) {
            uint64_t hash = Hash{}(key);
            size_t i = hash & mask;
            while (entries[i].value != UINT32_MAX)
                i = (i + 1) & mask;
            entries[i] = {key, uint32_t(hash >> 32), value};
        }
    }
    const uint32_t *find(SV key) const {
        uint64_t hash = Hash{}(key);
        uint32_t tag = uint32_t(hash >> 32);
        for (size_t i = hash & mask;; i = (i + 1) & mask) {
            auto &e = entries[i];
            if (e.value == UINT32_MAX)
                return nullptr;
            if (e.tag == tag && e.key == key)
                return &e.value;
        }
    }
};

// Visits a list in order, calling prefetch on each element a fixed distance
// before f, so random accesses that f makes have already been requested.
template <size_t Ahead = 16, class List, class P, class F> void each_ahead(const List &list, P prefetch, F f) {
    using Row = std::remove_reference_t<decltype(*list.first())>;
    std::array<const Row *, Ahead> ring;
    size_t n = 0;
    list.each([&](const Row &r) {
        prefetch(r);
        if (n >= Ahead)
            f(*ring[n % Ahead]);
        ring[n++ % Ahead] = &r;
    });
    for (size_t k = n > Ahead ? n - Ahead : 0; k < n; ++k)
        f(*ring[k % Ahead]);
}

// Input records own the key views. Keep just a fingerprint and winning record
// pointer in each slot, and compare the full ID whenever fingerprints match.
// The table has no erases and stays at roughly a two-thirds load factor or less.
template <class Row> class DedupTable {
    struct Entry {
        uint64_t hash = 0;
        const Row *value = nullptr;
    };
    std::vector<Entry> entries;

  public:
    explicit DedupTable(size_t n) {
        size_t cap = 8;
        while (cap - cap / 3 < n)
            cap *= 2;
        entries.resize(cap);
    }
    const Row *&slot(const Row &row, uint64_t hash) {
        // Sharding already consumed the low six bits. Reusing them here would
        // cluster every key in the shard into 1/64 of the initial buckets.
        size_t i = index(hash);
        while (entries[i].value &&
               (entries[i].hash != hash || entries[i].value->id != row.id))
            i = (i + 1) & (entries.size() - 1);
        entries[i].hash = hash;
        return entries[i].value;
    }
    size_t index(uint64_t hash) const { return (hash / SHARDS) & (entries.size() - 1); }
    void prefetch(uint64_t hash) const { __builtin_prefetch(&entries[index(hash)], 1); }
    // Visits winners in slot order. Their records are scattered in memory,
    // so fetch a few slots ahead.
    template <class F> void each(F f) {
        constexpr size_t ahead = 8;
        for (size_t i = 0; i < entries.size(); ++i) {
            if (i + ahead < entries.size() && entries[i + ahead].value)
                __builtin_prefetch(entries[i + ahead].value);
            if (entries[i].value)
                f(entries[i].value);
        }
    }
    // Inserts rows in order, prefetching each slot a few rows in advance.
    template <class List, class F> void insert_all(const List &list, F f) {
        each_ahead(list, [&](const Row &r) { prefetch(r.id.hash()); }, f);
    }
};

struct Mapped {
    const char *data = nullptr;
    size_t size = 0;
    explicit Mapped(const fs::path &p) {
        int fd = open(p.c_str(), O_RDONLY);
        if (fd < 0)
            throw std::runtime_error("cannot open " + p.string());
        struct stat st {};
        if (fstat(fd, &st)) {
            close(fd);
            throw std::runtime_error("cannot stat input");
        }
        size = st.st_size;
        if (size) {
            void *address = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
            if (address == MAP_FAILED) {
                close(fd);
                throw std::runtime_error("mmap failed");
            }
            data = static_cast<const char *>(address);
        }
        close(fd);
    }
    ~Mapped() {
        if (data)
            munmap(const_cast<char *>(data), size);
    }
    SV view() const { return size ? SV(data, size) : SV(); }
};
struct Inputs {
    std::vector<std::unique_ptr<Mapped>> files;
    SV file(const fs::path &p) {
        files.push_back(std::make_unique<Mapped>(p));
        return files.back()->view();
    }
    std::vector<SV> directory(const fs::path &p) {
        std::vector<fs::path> paths;
        for (auto &e : fs::directory_iterator(p))
            if (e.is_regular_file() && e.path().extension() == ".csv")
                paths.push_back(e.path());
        std::sort(paths.begin(), paths.end());
        std::vector<SV> result;
        for (auto &path : paths)
            result.push_back(file(path));
        return result;
    }
};

template <size_t N, class F> void rows(SV text, bool header, F f) {
#if defined(__SSE2__)
    if (header) {
        auto p = text.find('\n');
        if (p == SV::npos)
            return;
        text.remove_prefix(p + 1);
    }
    if (text.empty())
        return;
    // Scan commas, quotes and LF together. Count quotes while scanning, so a
    // valid quoted field never needs a second pass over its interior. Only LF
    // ends a row; CR, spaces and non-ASCII bytes remain ordinary field data.
    std::array<SV, N> fields;
    const char *field = text.data(), *cursor = field, *end = field + text.size();
    size_t count = 0, quotes = 0;
    bool good = true;
    auto special = [&](const char *position, char c) {
        if (c == '"') {
            ++quotes;
            return;
        }
        if (count < N) {
            SV value(field, size_t(position - field));
            if (quotes) {
                if (quotes != 2 || value.size() < 2 || value.front() != '"' || value.back() != '"')
                    good = false;
                else {
                    value.remove_prefix(1);
                    value.remove_suffix(1);
                }
            }
            fields[count++] = value;
        } else
            good = false;
        quotes = 0;
        // A synthetic terminator handles a final row without LF. Do not form a
        // pointer beyond the one-past-end position of the mapped input.
        field = c ? position + 1 : position;
        if (c != ',') {
            if (good && count == N)
                f(fields);
            good = true;
            count = 0;
        }
    };
    const __m128i comma = _mm_set1_epi8(','), quote = _mm_set1_epi8('"'), lf = _mm_set1_epi8('\n');
    while (size_t(end - cursor) >= 16) {
        __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(cursor));
        unsigned mask = unsigned(_mm_movemask_epi8(_mm_or_si128(
            _mm_or_si128(_mm_cmpeq_epi8(bytes, comma), _mm_cmpeq_epi8(bytes, quote)),
            _mm_cmpeq_epi8(bytes, lf))));
        while (mask) {
            unsigned at = unsigned(__builtin_ctz(mask));
            special(cursor + at, cursor[at]);
            mask &= mask - 1;
        }
        cursor += 16;
    }
    while (cursor != end) {
        char c = *cursor;
        if (c == ',' || c == '"' || c == '\n')
            special(cursor, c);
        ++cursor;
    }
    if (field != end || count)
        special(end, '\0');
#else
    if (header) {
        auto p = text.find('\n');
        if (p == SV::npos)
            return;
        text.remove_prefix(p + 1);
    }
    while (!text.empty()) {
        auto end = text.find('\n');
        SV line = text.substr(0, end);
        if (end == SV::npos)
            text = {};
        else
            text.remove_prefix(end + 1);
        std::array<SV, N> fields;
        size_t start = 0;
        bool good = true;
        for (size_t i = 0; i < N; ++i) {
            auto comma = line.find(',', start);
            if ((i == N - 1) != (comma == SV::npos)) {
                good = false;
                break;
            }
            SV field = line.substr(start, comma == SV::npos ? SV::npos : comma - start);
            if (!field.empty() && field.front() == '"') {
                if (field.size() < 2 || field.back() != '"') {
                    good = false;
                    break;
                }
                field.remove_prefix(1);
                field.remove_suffix(1);
            }
            if (field.find('"') != SV::npos) {
                good = false;
                break;
            }
            fields[i] = field;
            start = comma + 1;
        }
        if (good)
            f(fields);
    }
#endif
}
// One pass over a headerless chunk. Commas, quotes and LFs are located with
// SSE2 bitmasks; any line that contains a quote is handed to rows<N>, which
// applies the exact quoting rules. Other lines need exactly N - 1 commas.
#if defined(__x86_64__)
#include <immintrin.h>
// One line at a time with AVX-512: two 64-byte loads from the line start give
// comma, quote and LF bitmaps. A line must hold exactly N - 1 commas before its
// LF; each field's quotes must be none, or exactly its first and last byte with
// at least two bytes (then stripped), checked without branches. Lines of 128
// bytes or more and the last bytes of the text go to rows(), the reference rules.
template <size_t N, class F>
__attribute__((target("avx512bw,avx512f,bmi,bmi2,popcnt"))) void rows_avx512(SV text, F &f) {
    using U128 = unsigned __int128;
    const char *p = text.data(), *end = p + text.size();
    const __m512i comma = _mm512_set1_epi8(','), quote = _mm512_set1_epi8('"'), lf = _mm512_set1_epi8('\n');
    std::array<SV, N> fields;
    while (end - p >= 128) {
        __m512i lo = _mm512_loadu_si512(p), hi = _mm512_loadu_si512(p + 64);
        uint64_t lf_lo = _mm512_cmpeq_epi8_mask(lo, lf), lf_hi = _mm512_cmpeq_epi8_mask(hi, lf);
        unsigned length;
        U128 commas, quotes;
        if (lf_lo) {
            length = unsigned(_tzcnt_u64(lf_lo));
            commas = _bzhi_u64(_mm512_cmpeq_epi8_mask(lo, comma), length);
            quotes = _bzhi_u64(_mm512_cmpeq_epi8_mask(lo, quote), length);
        } else if (lf_hi) {
            unsigned rest = unsigned(_tzcnt_u64(lf_hi));
            length = 64 + rest;
            commas = U128(_bzhi_u64(_mm512_cmpeq_epi8_mask(hi, comma), rest)) << 64 |
                     _mm512_cmpeq_epi8_mask(lo, comma);
            quotes = U128(_bzhi_u64(_mm512_cmpeq_epi8_mask(hi, quote), rest)) << 64 |
                     _mm512_cmpeq_epi8_mask(lo, quote);
        } else {
            const char *stop = static_cast<const char *>(std::memchr(p, '\n', size_t(end - p)));
            const char *next = stop ? stop + 1 : end;
            rows<N>(SV(p, size_t(next - p)), false, f);
            p = next;
            continue;
        }
        unsigned count = unsigned(_mm_popcnt_u64(uint64_t(commas)) + _mm_popcnt_u64(uint64_t(commas >> 64)));
        if (count == N - 1) {
            bool good = true;
            unsigned start = 0;
            for (size_t i = 0; i < N; ++i) {
                unsigned stop = length;
                if (i + 1 < N) {
                    uint64_t low = uint64_t(commas);
                    stop = low ? unsigned(_tzcnt_u64(low)) : 64 + unsigned(_tzcnt_u64(uint64_t(commas >> 64)));
                    commas &= commas - 1;
                }
                unsigned width = stop - start;
                U128 q = (quotes >> start) & ((U128(1) << width) - 1);
                bool wrapped = width >= 2 && q == (U128(1) | (U128(1) << (width - 1)));
                good &= !q | wrapped;
                fields[i] = SV(p + start + wrapped, width - 2 * wrapped);
                start = stop + 1;
            }
            if (good)
                f(fields);
        }
        p += length + 1;
    }
    if (p < end)
        rows<N>(SV(p, size_t(end - p)), false, f);
}
static bool has_avx512() {
    static const bool yes = __builtin_cpu_supports("avx512bw") && __builtin_cpu_supports("avx512f") &&
                            __builtin_cpu_supports("bmi2") && __builtin_cpu_supports("popcnt");
    return yes;
}
#endif
template <size_t N, class F> void fast_rows(SV text, F f) {
#if defined(__x86_64__)
    if (has_avx512())
        return rows_avx512<N>(text, f);
#endif
    // Generated inputs quote about a fifth of all fields, so most lines hold a
    // quote. rows() validates quotes while it scans, in the same single pass.
    rows<N>(text, false, f);
}
// Buffers a few split rows so the caller can start fetching each row's
// random-access lookups before processing the first of them.
template <size_t N, class P, class F> void batched_rows(SV text, P prefetch, F f) {
    constexpr size_t batch = 16;
    std::array<std::array<SV, N>, batch> rows_buffer;
    std::array<decltype(prefetch(rows_buffer[0])), batch> keys;
    size_t n = 0;
    auto flush = [&] {
        for (size_t i = 0; i < n; ++i)
            f(rows_buffer[i], keys[i]);
        n = 0;
    };
    fast_rows<N>(text, [&](auto &r) {
        rows_buffer[n] = r;
        keys[n] = prefetch(r);
        if (++n == batch)
            flush();
    });
    flush();
}
static I64 days_from_civil(int y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yo = y - era * 400;
    const unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    return era * 146097 + (yo * 365 + yo / 4 - yo / 100 + doy) - 719468;
}
static I64 timestamp(SV s) {
    if (s.empty())
        return BAD;
    if ((s.size() != 20 && s.size() != 25) || s[4] != '-') {
        I64 n = 0;
        // Up to 18 decimal digits cannot overflow int64_t. The slower path
        // retains arbitrary leading zeroes and checked 19+ digit conversion.
        if (s.size() <= 18) {
            for (unsigned char c : s) {
                unsigned digit = unsigned(c) - '0';
                if (digit > 9)
                    return BAD;
                n = n * 10 + digit;
            }
        } else {
            for (unsigned char c : s) {
                unsigned digit = unsigned(c) - '0';
                if (digit > 9 || n > (std::numeric_limits<I64>::max() - digit) / 10)
                    return BAD;
                n = n * 10 + digit;
            }
        }
        return n;
    }
    if (s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':')
        return BAD;
    auto number = [&](int at, int count) {
        int n = 0;
        for (int i = 0; i < count; ++i) {
            unsigned digit = unsigned(static_cast<unsigned char>(s[at + i])) - '0';
            if (digit > 9)
                return -1;
            n = n * 10 + digit;
        }
        return n;
    };
    // Many timestamps share a date. Cache only fully validated YYYY-MM-DD
    // prefixes, comparing all ten bytes before reusing the civil-date result.
    // The cache belongs to this worker thread and never survives a process.
    struct DateEntry {
        uint64_t prefix = 0;
        uint16_t suffix = 0;
        I64 midnight = BAD;
    };
    static thread_local std::array<DateEntry, 32> dates;
    uint64_t prefix;
    uint16_t suffix;
    std::memcpy(&prefix, s.data(), sizeof(prefix));
    std::memcpy(&suffix, s.data() + 8, sizeof(suffix));
    auto &entry = dates[(unsigned(static_cast<unsigned char>(s[8])) * 10 +
                         unsigned(static_cast<unsigned char>(s[9]))) & 31];
    I64 midnight;
    if (entry.prefix == prefix && entry.suffix == suffix && entry.midnight != BAD)
        midnight = entry.midnight;
    else {
        int y = number(0, 4), m = number(5, 2), d = number(8, 2);
        static const int md[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        if (y < 1 || m < 1 || m > 12 || d < 1 ||
            d > md[m] + (m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)))
            return BAD;
        midnight = days_from_civil(y, m, d) * DAY;
        entry = {prefix, suffix, midnight};
    }
    int h = number(11, 2), mi = number(14, 2), sec = number(17, 2);
    if (h < 0 || h > 23 || mi < 0 || mi > 59 || sec < 0 || sec > 59)
        return BAD;
    I64 t = midnight + (h * 3600 + mi * 60 + sec) * 1000LL;
    if (s.size() == 20)
        return s[19] == 'Z' ? t : BAD;
    if ((s[19] != '+' && s[19] != '-') || s[22] != ':')
        return BAD;
    int oh = number(20, 2), om = number(23, 2);
    if (oh < 0 || oh > 23 || om < 0 || om > 59)
        return BAD;
    return t - (s[19] == '+' ? 1 : -1) * (oh * 60 + om) * 60000LL;
}
static int price(SV s) {
    if (s.empty())
        return 0;
    int n = 0;
    for (unsigned char c : s) {
        if (c < '0' || c > '9' || n > 10000000 / 10)
            return 0;
        n = n * 10 + c - '0';
    }
    return n >= 1 && n <= 10000000 ? n : 0;
}
static I64 floor_day(I64 t) {
    I64 d = t / DAY;
    return d - (t % DAY < 0);
}

template <class F> void parallel(size_t jobs, size_t threads, F f) {
    std::atomic<size_t> next{0};
    std::exception_ptr failure;
    std::mutex lock;
    std::vector<std::thread> pool;
    for (size_t t = 0; t < std::min(jobs, threads); ++t)
        pool.emplace_back([&, t] {
            try {
                for (size_t i; (i = next.fetch_add(1)) < jobs;)
                    f(i, t);
            } catch (...) {
                std::lock_guard<std::mutex> guard(lock);
                if (!failure)
                    failure = std::current_exception();
                next = jobs;
            }
        });
    for (auto &t : pool)
        t.join();
    if (failure)
        std::rethrow_exception(failure);
}
// A nominal byte range of one input file. Boundaries are resolved by the
// worker that parses it, so no input is touched serially beforehand (on cold
// storage, each touch is a blocking read). A record belongs to the range that
// holds its first byte; the header is the record at offset 0 and is skipped.
struct Chunk {
    SV file;
    size_t begin, end;
    SV resolve() const {
        auto line_start = [&](size_t at) {
            // The first record start at or after `at` (at > 0).
            if (at >= file.size())
                return file.size();
            auto p = file.find('\n', at - 1);
            return p == SV::npos ? file.size() : p + 1;
        };
        size_t first = line_start(begin ? begin : 1), last = line_start(end);
        return first < last ? file.substr(first, last - first) : SV();
    }
};
static std::vector<Chunk> chunks(const std::vector<SV> &files) {
    constexpr size_t size = 1 << 20;
    std::vector<Chunk> jobs;
    for (SV text : files)
        for (size_t at = 0; at < text.size(); at += size)
            jobs.push_back({text, at, std::min(text.size(), at + size)});
    return jobs;
}
// A person name together with its hash, so later phases never rehash cold input.
struct PersonRef {
    Key name;
    uint64_t hash;
};

// Linked IDs. Link rows are parsed in parallel, IDs are interned into
// hash-sharded tables in parallel, and union-find then runs on dense integers.
// Afterwards every table is read-only and serves identity queries directly.
class Identity {
    // While building, canon.v holds the ID's dense index; afterwards canon is
    // the person's canonical ID, so a hit resolves within this one entry. Empty
    // keys never occur (links with an empty side are dropped) and mark free slots.
    struct Entry {
        Key key, canon;
    };
    struct Ref {
        Key key;
        uint64_t hash;
        uint32_t slot; // 2 * pair index + side, relative to its job
    };
    struct Shard {
        std::vector<Entry> table;
        size_t mask = 0;
        uint32_t base = 0;
    };
    std::array<Shard, SHARDS> shards;
    std::vector<Key> names;
    static size_t index(uint64_t hash, size_t mask) { return (hash / SHARDS) & mask; }

  public:
    void build(const std::vector<Chunk> &jobs, size_t threads) {
        // Pass 1: parse each chunk and bucket its ID references by shard.
        std::vector<std::array<std::vector<Ref>, SHARDS>> refs(jobs.size());
        std::vector<uint32_t> pairs(jobs.size() + 1);
        parallel(jobs.size(), threads, [&](size_t job, size_t) {
            uint32_t n = 0;
            fast_rows<2>(jobs[job].resolve(), [&](auto &r) {
                if (r[0].empty() || r[1].empty())
                    return;
                for (int side = 0; side < 2; ++side) {
                    Key key = Key::of(r[side]);
                    uint64_t hash = key.hash();
                    refs[job][hash & (SHARDS - 1)].push_back({key, hash, 2 * n + side});
                }
                ++n;
            });
            pairs[job + 1] = n;
        });
        for (size_t j = 0; j < jobs.size(); ++j)
            pairs[j + 1] += pairs[j];
        // Pass 2: intern each shard independently. Sides receive (shard, local ID).
        std::vector<uint64_t> sides(size_t(pairs.back()) * 2);
        std::array<std::vector<Key>, SHARDS> local_names;
        parallel(SHARDS, threads, [&](size_t shard, size_t) {
            size_t count = 0;
            for (auto &job : refs)
                count += job[shard].size();
            size_t cap = 16;
            while (cap * 2 / 3 < count)
                cap *= 2;
            auto &sh = shards[shard];
            sh.table.assign(cap, {});
            sh.mask = cap - 1;
            auto &nm = local_names[shard];
            for (size_t j = 0; j < refs.size(); ++j) {
                for (auto &r : refs[j][shard]) {
                    size_t i = index(r.hash, sh.mask);
                    while (!sh.table[i].key.empty() && sh.table[i].key != r.key)
                        i = (i + 1) & sh.mask;
                    auto &e = sh.table[i];
                    if (e.key.empty()) {
                        e.key = r.key;
                        e.canon.v = nm.size();
                        nm.push_back(r.key);
                    }
                    sides[size_t(pairs[j]) * 2 + r.slot] = uint64_t(shard) << 32 | e.canon.v;
                }
                std::vector<Ref>().swap(refs[j][shard]);
            }
        });
        std::vector<std::array<std::vector<Ref>, SHARDS>>().swap(refs);
        uint32_t total = 0;
        for (size_t s = 0; s < SHARDS; ++s) {
            shards[s].base = total;
            total += local_names[s].size();
        }
        names.resize(total);
        parallel(SHARDS, threads, [&](size_t shard, size_t) {
            auto base = shards[shard].base;
            std::copy(local_names[shard].begin(), local_names[shard].end(), names.begin() + base);
            for (auto &e : shards[shard].table)
                if (!e.key.empty())
                    e.canon.v += base;
        });
        auto global = [&](uint64_t side) { return shards[side >> 32].base + uint32_t(side); };
        std::vector<uint32_t> parent(total);
        for (uint32_t i = 0; i < total; ++i)
            parent[i] = i;
        auto find = [&](uint32_t x) {
            while (parent[x] != x) {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            return x;
        };
        std::vector<uint32_t> size(total, 1);
        for (size_t k = 0; k < sides.size(); k += 2) {
            auto x = find(global(sides[k])), y = find(global(sides[k + 1]));
            if (x == y)
                continue;
            if (size[x] < size[y])
                std::swap(x, y);
            parent[y] = x;
            size[x] += size[y];
        }
        std::vector<uint32_t>().swap(size);
        std::vector<uint64_t>().swap(sides);
        // Flatten every path, then name each component by its bytewise-smallest
        // ID. The minimum is order-independent, so ranges run concurrently with
        // a compare-and-swap per improvement.
        for (uint32_t i = 0; i < total; ++i)
            parent[i] = find(i);
        std::unique_ptr<std::atomic<uint32_t>[]> best(new std::atomic<uint32_t>[total]);
        for (uint32_t i = 0; i < total; ++i)
            best[i].store(i, std::memory_order_relaxed);
        constexpr size_t range = 1 << 16;
        parallel((total + range - 1) / range, threads, [&](size_t job, size_t) {
            uint32_t first = job * range, last = std::min<size_t>(total, first + range);
            for (uint32_t i = first; i < last; ++i) {
                auto &slot = best[parent[i]];
                uint32_t current = slot.load(std::memory_order_relaxed);
                while (names[i] < names[current] &&
                       !slot.compare_exchange_weak(current, i, std::memory_order_relaxed))
                    ;
            }
        });
        parallel(SHARDS, threads, [&](size_t shard, size_t) {
            for (auto &e : shards[shard].table)
                if (!e.key.empty())
                    e.canon = names[best[parent[uint32_t(e.canon.v)]].load(std::memory_order_relaxed)];
        });
        std::vector<Key>().swap(names);
    }
    void prefetch(uint64_t hash) const {
        auto &sh = shards[hash & (SHARDS - 1)];
        if (!sh.table.empty())
            __builtin_prefetch(&sh.table[index(hash, sh.mask)]);
    }
    // The person's canonical ID: the smallest linked ID, or the ID itself.
    const Key &canonical(const Key &id, uint64_t hash) const {
        auto &sh = shards[hash & (SHARDS - 1)];
        if (sh.table.empty())
            return id;
        for (size_t i = index(hash, sh.mask);; i = (i + 1) & sh.mask) {
            auto &e = sh.table[i];
            if (e.key.empty())
                return id;
            if (e.key == id)
                return e.canon;
        }
    }
};

// Records carry keys but no hashes: a Key rehashes in a few nanoseconds from
// bytes already in the record, which is cheaper than moving 8 more bytes of
// every record through memory at each partitioning step.
// A valid event, stored once in the shard of its user ID. Deduplication
// tombstones losing copies in place by clearing the user (no stored event has
// an empty user); identity resolution rewrites the user to its canonical ID.
// `when` is the time offset from window_start (never negative, so it fits in
// 63 bits); a purchase (3% of events) instead sets the top bit and refers to its
// worker's purchase table, which holds its time, cents and category.
struct EventRec {
    Key user;
    uint64_t when;
};
struct Purchase {
    I64 ts;
    int cents;
    uint32_t category;
};
constexpr uint64_t PURCHASE = uint64_t(1) << 63;
// Every event row with an ID, valid or not: invalid earliest copies still
// block later valid ones. `event` is null for an invalid row.
struct DedupRec {
    Key id;
    I64 ingest;
    EventRec *event;
};
struct ClickRec {
    Key id, user;
    I64 ts;
    uint32_t campaign;
};
static_assert(sizeof(EventRec) == 16 && sizeof(DedupRec) == 24 && sizeof(ClickRec) == 32);
// After interning, each person's records are grouped by a counting sort and
// only the short per-person runs are ordered by time.
struct CompactEvent {
    I64 ts;
    int cents;
    uint32_t category;
};
struct CompactClick {
    Key id;
    I64 ts;
    uint32_t campaign;
};
// Most people have a few dozen records, where a plain insertion sort beats
// std::sort's per-call setup. Longer runs (shared devices) use std::sort.
template <class T, class Less> static void sort_run(T *first, T *last, Less less) {
    if (last - first > 32) {
        std::sort(first, last, less);
        return;
    }
    for (T *i = first + 1; i < last; ++i) {
        T value = *i;
        T *j = i;
        for (; j > first && less(value, j[-1]); --j)
            *j = j[-1];
        *j = value;
    }
}
// Stable LSD radix sort on key(row), whose values lie in [0, span]: 11 bits
// per pass (a 14-day window in milliseconds takes three). Passes in which every
// row has the same digit are skipped. Meant for cache-sized arrays.
template <class T, class KeyOf>
static void radix_sort(std::vector<T> &rows, std::vector<T> &scratch, uint64_t span, KeyOf key) {
    constexpr unsigned BITS = 11, BUCKETS = 1u << BITS;
    unsigned passes = 0;
    while (passes * BITS < 64 && (span >> (passes * BITS)))
        ++passes;
    if (rows.size() < 2 || !passes)
        return;
    scratch.resize(rows.size());
    std::vector<uint32_t> counts(size_t(passes) * BUCKETS);
    for (const auto &r : rows) {
        uint64_t k = key(r);
        for (unsigned p = 0; p < passes; ++p)
            ++counts[p * BUCKETS + ((k >> (p * BITS)) & (BUCKETS - 1))];
    }
    for (unsigned p = 0; p < passes; ++p) {
        uint32_t *c = &counts[p * BUCKETS];
        if (c[(key(rows[0]) >> (p * BITS)) & (BUCKETS - 1)] == rows.size())
            continue;
        uint32_t sum = 0;
        for (unsigned b = 0; b < BUCKETS; ++b) {
            uint32_t n = c[b];
            c[b] = sum;
            sum += n;
        }
        for (const auto &r : rows)
            scratch[c[(key(r) >> (p * BITS)) & (BUCKETS - 1)]++] = r;
        rows.swap(scratch);
    }
}
// Shard-local person interning. Hashes were computed during parsing; full
// names are still compared whenever hashes match, so grouping stays exact.
class PersonTable {
    struct Entry {
        uint64_t hash;
        Key name;
        uint32_t id;
    };
    std::vector<Entry> entries;
    std::vector<Key> names;
    size_t mask;
    // Bits 0-11 select a record's shard and piece, so all keys in one table
    // share them: index on the bits above.
    size_t index(uint64_t hash) const { return (hash >> 12) & mask; }

  public:
    // Requests a record's slot and name bytes before intern() or find().
    void prefetch(uint64_t hash) const { __builtin_prefetch(&entries[index(hash)]); }
    explicit PersonTable(size_t n) {
        size_t cap = 16;
        while (cap / 2 < n)
            cap *= 2;
        entries.assign(cap, {0, {}, UINT32_MAX});
        mask = cap - 1;
    }
    uint32_t intern(const PersonRef &p) {
        for (size_t i = index(p.hash);; i = (i + 1) & mask) {
            auto &e = entries[i];
            if (e.id == UINT32_MAX) {
                if (names.size() * 2 >= entries.size())
                    break;
                e = {p.hash, p.name, uint32_t(names.size())};
                names.push_back(p.name);
                return e.id;
            }
            if (e.hash == p.hash && e.name == p.name)
                return e.id;
        }
        grow();
        return intern(p);
    }
    uint32_t find(const PersonRef &p) const {
        for (size_t i = index(p.hash);; i = (i + 1) & mask) {
            auto &e = entries[i];
            if (e.id == UINT32_MAX)
                return UINT32_MAX;
            if (e.hash == p.hash && e.name == p.name)
                return e.id;
        }
    }
    void grow() {
        std::vector<Entry> old(entries.size() * 2, {0, {}, UINT32_MAX});
        old.swap(entries);
        mask = entries.size() - 1;
        for (auto &e : old)
            if (e.id != UINT32_MAX) {
                size_t i = index(e.hash);
                while (entries[i].id != UINT32_MAX)
                    i = (i + 1) & mask;
                entries[i] = e;
            }
    }
    size_t size() const { return names.size(); }
    const Key &name(uint32_t id) const { return names[id]; }
};
// Append-only storage for partition buffers. Blocks grow geometrically and
// never move, so records are written once instead of being copied on growth.
template <class T> class BlockList {
    static_assert(std::is_trivially_copyable_v<T> && std::is_trivially_destructible_v<T>);
    struct Free {
        void operator()(T *p) const { std::free(p); }
    };
    std::vector<std::unique_ptr<T, Free>> blocks;
    size_t count = 0, used = 0, capacity = 0;

  public:
    T *push_back(const T &value) {
        if (used == capacity) {
            capacity = std::min<size_t>(size_t(256) << std::min<size_t>(blocks.size(), 6), 16384);
            T *block = static_cast<T *>(std::malloc(capacity * sizeof(T)));
            if (!block)
                throw std::bad_alloc();
            blocks.emplace_back(block);
            used = 0;
        }
        T *stored = new (blocks.back().get() + used++) T(value);
        ++count;
        return stored;
    }
    size_t size() const { return count; }
    const T *first() const { return blocks.empty() ? nullptr : blocks[0].get(); }
    template <class F> void each(F f) const {
        for (size_t b = 0; b < blocks.size(); ++b) {
            size_t n = b + 1 == blocks.size() ? used
                                              : std::min<size_t>(size_t(256) << std::min<size_t>(b, 6), 16384);
            const T *block = blocks[b].get();
            for (size_t i = 0; i < n; ++i)
                f(block[i]);
        }
    }
    template <class F> void each_mut(F f) {
        for (size_t b = 0; b < blocks.size(); ++b) {
            size_t n = b + 1 == blocks.size() ? used
                                              : std::min<size_t>(size_t(256) << std::min<size_t>(b, 6), 16384);
            T *block = blocks[b].get();
            for (size_t i = 0; i < n; ++i)
                f(block[i]);
        }
    }
    void release() {
        std::vector<std::unique_ptr<T, Free>>().swap(blocks);
        count = used = capacity = 0;
    }
};
template <class T> using Partitions = std::array<BlockList<T>, SHARDS>;

// each_ahead for in-place updates: prefetch(row) a fixed distance before f(row).
template <size_t Ahead = 16, class T, class P, class F>
void each_ahead_mut(BlockList<T> &list, P prefetch, F f) {
    std::array<T *, Ahead> ring;
    size_t n = 0;
    list.each_mut([&](T &r) {
        prefetch(r);
        if (n >= Ahead)
            f(*ring[n % Ahead]);
        ring[n++ % Ahead] = &r;
    });
    for (size_t k = n > Ahead ? n - Ahead : 0; k < n; ++k)
        f(*ring[k % Ahead]);
}
struct UserRow {
    Key person;
    int32_t day;
    I64 sessions, events, revenue;
    int32_t category;
};
struct Stats {
    I64 sessions = 0, purchasing = 0, revenue = 0, assists = 0;
    Stats &operator+=(const Stats &x) {
        sessions += x.sessions;
        purchasing += x.purchasing;
        revenue += x.revenue;
        assists += x.assists;
        return *this;
    }
};
using CampaignDaily = std::unordered_map<uint64_t, Stats>;
static uint64_t campaign_key(uint32_t c, int32_t d) { return (uint64_t(uint32_t(d)) << 32) | c; }

// Campaigns and dates have a small, input-derived domain in the challenge.
// Keep one accumulator per worker, reused across its partitions. Fall back to
// sparse storage when a wider window/domain would exceed the shared budget.
class CampaignAccumulator {
    size_t campaign_count;
    int32_t first_day;
    std::vector<Stats> dense;
    CampaignDaily sparse;

  public:
    CampaignAccumulator(size_t campaigns, int32_t first, size_t days, size_t workers)
        : campaign_count(campaigns), first_day(first) {
        constexpr size_t budget = 64 * 1024 * 1024;
        size_t max_cells = budget / workers / sizeof(Stats);
        if (campaigns && days <= max_cells / campaigns)
            dense.resize(campaigns * days);
    }
    Stats &get(uint32_t campaign, int32_t day) {
        if (!dense.empty())
            return dense[size_t(int64_t(day) - first_day) * campaign_count + campaign];
        return sparse[campaign_key(campaign, day)];
    }
    void merge_into(CampaignDaily &result) const {
        if (dense.empty()) {
            for (auto &[key, value] : sparse)
                result[key] += value;
        } else {
            for (size_t i = 0; i < dense.size(); ++i) {
                auto &value = dense[i];
                if (value.sessions || value.assists)
                    result[campaign_key(
                        i % campaign_count,
                        int32_t(int64_t(first_day) + int64_t(i / campaign_count)))] += value;
            }
        }
    }
};

static void check(arrow::Status s) {
    if (!s.ok())
        throw std::runtime_error(s.ToString());
}
template <class T> static T unwrap(arrow::Result<T> r) {
    if (!r.ok())
        throw std::runtime_error(r.status().ToString());
    return std::move(r).ValueOrDie();
}
template <class B> static std::shared_ptr<arrow::Array> finish(B &b) { return unwrap(b.Finish()); }
static void write_table(const fs::path &path, std::shared_ptr<arrow::Table> table) {
    fs::create_directories(path.parent_path());
    auto stream = unwrap(arrow::io::FileOutputStream::Open(path.string()));
    parquet::WriterProperties::Builder properties;
    // Uncompressed, and no dictionary for user_id (nearly every value is
    // distinct): encoding cost, not file size, is what is timed. The 20M
    // output is about 37 MB this way.
    properties.compression(parquet::Compression::UNCOMPRESSED);
    properties.disable_dictionary("user_id");
    // Encode the columns of each row group concurrently; the file layout is unchanged.
    auto arrow_properties = parquet::ArrowWriterProperties::Builder().set_use_threads(true)->build();
    check(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), stream, 65536,
                                     properties.build(), arrow_properties));
    check(stream->Close());
}
// Calls f for rows [first, last) of the concatenation of all lists.
template <class F>
static void each_row(const std::vector<std::vector<UserRow>> &lists, size_t first, size_t last, F f) {
    size_t base = 0;
    for (auto &list : lists) {
        size_t from = std::max(first, base), to = std::min(last, base + list.size());
        for (size_t i = from; i < to; ++i)
            f(list[i - base]);
        base += list.size();
        if (base >= last)
            break;
    }
}
// Rows [first, last) of all workers' rows, in order, form one Parquet part.
static void write_users(const fs::path &path,
                        const std::vector<std::vector<UserRow>> &results, size_t first,
                        size_t last, const std::vector<SV> &categories) {
    arrow::StringBuilder user, category;
    arrow::Date32Builder day;
    arrow::Int64Builder sessions, events, revenue;
    size_t n = last - first, bytes = 0;
    each_row(results, first, last, [&](const UserRow &r) { bytes += r.person.size(); });
    check(user.Reserve(n));
    check(user.ReserveData(bytes));
    check(category.Reserve(n));
    check(day.Reserve(n));
    check(sessions.Reserve(n));
    check(events.Reserve(n));
    check(revenue.Reserve(n));
    each_row(results, first, last, [&](const UserRow &r) {
            char buf[16];
            SV name = r.person.text(buf);
            check(user.Append(name.data(), name.size()));
            day.UnsafeAppend(r.day);
            sessions.UnsafeAppend(r.sessions);
            events.UnsafeAppend(r.events);
            revenue.UnsafeAppend(r.revenue);
            if (r.category < 0)
                check(category.AppendNull());
            else {
                auto c = categories[r.category];
                check(category.Append(c.data(), c.size()));
            }
        });
    auto schema = arrow::schema(
        {arrow::field("user_id", arrow::utf8()), arrow::field("day", arrow::date32()),
         arrow::field("sessions", arrow::int64()), arrow::field("events", arrow::int64()),
         arrow::field("revenue_cents", arrow::int64()),
         arrow::field("top_category", arrow::utf8())});
    write_table(path, arrow::Table::Make(schema, {finish(user), finish(day), finish(sessions),
                                                  finish(events), finish(revenue),
                                                  finish(category)}));
}
static void write_campaigns(const fs::path &out, const CampaignDaily &daily,
                            const std::vector<std::pair<SV, SV>> &campaigns) {
    arrow::StringBuilder campaign, channel;
    arrow::Date32Builder day;
    arrow::Int64Builder sessions, purchasing, revenue, assists;
    for (auto &[key, r] : daily) {
        auto [c, ch] = campaigns[uint32_t(key)];
        check(campaign.Append(c.data(), c.size()));
        check(channel.Append(ch.data(), ch.size()));
        check(day.Append(int32_t(key >> 32)));
        check(sessions.Append(r.sessions));
        check(purchasing.Append(r.purchasing));
        check(revenue.Append(r.revenue));
        check(assists.Append(r.assists));
    }
    auto schema = arrow::schema(
        {arrow::field("campaign_id", arrow::utf8()), arrow::field("channel", arrow::utf8()),
         arrow::field("day", arrow::date32()), arrow::field("sessions", arrow::int64()),
         arrow::field("purchasing_sessions", arrow::int64()),
         arrow::field("revenue_cents", arrow::int64()),
         arrow::field("assisted_sessions", arrow::int64())});
    write_table(out / "campaign_daily" / "part-0.parquet",
                arrow::Table::Make(schema, {finish(campaign), finish(channel), finish(day),
                                            finish(sessions), finish(purchasing), finish(revenue),
                                            finish(assists)}));
}
static std::string env(const char *key, const char *fallback) {
    auto p = std::getenv(key);
    return p ? p : fallback;
}

static int run() {
    try {
        auto begin = std::chrono::steady_clock::now(), last = begin;
        auto timing = [&](const char *name) {
            auto now = std::chrono::steady_clock::now();
            std::cerr << name << "=" << std::chrono::duration<double>(now - last).count() << " ";
            last = now;
        };
        fs::path data = env("DATA_DIR", "/data"), out = env("OUTPUT_DIR", "/output");
        size_t threads = std::stoul(env("NCPU", "8"));
        if (!threads || threads > 256)
            throw std::runtime_error("NCPU must be 1..256");
        Inputs inputs;
        // The manifest's schema guarantees these two unescaped F1 string values.
        SV manifest = inputs.file(data / "manifest.json");
        auto manifest_time = [&](SV name) {
            auto at = manifest.find(name);
            if (at == SV::npos)
                throw std::runtime_error("missing manifest key");
            at = manifest.find(':', at);
            at = manifest.find('"', at);
            auto end = manifest.find('"', at + 1);
            return timestamp(manifest.substr(at + 1, end - at - 1));
        };
        I64 lo = manifest_time("window_start"), hi = manifest_time("window_end");
        if (lo == BAD || hi == BAD || lo >= hi)
            throw std::runtime_error("invalid manifest window");
        Identity people;
        people.build(chunks(inputs.directory(data / "links")), threads);
        std::vector<SV> categories{"unknown"};
        Map<uint32_t> cat_ids{{"unknown", 0}}, product_map;
        rows<2>(inputs.file(data / "products.csv"), true, [&](auto &r) {
            auto [it, added] = cat_ids.emplace(r[1], categories.size());
            if (added)
                categories.push_back(r[1]);
            product_map[r[0]] = it->second;
        });
        FlatIds products(product_map);
        std::vector<std::pair<SV, SV>> campaigns{{"organic", "organic"}};
        Map<uint32_t> campaign_map;
        rows<2>(inputs.file(data / "campaigns.csv"), true, [&](auto &r) {
            campaign_map[r[0]] = campaigns.size();
            campaigns.emplace_back(r[0], r[1]);
        });
        FlatIds campaign_ids(campaign_map);
        auto event_jobs = chunks(inputs.directory(data / "events"));
        auto click_jobs = chunks(inputs.directory(data / "clicks"));
        timing("setup");
        // Valid events and clicks are stored once, in the shard of their raw
        // user ID. Identity tables are sharded by the same hash, so each shard
        // later resolves its users against one small, cache-resident table.
        std::vector<Partitions<EventRec>> events_by_user(threads);
        std::vector<std::vector<Purchase>> purchases(threads);
        std::vector<Partitions<DedupRec>> event_ids(threads);
        parallel(event_jobs.size(), threads, [&](size_t job, size_t worker) {
            fast_rows<7>(event_jobs[job].resolve(), [&](auto &r) {
                if (r[0].empty())
                    return;
                EventRec *stored = nullptr;
                // ingest_ts is guaranteed valid F1; integer ordering is the
                // same as its original lexical ordering. Reuse this parse for
                // lateness and deduplication, even for invalid winning events.
                I64 ingest = timestamp(r[3]);
                bool valid = !r[1].empty() && !r[5].empty() &&
                             (r[4] == "view" || r[4] == "cart" || r[4] == "purchase");
                I64 ts = valid ? timestamp(r[2]) : BAD;
                if (ts != BAD && ts >= lo && ts < hi) {
                    int cents = r[4] == "purchase" ? price(r[6]) : 0;
                    if (ingest != BAD && ingest - ts <= DAY && (r[4] != "purchase" || cents)) {
                        // Categories only contribute for purchases. Avoid a dictionary
                        // lookup for the much larger view/cart population.
                        uint32_t category = 0;
                        if (cents) {
                            if (auto product = products.find(r[5]))
                                category = *product;
                        }
                        Key user = Key::of(r[1]);
                        uint64_t when = uint64_t(ts - lo);
                        if (cents) {
                            auto &table = purchases[worker];
                            when = PURCHASE | uint64_t(worker) << 32 | table.size();
                            table.push_back({ts, cents, category});
                        }
                        stored = events_by_user[worker][user.hash() & (SHARDS - 1)].push_back({user, when});
                    }
                }
                // Invalid winners must remain present until global ID deduplication.
                Key id = Key::of(r[0]);
                event_ids[worker][id.hash() & (SHARDS - 1)].push_back({id, ingest, stored});
            });
        });
        timing("parse_events");
        // The earliest copy of each event ID wins. Every other stored copy is
        // tombstoned in place, so winners are never copied.
        parallel(SHARDS, threads, [&](size_t shard, size_t) {
            size_t count = 0;
            for (auto &p : event_ids)
                count += p[shard].size();
            DedupTable<DedupRec> unique(count);
            auto drop = [](const DedupRec *r) {
                if (r->event)
                    r->event->user = Key{};
            };
            for (auto &p : event_ids)
                unique.insert_all(p[shard], [&](const DedupRec &r) {
                    auto &old = unique.slot(r, r.id.hash());
                    if (!old)
                        old = &r;
                    else if (r.ingest < old->ingest) {
                        drop(old);
                        old = &r;
                    } else
                        drop(&r);
                });
            for (auto &p : event_ids)
                p[shard].release();
        });
        event_ids.clear();
        event_ids.shrink_to_fit();
        timing("dedup_events");
        // Click duplicates are identical after unquoting (so validation
        // commutes), and identical clicks change neither the latest click nor
        // the set of assisting campaigns. They need no global deduplication.
        std::vector<Partitions<ClickRec>> clicks_by_user(threads);
        parallel(click_jobs.size(), threads, [&](size_t job, size_t worker) {
            fast_rows<4>(click_jobs[job].resolve(), [&](auto &r) {
                if (r[0].empty() || r[1].empty())
                    return;
                auto c = campaign_ids.find(r[2]);
                if (!c)
                    return;
                I64 ts = timestamp(r[3]);
                if (ts == BAD || ts < lo - REACH || ts >= hi)
                    return;
                Key user = Key::of(r[1]);
                clicks_by_user[worker][user.hash() & (SHARDS - 1)].push_back(
                    {Key::of(r[0]), user, ts, *c});
            });
        });
        timing("clicks");
        // Cache-sized pieces. Each shard's records are resolved against that
        // shard's identity table and split by six more bits of the canonical
        // user's hash into PIECES pieces of a few thousand events; a record whose
        // person lives in another shard is copied there. Each piece is then
        // aggregated on its own: its person table, records and sort fit in a
        // core's cache, and 4096 small jobs keep threads evenly loaded.
        constexpr size_t PIECES = 64;
        struct PieceEvent {
            Key person; // canonical
            uint64_t when; // as in EventRec
        };
        struct PieceClick {
            Key person, id;
            I64 ts;
            uint32_t campaign;
        };
        auto piece_of = [](uint64_t h) { return size_t(h >> 6) & (PIECES - 1); };
        std::vector<std::array<BlockList<PieceEvent>, PIECES>> piece_events(SHARDS);
        std::vector<std::array<BlockList<PieceClick>, PIECES>> piece_clicks(SHARDS);
        std::array<Partitions<PieceEvent>, SHARDS> moved_events; // [from][to]
        std::array<Partitions<PieceClick>, SHARDS> moved_clicks;
        parallel(SHARDS, threads, [&](size_t shard, size_t) {
            for (auto &p : events_by_user) {
                each_ahead(
                    p[shard], [&](const EventRec &e) { people.prefetch(e.user.hash()); },
                    [&](const EventRec &e) {
                        if (e.user.empty())
                            return;
                        uint64_t h = e.user.hash();
                        const Key &canon = people.canonical(e.user, h);
                        if (canon != e.user)
                            h = canon.hash();
                        PieceEvent pe{canon, e.when};
                        size_t to = h & (SHARDS - 1);
                        if (to == shard)
                            piece_events[shard][piece_of(h)].push_back(pe);
                        else
                            moved_events[shard][to].push_back(pe);
                    });
                p[shard].release();
            }
            for (auto &p : clicks_by_user) {
                each_ahead(
                    p[shard], [&](const ClickRec &c) { people.prefetch(c.user.hash()); },
                    [&](const ClickRec &c) {
                        uint64_t h = c.user.hash();
                        const Key &canon = people.canonical(c.user, h);
                        if (canon != c.user)
                            h = canon.hash();
                        PieceClick pc{canon, c.id, c.ts, c.campaign};
                        size_t to = h & (SHARDS - 1);
                        if (to == shard)
                            piece_clicks[shard][piece_of(h)].push_back(pc);
                        else
                            moved_clicks[shard][to].push_back(pc);
                    });
                p[shard].release();
            }
        });
        parallel(SHARDS, threads, [&](size_t shard, size_t) {
            for (size_t from = 0; from < SHARDS; ++from) {
                auto &me = moved_events[from][shard];
                me.each([&](const PieceEvent &e) { piece_events[shard][piece_of(e.person.hash())].push_back(e); });
                me.release();
                auto &mc = moved_clicks[from][shard];
                mc.each([&](const PieceClick &c) { piece_clicks[shard][piece_of(c.person.hash())].push_back(c); });
                mc.release();
            }
        });
        timing("resolve");
        std::vector<std::vector<UserRow>> user_rows(threads);
        std::vector<CampaignAccumulator> campaign_rows;
        for (size_t i = 0; i < threads; ++i)
            campaign_rows.emplace_back(campaigns.size(), floor_day(lo),
                                       floor_day(hi - 1) - floor_day(lo) + 1, threads);
        parallel(SHARDS * PIECES, threads, [&](size_t job, size_t worker) {
            auto &piece_e = piece_events[job / PIECES][job % PIECES];
            auto &piece_c = piece_clicks[job / PIECES][job % PIECES];
            // Intern persons, then group each person's records contiguously with
            // a counting sort; only the per-person runs are sorted by time.
            constexpr uint32_t NONE = UINT32_MAX;
            PersonTable people_here(piece_e.size() / 8 + 16);
            struct PersonEvent {
                uint32_t person;
                CompactEvent event;
            };
            std::vector<PersonEvent> gathered;
            gathered.reserve(piece_e.size());
            piece_e.each([&](const PieceEvent &e) {
                CompactEvent ce{lo + I64(e.when), 0, 0};
                if (e.when & PURCHASE) {
                    auto &pu = purchases[(e.when >> 32) & 0xFF][uint32_t(e.when)];
                    ce = {pu.ts, pu.cents, pu.category};
                }
                gathered.push_back({people_here.intern({e.person, e.person.hash()}), ce});
            });
            piece_e.release();
            // Radix-sort the piece by time; the stable counting sort by person
            // below then leaves every person's events in time order.
            {
                std::vector<PersonEvent> scratch;
                radix_sort(gathered, scratch, uint64_t(hi - 1 - lo),
                           [&](const PersonEvent &g) { return uint64_t(g.event.ts - lo); });
            }
            size_t np = people_here.size();
            std::vector<uint32_t> event_start(np + 1), click_start(np + 1);
            for (auto &g : gathered)
                ++event_start[g.person + 1];
            for (size_t p = 0; p < np; ++p)
                event_start[p + 1] += event_start[p];
            std::vector<CompactEvent> events(gathered.size());
            {
                std::vector<uint32_t> fill(event_start.begin(), event_start.end() - 1);
                for (auto &g : gathered)
                    events[fill[g.person]++] = g.event;
            }
            std::vector<PersonEvent>().swap(gathered);
            struct PersonClick {
                uint32_t person;
                CompactClick click;
            };
            std::vector<PersonClick> gathered_clicks;
            gathered_clicks.reserve(piece_c.size());
            piece_c.each([&](const PieceClick &c) {
                auto p = people_here.find({c.person, c.person.hash()});
                if (p != NONE)
                    gathered_clicks.push_back({p, {c.id, c.ts, c.campaign}});
            });
            piece_c.release();
            {
                std::vector<PersonClick> scratch;
                radix_sort(gathered_clicks, scratch, uint64_t(hi - 1 - (lo - REACH)),
                           [&](const PersonClick &g) { return uint64_t(g.click.ts - (lo - REACH)); });
            }
            for (auto &g : gathered_clicks)
                ++click_start[g.person + 1];
            for (size_t p = 0; p < np; ++p)
                click_start[p + 1] += click_start[p];
            std::vector<CompactClick> clicks(gathered_clicks.size());
            {
                std::vector<uint32_t> fill(click_start.begin(), click_start.end() - 1);
                for (auto &g : gathered_clicks)
                    clicks[fill[g.person]++] = g.click;
            }
            std::vector<PersonClick>().swap(gathered_clicks);
            // A person's clicks at the same millisecond: the smallest ID must come
            // last. Tie groups never cross into the next person's range.
            for (size_t p = 0; p < np; ++p) {
                size_t end = click_start[p + 1];
                for (size_t i = click_start[p] + 1; i < end; ++i)
                    if (clicks[i].ts == clicks[i - 1].ts) {
                        size_t first = i - 1, last = i + 1;
                        while (last < end && clicks[last].ts == clicks[first].ts)
                            ++last;
                        std::sort(clicks.begin() + first, clicks.begin() + last,
                                  [](const CompactClick &a, const CompactClick &b) { return b.id < a.id; });
                        i = last - 1;
                    }
            }
            auto &rows = user_rows[worker];
            auto &daily = campaign_rows[worker];
            std::vector<I64> category_revenue(categories.size());
            std::vector<uint32_t> active_counts(campaigns.size()), positions(campaigns.size()),
                active;
            for (uint32_t person = 0; person < np; ++person) {
                size_t at = event_start[person], end = event_start[person + 1];
                size_t ci = click_start[person], ce = click_start[person + 1];
                for (auto c : active)
                    active_counts[c] = 0;
                active.clear();
                size_t left = ci, right = ci;
                while (at < end) {
                    int32_t day = floor_day(events[at].ts);
                    UserRow row{people_here.name(person), day, 0, 0, 0, -1};
                    std::fill(category_revenue.begin(), category_revenue.end(), 0);
                    do {
                        I64 start = events[at].ts, previous = start, rev = 0;
                        size_t j = at;
                        do {
                            auto &e = events[j];
                            previous = e.ts;
                            rev += e.cents;
                            if (e.cents)
                                category_revenue[e.category] += e.cents;
                            ++j;
                        } while (j < end && events[j].ts - previous <= 1800000);
                        ++row.sessions;
                        row.events += j - at;
                        row.revenue += rev;
                        while (right < ce && clicks[right].ts <= start) {
                            auto c = clicks[right++].campaign;
                            if (!active_counts[c]++) {
                                positions[c] = active.size();
                                active.push_back(c);
                            }
                        }
                        while (left < right && clicks[left].ts < start - REACH) {
                            auto c = clicks[left++].campaign;
                            if (!--active_counts[c]) {
                                auto pos = positions[c];
                                active[pos] = active.back();
                                positions[active[pos]] = pos;
                                active.pop_back();
                            }
                        }
                        uint32_t credit = right > left ? clicks[right - 1].campaign : 0;
                        auto &stat = daily.get(credit, day);
                        ++stat.sessions;
                        stat.purchasing += rev > 0;
                        stat.revenue += rev;
                        for (auto c : active)
                            ++daily.get(c, day).assists;
                        at = j;
                    } while (at < end && floor_day(events[at].ts) == day);
                    for (size_t c = 0; c < categories.size(); ++c)
                        if (category_revenue[c] &&
                            (row.category < 0 ||
                             category_revenue[c] > category_revenue[row.category] ||
                             (category_revenue[c] == category_revenue[row.category] &&
                              categories[c] < categories[row.category])))
                            row.category = c;
                    rows.push_back(row);
                }
            }
        });
        timing("sessionize_aggregate");
        CampaignDaily daily;
        for (auto &part : campaign_rows)
            part.merge_into(daily);
        // Large user tables are split into part files encoded concurrently;
        // readers take every *.parquet file in the table directory.
        size_t total_rows = 0;
        for (auto &part : user_rows)
            total_rows += part.size();
        size_t parts = std::max<size_t>(1, std::min(threads, total_rows / 262144));
        // Equal row ranges across all workers' rows.
        std::vector<size_t> bounds;
        for (size_t i = 0; i <= parts; ++i)
            bounds.push_back(total_rows * i / parts);
        parallel(parts + 1, std::min(threads, parts + 1), [&](size_t i, size_t) {
            if (i == parts)
                write_campaigns(out, daily, campaigns);
            else
                write_users(out / "user_daily" / ("part-" + std::to_string(i) + ".parquet"),
                            user_rows, bounds[i], bounds[i + 1], categories);
        });
        timing("write_parquet");
        I64 n = 0, sessions = 0, events = 0, revenue = 0;
        for (auto &part : user_rows)
            for (auto &r : part) {
                ++n;
                sessions += r.sessions;
                events += r.events;
                revenue += r.revenue;
            }
        std::cerr << "\n{\"user_rows\":" << n << ",\"campaign_rows\":" << daily.size()
                  << ",\"sessions\":" << sessions << ",\"events\":" << events
                  << ",\"revenue_cents\":" << revenue << ",\"elapsed_seconds\":"
                  << std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count()
                  << "}\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "pipeline: " << e.what() << '\n';
        return 1;
    }
}

// Optional lifecycle markers use the same OS monotonic clock as the parent
// benchmark, separating launch/loader time from local cleanup and process exit.
static void lifecycle(const char *phase) {
    struct timespec ts {};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        throw std::runtime_error("cannot read monotonic clock");
    std::cerr << "lifecycle_" << phase << "_ns=" << uint64_t(ts.tv_sec) * 1000000000ULL + ts.tv_nsec
              << '\n';
}

int main() {
    bool profile = std::getenv("PROFILE_LIFECYCLE") != nullptr;
    if (profile)
        lifecycle("main_enter");
    int result = run();
    if (profile)
        lifecycle("main_exit");
    return result;
}
