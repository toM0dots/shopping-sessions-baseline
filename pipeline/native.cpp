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
        each_ahead(list, [&](const Row &r) { prefetch(r.hash); }, f);
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
template <size_t N, class F> void fast_rows(SV text, F f) {
    const char *p = text.data(), *end = p + text.size(), *line = p, *field = p;
    std::array<SV, N> fields;
    size_t commas = 0;
    bool quoted = false;
    auto special = [&](const char *q) {
        char c = *q;
        if (c == ',') {
            if (commas < N)
                fields[commas] = SV(field, q - field);
            ++commas;
            field = q + 1;
        } else if (c == '"') {
            quoted = true;
        } else {
            if (quoted)
                rows<N>(SV(line, q - line), false, f);
            else if (commas == N - 1) {
                fields[N - 1] = SV(field, q - field);
                f(fields);
            }
            line = field = q + 1;
            commas = 0;
            quoted = false;
        }
    };
#if defined(__SSE2__)
    const __m128i comma = _mm_set1_epi8(','), quote = _mm_set1_epi8('"'), lf = _mm_set1_epi8('\n');
    for (; end - p >= 16; p += 16) {
        __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i *>(p));
        unsigned mask = unsigned(_mm_movemask_epi8(_mm_or_si128(
            _mm_or_si128(_mm_cmpeq_epi8(v, comma), _mm_cmpeq_epi8(v, quote)), _mm_cmpeq_epi8(v, lf))));
        while (mask) {
            special(p + __builtin_ctz(mask));
            mask &= mask - 1;
        }
    }
#endif
    for (; p < end; ++p)
        if (*p == ',' || *p == '"' || *p == '\n')
            special(p);
    if (line < end) {
        // A final record without a trailing LF.
        if (quoted)
            rows<N>(SV(line, end - line), false, f);
        else if (commas == N - 1) {
            fields[N - 1] = SV(field, end - field);
            f(fields);
        }
    }
}
// Buffers a few split rows so the caller can start fetching each row's
// random-access lookups before processing the first of them.
template <size_t N, class P, class F> void batched_rows(SV text, P prefetch, F f) {
    constexpr size_t batch = 16;
    std::array<std::array<SV, N>, batch> rows_buffer;
    std::array<uint64_t, batch> keys;
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
    SV name;
    uint64_t hash;
};

// Linked IDs. Link rows are parsed in parallel, IDs are interned into
// hash-sharded tables in parallel, and union-find then runs on dense integers.
// Afterwards every table is read-only and serves identity queries directly.
class Identity {
    struct Entry {
        SV key;
        uint32_t tag = 0, canon = 0;
    };
    struct Ref {
        SV key;
        uint64_t hash;
        uint32_t slot; // 2 * pair index + side, relative to its job
    };
    struct Shard {
        std::vector<Entry> table;
        size_t mask = 0;
        uint32_t base = 0;
    };
    std::array<Shard, SHARDS> shards;
    std::vector<SV> names;
    std::vector<uint64_t> hashes;
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
                    uint64_t hash = Hash{}(r[side]);
                    refs[job][hash & (SHARDS - 1)].push_back({r[side], hash, 2 * n + side});
                }
                ++n;
            });
            pairs[job + 1] = n;
        });
        for (size_t j = 0; j < jobs.size(); ++j)
            pairs[j + 1] += pairs[j];
        // Pass 2: intern each shard independently. Sides receive (shard, local ID).
        std::vector<uint64_t> sides(size_t(pairs.back()) * 2);
        std::array<std::vector<SV>, SHARDS> local_names;
        std::array<std::vector<uint64_t>, SHARDS> local_hashes;
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
            auto &hs = local_hashes[shard];
            for (size_t j = 0; j < refs.size(); ++j) {
                for (auto &r : refs[j][shard]) {
                    uint32_t tag = uint32_t(r.hash >> 32);
                    size_t i = index(r.hash, sh.mask);
                    while (!sh.table[i].key.empty() &&
                           (sh.table[i].tag != tag || sh.table[i].key != r.key))
                        i = (i + 1) & sh.mask;
                    auto &e = sh.table[i];
                    if (e.key.empty()) {
                        e = {r.key, tag, uint32_t(nm.size())};
                        nm.push_back(r.key);
                        hs.push_back(r.hash);
                    }
                    sides[size_t(pairs[j]) * 2 + r.slot] = uint64_t(shard) << 32 | e.canon;
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
        hashes.resize(total);
        parallel(SHARDS, threads, [&](size_t shard, size_t) {
            auto base = shards[shard].base;
            std::copy(local_names[shard].begin(), local_names[shard].end(), names.begin() + base);
            std::copy(local_hashes[shard].begin(), local_hashes[shard].end(), hashes.begin() + base);
            for (auto &e : shards[shard].table)
                if (!e.key.empty())
                    e.canon += base;
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
                    e.canon = best[parent[e.canon]].load(std::memory_order_relaxed);
        });
    }
    void prefetch(uint64_t hash) const {
        auto &sh = shards[hash & (SHARDS - 1)];
        if (!sh.table.empty())
            __builtin_prefetch(&sh.table[index(hash, sh.mask)]);
    }
    PersonRef person(SV s) const { return person(s, Hash{}(s)); }
    PersonRef person(SV s, uint64_t hash) const {
        auto &sh = shards[hash & (SHARDS - 1)];
        if (sh.table.empty())
            return {s, hash};
        uint32_t tag = uint32_t(hash >> 32);
        for (size_t i = index(hash, sh.mask);; i = (i + 1) & sh.mask) {
            auto &e = sh.table[i];
            if (e.key.empty())
                return {s, hash};
            if (e.tag == tag && e.key == s)
                return {names[e.canon], hashes[e.canon]};
        }
    }
};

struct Event {
    PersonRef person;
    I64 ts;
    int cents;
    uint32_t category;
};
struct RawEvent {
    SV id;
    I64 ingest;
    uint64_t hash;
    Event event;
};
struct Click {
    SV id;
    PersonRef person;
    uint64_t hash; // of id
    I64 ts;
    uint32_t campaign;
};
// After interning, each person's records are grouped by a counting sort and
// only the short per-person runs are ordered by time.
struct CompactEvent {
    I64 ts;
    int cents;
    uint32_t category;
};
struct CompactClick {
    SV id;
    I64 ts;
    uint32_t campaign;
};
// Shard-local person interning. Hashes were computed during parsing; full
// names are still compared whenever hashes match, so grouping stays exact.
class PersonTable {
    // Linked IDs share one canonical name, so equal pointers avoid a cold read.
    static bool same(SV a, SV b) {
        return a.size() == b.size() && (a.data() == b.data() || a == b);
    }
    struct Entry {
        uint64_t hash;
        SV name;
        uint32_t id;
    };
    std::vector<Entry> entries;
    std::vector<SV> names;
    size_t mask;
    size_t index(uint64_t hash) const { return (hash / SHARDS) & mask; }

  public:
    // Requests a record's slot and name bytes before intern() or find().
    void prefetch(const PersonRef &p) const {
        __builtin_prefetch(&entries[index(p.hash)]);
        __builtin_prefetch(p.name.data());
    }
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
            if (e.hash == p.hash && same(e.name, p.name))
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
            if (e.hash == p.hash && same(e.name, p.name))
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
    SV name(uint32_t id) const { return names[id]; }
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
    void push_back(const T &value) {
        if (used == capacity) {
            capacity = std::min<size_t>(size_t(256) << std::min<size_t>(blocks.size(), 6), 16384);
            T *block = static_cast<T *>(std::malloc(capacity * sizeof(T)));
            if (!block)
                throw std::bad_alloc();
            blocks.emplace_back(block);
            used = 0;
        }
        new (blocks.back().get() + used++) T(value);
        ++count;
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
    void release() {
        std::vector<std::unique_ptr<T, Free>>().swap(blocks);
        count = used = capacity = 0;
    }
};
template <class T> using Partitions = std::array<BlockList<T>, SHARDS>;
struct UserRow {
    SV person;
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
    properties.compression(parquet::Compression::SNAPPY);
    // Encode the columns of each row group concurrently; the file layout is unchanged.
    auto arrow_properties = parquet::ArrowWriterProperties::Builder().set_use_threads(true)->build();
    check(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), stream, 65536,
                                     properties.build(), arrow_properties));
    check(stream->Close());
}
// Rows from shards [first, last) form one Parquet part file.
static void write_users(const fs::path &path,
                        const std::array<std::vector<UserRow>, SHARDS> &results, size_t first,
                        size_t last, const std::vector<SV> &categories) {
    arrow::StringBuilder user, category;
    arrow::Date32Builder day;
    arrow::Int64Builder sessions, events, revenue;
    size_t n = 0, bytes = 0;
    for (size_t s = first; s < last; ++s) {
        n += results[s].size();
        for (auto &r : results[s])
            bytes += r.person.size();
    }
    check(user.Reserve(n));
    check(user.ReserveData(bytes));
    check(category.Reserve(n));
    check(day.Reserve(n));
    check(sessions.Reserve(n));
    check(events.Reserve(n));
    check(revenue.Reserve(n));
    for (size_t s = first; s < last; ++s)
        for (auto &r : results[s]) {
            user.UnsafeAppend(r.person.data(), r.person.size());
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
        }
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
        std::vector<Partitions<RawEvent>> parsed(threads);
        parallel(event_jobs.size(), threads, [&](size_t job, size_t worker) {
            batched_rows<7>(
                event_jobs[job].resolve(),
                [&](auto &r) {
                    uint64_t hash = Hash{}(r[1]);
                    people.prefetch(hash);
                    return hash;
                },
                [&](auto &r, uint64_t person_hash) {
                if (r[0].empty())
                    return;
                Event event{{}, BAD, 0, 0};
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
                        event = {people.person(r[1], person_hash), ts, cents, category};
                    }
                }
                // Invalid winners must remain present until global ID deduplication.
                uint64_t hash = Hash{}(r[0]);
                parsed[worker][hash & (SHARDS - 1)].push_back({r[0], ingest, hash, event});
            });
        });
        timing("parse_events");
        std::array<Partitions<Event>, SHARDS> event_parts;
        parallel(SHARDS, threads, [&](size_t shard, size_t) {
            size_t count = 0;
            for (auto &p : parsed)
                count += p[shard].size();
            DedupTable<RawEvent> unique(count);
            for (auto &p : parsed)
                unique.insert_all(p[shard], [&](const RawEvent &r) {
                    auto &old = unique.slot(r, r.hash);
                    if (!old || r.ingest < old->ingest)
                        old = &r;
                });
            unique.each([&](auto *r) {
                if (r->event.ts != BAD)
                    event_parts[shard][r->event.person.hash & (SHARDS - 1)].push_back(r->event);
            });
            // Winners were copied out, so this shard's raw rows can go now,
            // which lowers the peak while other shards are still deduplicated.
            for (auto &p : parsed)
                p[shard].release();
        });
        parsed.clear();
        parsed.shrink_to_fit();
        timing("dedup_events");
        std::vector<Partitions<Click>> parsed_clicks(threads);
        parallel(click_jobs.size(), threads, [&](size_t job, size_t worker) {
            batched_rows<4>(
                click_jobs[job].resolve(),
                [&](auto &r) {
                    uint64_t hash = Hash{}(r[1]);
                    people.prefetch(hash);
                    return hash;
                },
                [&](auto &r, uint64_t person_hash) {
                if (r[0].empty() || r[1].empty())
                    return;
                auto c = campaign_ids.find(r[2]);
                if (!c)
                    return;
                I64 ts = timestamp(r[3]);
                if (ts == BAD || ts < lo - REACH || ts >= hi)
                    return;
                // Click duplicates are identical after unquoting, so validation commutes.
                uint64_t hash = Hash{}(r[0]);
                parsed_clicks[worker][hash & (SHARDS - 1)].push_back(
                    {r[0], people.person(r[1], person_hash), hash, ts, *c});
            });
        });
        std::array<Partitions<Click>, SHARDS> click_parts;
        parallel(SHARDS, threads, [&](size_t shard, size_t) {
            size_t count = 0;
            for (auto &p : parsed_clicks)
                count += p[shard].size();
            DedupTable<Click> unique(count);
            for (auto &p : parsed_clicks)
                unique.insert_all(p[shard], [&](const Click &r) { unique.slot(r, r.hash) = &r; });
            unique.each([&](auto *c) {
                click_parts[shard][c->person.hash & (SHARDS - 1)].push_back(*c);
            });
            for (auto &p : parsed_clicks)
                p[shard].release();
        });
        parsed_clicks.clear();
        parsed_clicks.shrink_to_fit();
        timing("clicks");
        std::array<std::vector<UserRow>, SHARDS> user_rows;
        std::vector<CampaignAccumulator> campaign_rows;
        size_t aggregate_workers = std::min(SHARDS, threads);
        for (size_t i = 0; i < aggregate_workers; ++i)
            campaign_rows.emplace_back(campaigns.size(), floor_day(lo),
                                       floor_day(hi - 1) - floor_day(lo) + 1, aggregate_workers);
        parallel(SHARDS, threads, [&](size_t shard, size_t worker) {
            size_t ne = 0, nc = 0;
            for (size_t s = 0; s < SHARDS; ++s) {
                ne += event_parts[s][shard].size();
                nc += click_parts[s][shard].size();
            }
            // Intern persons, then group each person's records contiguously with
            // a counting sort. Output row order is irrelevant; only per-person
            // time order matters, so just the short runs are sorted.
            PersonTable people_here(ne / 8 + 16);
            std::vector<uint32_t> event_person(ne);
            {
                size_t k = 0;
                for (size_t s = 0; s < SHARDS; ++s)
                    each_ahead(
                        event_parts[s][shard], [&](const Event &e) { people_here.prefetch(e.person); },
                        [&](const Event &e) { event_person[k++] = people_here.intern(e.person); });
            }
            size_t np = people_here.size();
            std::vector<uint32_t> event_start(np + 1), click_start(np + 1);
            for (auto p : event_person)
                ++event_start[p + 1];
            for (size_t p = 0; p < np; ++p)
                event_start[p + 1] += event_start[p];
            std::vector<CompactEvent> events(ne);
            {
                std::vector<uint32_t> fill(event_start.begin(), event_start.end() - 1);
                size_t k = 0;
                for (size_t s = 0; s < SHARDS; ++s) {
                    auto &ev = event_parts[s][shard];
                    ev.each([&](const Event &e) {
                        events[fill[event_person[k++]]++] = {e.ts, e.cents, e.category};
                    });
                    ev.release();
                }
            }
            std::vector<uint32_t>().swap(event_person);
            std::vector<uint32_t> click_person(nc);
            {
                size_t k = 0;
                for (size_t s = 0; s < SHARDS; ++s)
                    each_ahead(
                        click_parts[s][shard], [&](const Click &c) { people_here.prefetch(c.person); },
                        [&](const Click &c) {
                        auto p = people_here.find(c.person);
                        click_person[k++] = p;
                        if (p != UINT32_MAX)
                            ++click_start[p + 1];
                    });
            }
            for (size_t p = 0; p < np; ++p)
                click_start[p + 1] += click_start[p];
            std::vector<CompactClick> clicks(click_start[np]);
            {
                std::vector<uint32_t> fill(click_start.begin(), click_start.end() - 1);
                size_t k = 0;
                for (size_t s = 0; s < SHARDS; ++s) {
                    auto &cl = click_parts[s][shard];
                    cl.each([&](const Click &c) {
                        auto p = click_person[k++];
                        if (p != UINT32_MAX)
                            clicks[fill[p]++] = {c.id, c.ts, c.campaign};
                    });
                    cl.release();
                }
            }
            std::vector<uint32_t>().swap(click_person);
            for (size_t p = 0; p < np; ++p) {
                auto first = events.begin() + event_start[p], last = events.begin() + event_start[p + 1];
                if (last - first > 1)
                    std::sort(first, last, [](auto &a, auto &b) { return a.ts < b.ts; });
                auto cf = clicks.begin() + click_start[p], cl = clicks.begin() + click_start[p + 1];
                if (cl - cf > 1)
                    std::sort(cf, cl, [](auto &a, auto &b) {
                        return a.ts != b.ts ? a.ts < b.ts : a.id > b.id;
                    });
            }
            auto &rows = user_rows[shard];
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
        std::vector<size_t> bounds{0};
        for (size_t s = 0, acc = 0; s < SHARDS; ++s) {
            acc += user_rows[s].size();
            if (bounds.size() < parts && acc * parts >= total_rows * bounds.size())
                bounds.push_back(s + 1);
        }
        bounds.resize(parts);
        bounds.push_back(SHARDS);
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
