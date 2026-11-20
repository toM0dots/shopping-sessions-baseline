// Native Shopping Sessions pipeline. Input-backed string views remain alive
// until the Parquet writers finish; no reference answers or cached results.
#include <algorithm>
#include <array>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <fcntl.h>
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
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using SV = std::string_view;
using I64 = int64_t;
constexpr I64 DAY = 86400000, REACH = 7 * DAY;
constexpr I64 BAD = std::numeric_limits<I64>::min();
constexpr size_t SHARDS = 64;

static uint64_t mix(uint64_t h) {
    h ^= h >> 30;
    h *= 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 27;
    h *= 0x94d049bb133111ebULL;
    return h ^ (h >> 31);
}
struct Hash {
    size_t operator()(SV s) const {
        uint64_t h = s.size(), v;
        while (s.size() >= 8) {
            std::memcpy(&v, s.data(), 8);
            h = mix(h ^ v);
            s.remove_prefix(8);
        }
        v = 0;
        if (!s.empty())
            std::memcpy(&v, s.data(), s.size());
        return mix(h ^ v);
    }
};
template <class T> using Map = std::unordered_map<SV, T, Hash>;

// No erases or owned key allocations. Empty keys are excluded by CSV semantics.
template <class T> class FlatMap {
    struct Entry {
        SV key;
        T value{};
    };
    std::vector<Entry> entries;

  public:
    explicit FlatMap(size_t n) {
        size_t cap = 8;
        while (cap < n * 2)
            cap *= 2;
        entries.resize(cap);
    }
    T &slot(SV key) {
        // Sharding already consumed the low six bits. Reusing them here would
        // cluster every key in the shard into 1/64 of the initial buckets.
        size_t i = (Hash{}(key) / SHARDS) & (entries.size() - 1);
        while (!entries[i].key.empty() && entries[i].key != key)
            i = (i + 1) & (entries.size() - 1);
        entries[i].key = key;
        return entries[i].value;
    }
    template <class F> void each(F f) {
        for (auto &e : entries)
            if (!e.key.empty())
                f(e.value);
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
    if (s.size() != 20 && s.size() != 25) {
        I64 n = 0;
        for (unsigned char c : s) {
            if (c < '0' || c > '9' || n > (std::numeric_limits<I64>::max() - (c - '0')) / 10)
                return BAD;
            n = n * 10 + c - '0';
        }
        return n;
    }
    // A 20/25-character all-digit timestamp can have arbitrarily many zeroes.
    if (s[4] != '-') {
        I64 n = 0;
        for (unsigned char c : s) {
            if (c < '0' || c > '9' || n > (std::numeric_limits<I64>::max() - (c - '0')) / 10)
                return BAD;
            n = n * 10 + c - '0';
        }
        return n;
    }
    if (s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':')
        return BAD;
    auto number = [&](int at, int count) {
        int n = 0;
        for (int i = 0; i < count; ++i) {
            unsigned char c = s[at + i];
            if (c < '0' || c > '9')
                return -1;
            n = n * 10 + c - '0';
        }
        return n;
    };
    int y = number(0, 4), m = number(5, 2), d = number(8, 2), h = number(11, 2), mi = number(14, 2),
        sec = number(17, 2);
    static const int md[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (y < 1 || m < 1 || m > 12 || d < 1 || h < 0 || h > 23 || mi < 0 || mi > 59 || sec < 0 ||
        sec > 59)
        return BAD;
    if (d > md[m] + (m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)))
        return BAD;
    I64 t = days_from_civil(y, m, d) * DAY + (h * 3600 + mi * 60 + sec) * 1000LL;
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

struct People {
    Map<uint32_t> ids;
    std::vector<uint32_t> parent, size;
    std::vector<SV> names;
    uint32_t id(SV s) {
        auto [it, new_id] = ids.emplace(s, parent.size());
        if (new_id) {
            parent.push_back(it->second);
            size.push_back(1);
            names.push_back(s);
        }
        return it->second;
    }
    uint32_t find(uint32_t i) {
        while (parent[i] != i) {
            parent[i] = parent[parent[i]];
            i = parent[i];
        }
        return i;
    }
    void join(SV a, SV b) {
        auto x = find(id(a)), y = find(id(b));
        if (x == y)
            return;
        if (size[x] < size[y])
            std::swap(x, y);
        parent[y] = x;
        size[x] += size[y];
        names[x] = std::min(names[x], names[y]);
    }
    void freeze() {
        for (auto &[key, i] : ids)
            i = find(i);
    }
    SV person(SV s) const {
        auto it = ids.find(s);
        return it == ids.end() ? s : names[it->second];
    }
};

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
static std::vector<SV> chunks(const std::vector<SV> &files) {
    std::vector<SV> jobs;
    for (SV text : files) {
        auto p = text.find('\n');
        if (p == SV::npos)
            continue;
        text.remove_prefix(p + 1);
        while (!text.empty()) {
            size_t n = std::min<size_t>(text.size(), 1 << 20);
            if (n < text.size()) {
                auto end = text.find('\n', n);
                n = end == SV::npos ? text.size() : end + 1;
            }
            jobs.push_back(text.substr(0, n));
            text.remove_prefix(n);
        }
    }
    return jobs;
}
struct Event {
    SV person;
    I64 ts;
    int cents;
    uint32_t category;
};
struct RawEvent {
    SV id, ingest;
    Event event;
};
struct Click {
    SV id, person;
    I64 ts;
    uint32_t campaign;
};
template <class T> using Partitions = std::array<std::vector<T>, SHARDS>;
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
    check(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), stream, 65536,
                                     properties.build()));
    check(stream->Close());
}
static void write_users(const fs::path &out,
                        const std::array<std::vector<UserRow>, SHARDS> &results,
                        const std::vector<SV> &categories) {
    arrow::StringBuilder user, category;
    arrow::Date32Builder day;
    arrow::Int64Builder sessions, events, revenue;
    size_t n = 0;
    for (auto &part : results)
        n += part.size();
    check(user.Reserve(n));
    check(category.Reserve(n));
    check(day.Reserve(n));
    check(sessions.Reserve(n));
    check(events.Reserve(n));
    check(revenue.Reserve(n));
    for (auto &part : results)
        for (auto &r : part) {
            check(user.Append(r.person.data(), r.person.size()));
            check(day.Append(r.day));
            check(sessions.Append(r.sessions));
            check(events.Append(r.events));
            check(revenue.Append(r.revenue));
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
    write_table(out / "user_daily" / "part-0.parquet",
                arrow::Table::Make(schema, {finish(user), finish(day), finish(sessions),
                                            finish(events), finish(revenue), finish(category)}));
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

int main() {
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
        People people;
        for (SV file : inputs.directory(data / "links"))
            rows<2>(file, true, [&](auto &r) {
                if (!r[0].empty() && !r[1].empty())
                    people.join(r[0], r[1]);
            });
        people.freeze();
        std::vector<SV> categories{"unknown"};
        Map<uint32_t> cat_ids{{"unknown", 0}}, products;
        rows<2>(inputs.file(data / "products.csv"), true, [&](auto &r) {
            auto [it, added] = cat_ids.emplace(r[1], categories.size());
            if (added)
                categories.push_back(r[1]);
            products[r[0]] = it->second;
        });
        std::vector<std::pair<SV, SV>> campaigns{{"organic", "organic"}};
        Map<uint32_t> campaign_ids;
        rows<2>(inputs.file(data / "campaigns.csv"), true, [&](auto &r) {
            campaign_ids[r[0]] = campaigns.size();
            campaigns.emplace_back(r[0], r[1]);
        });
        auto event_jobs = chunks(inputs.directory(data / "events"));
        auto click_jobs = chunks(inputs.directory(data / "clicks"));
        timing("setup");
        std::vector<Partitions<RawEvent>> parsed(threads);
        parallel(event_jobs.size(), threads, [&](size_t job, size_t worker) {
            rows<7>(event_jobs[job], false, [&](auto &r) {
                if (r[0].empty())
                    return;
                Event event{{}, BAD, 0, 0};
                bool valid = !r[1].empty() && !r[5].empty() &&
                             (r[4] == "view" || r[4] == "cart" || r[4] == "purchase");
                I64 ts = valid ? timestamp(r[2]) : BAD;
                if (ts != BAD && ts >= lo && ts < hi) {
                    I64 ingest = timestamp(r[3]);
                    int cents = r[4] == "purchase" ? price(r[6]) : 0;
                    if (ingest != BAD && ingest - ts <= DAY && (r[4] != "purchase" || cents)) {
                        // Categories only contribute for purchases. Avoid a dictionary
                        // lookup for the much larger view/cart population.
                        uint32_t category = 0;
                        if (cents) {
                            auto product = products.find(r[5]);
                            if (product != products.end())
                                category = product->second;
                        }
                        event = {people.person(r[1]), ts, cents, category};
                    }
                }
                // Invalid winners must remain present until global ID deduplication.
                parsed[worker][Hash{}(r[0]) & (SHARDS - 1)].push_back({r[0], r[3], event});
            });
        });
        timing("parse_events");
        std::array<Partitions<Event>, SHARDS> event_parts;
        parallel(SHARDS, threads, [&](size_t shard, size_t) {
            size_t count = 0;
            for (auto &p : parsed)
                count += p[shard].size();
            FlatMap<const RawEvent *> unique(count);
            for (auto &p : parsed)
                for (auto &r : p[shard]) {
                    auto &old = unique.slot(r.id);
                    if (!old || r.ingest < old->ingest)
                        old = &r;
                }
            unique.each([&](auto *r) {
                if (r->event.ts != BAD)
                    event_parts[shard][Hash{}(r->event.person) & (SHARDS - 1)].push_back(r->event);
            });
        });
        parsed.clear();
        timing("dedup_events");
        std::vector<Partitions<Click>> parsed_clicks(threads);
        parallel(click_jobs.size(), threads, [&](size_t job, size_t worker) {
            rows<4>(click_jobs[job], false, [&](auto &r) {
                if (r[0].empty() || r[1].empty())
                    return;
                auto c = campaign_ids.find(r[2]);
                if (c == campaign_ids.end())
                    return;
                I64 ts = timestamp(r[3]);
                if (ts == BAD || ts < lo - REACH || ts >= hi)
                    return;
                // Click duplicates are identical after unquoting, so validation commutes.
                parsed_clicks[worker][Hash{}(r[0]) & (SHARDS - 1)].push_back(
                    {r[0], people.person(r[1]), ts, c->second});
            });
        });
        std::array<Partitions<Click>, SHARDS> click_parts;
        parallel(SHARDS, threads, [&](size_t shard, size_t) {
            size_t count = 0;
            for (auto &p : parsed_clicks)
                count += p[shard].size();
            FlatMap<const Click *> unique(count);
            for (auto &p : parsed_clicks)
                for (auto &r : p[shard])
                    unique.slot(r.id) = &r;
            unique.each([&](auto *c) {
                click_parts[shard][Hash{}(c->person) & (SHARDS - 1)].push_back(*c);
            });
        });
        parsed_clicks.clear();
        timing("clicks");
        std::array<std::vector<UserRow>, SHARDS> user_rows;
        std::array<CampaignDaily, SHARDS> campaign_rows;
        parallel(SHARDS, threads, [&](size_t shard, size_t) {
            std::vector<Event> events;
            std::vector<Click> clicks;
            size_t ne = 0, nc = 0;
            for (size_t s = 0; s < SHARDS; ++s) {
                ne += event_parts[s][shard].size();
                nc += click_parts[s][shard].size();
            }
            events.reserve(ne);
            clicks.reserve(nc);
            for (size_t s = 0; s < SHARDS; ++s) {
                auto &ev = event_parts[s][shard];
                events.insert(events.end(), ev.begin(), ev.end());
                std::vector<Event>().swap(ev);
                auto &cl = click_parts[s][shard];
                clicks.insert(clicks.end(), cl.begin(), cl.end());
                std::vector<Click>().swap(cl);
            }
            std::sort(events.begin(), events.end(), [](auto &a, auto &b) {
                return a.person != b.person ? a.person < b.person : a.ts < b.ts;
            });
            std::sort(clicks.begin(), clicks.end(), [](auto &a, auto &b) {
                return a.person != b.person ? a.person < b.person
                                            : (a.ts != b.ts ? a.ts < b.ts : a.id > b.id);
            });
            auto &rows = user_rows[shard];
            auto &daily = campaign_rows[shard];
            std::vector<I64> category_revenue(categories.size());
            std::vector<uint32_t> active_counts(campaigns.size()), positions(campaigns.size()),
                active;
            size_t at = 0, ci = 0;
            while (at < events.size()) {
                SV person = events[at].person;
                size_t end = at + 1;
                while (end < events.size() && events[end].person == person)
                    ++end;
                while (ci < clicks.size() && clicks[ci].person < person)
                    ++ci;
                size_t ce = ci;
                while (ce < clicks.size() && clicks[ce].person == person)
                    ++ce;
                for (auto c : active)
                    active_counts[c] = 0;
                active.clear();
                size_t left = ci, right = ci;
                while (at < end) {
                    int32_t day = floor_day(events[at].ts);
                    UserRow row{person, day, 0, 0, 0, -1};
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
                        auto &stat = daily[campaign_key(credit, day)];
                        ++stat.sessions;
                        stat.purchasing += rev > 0;
                        stat.revenue += rev;
                        for (auto c : active)
                            ++daily[campaign_key(c, day)].assists;
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
                ci = ce;
            }
        });
        timing("sessionize_aggregate");
        CampaignDaily daily;
        for (auto &part : campaign_rows)
            for (auto &[key, r] : part)
                daily[key] += r;
        parallel(2, std::min<size_t>(threads, 2), [&](size_t i, size_t) {
            if (i == 0)
                write_users(out, user_rows, categories);
            else
                write_campaigns(out, daily, campaigns);
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
