// Writes a training dataset for profile-guided optimization (PGO) in the
// shape of the challenge's data: base-36 user/event/click IDs, about a fifth of
// fields quoted, timestamps 60% F1 / 10% F2 / 30% F3 with a few bad and late
// ones, duplicate copies with later ingest times, purchases/carts/views,
// Zipf-sized persons with several linked IDs, and a few malformed lines.
// It only has to make the compiler see realistic branches; it is not a
// reference and its tables are never checked.
//
// usage: training_gen OUT EVENTS [SEED]
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using I64 = int64_t;
static const I64 SECOND = 1000, MINUTE = 60 * SECOND, HOUR = 60 * MINUTE, DAY = 24 * HOUR;
static std::mt19937_64 rng;
static uint64_t below(uint64_t n) { return n ? rng() % n : 0; }
static bool chance(double p) { return std::uniform_real_distribution<double>(0, 1)(rng) < p; }

static std::string base36(uint64_t v) {
    const char *d = "0123456789abcdefghijklmnopqrstuvwxyz";
    std::string s;
    do {
        s.insert(s.begin(), d[v % 36]);
        v /= 36;
    } while (v);
    return s;
}
static uint64_t mix40(uint64_t v) {
    v = (v * 0x9E3779B97F4BULL) & ((1ULL << 40) - 1);
    v ^= v >> 17;
    return (v * 0xD6E8FEB86659ULL) & ((1ULL << 40) - 1);
}
static void civil(I64 ms, int &y, int &mo, int &d, int &h, int &mi, int &s) {
    I64 days = ms / DAY - (ms % DAY < 0), sod = (ms - days * DAY) / SECOND;
    I64 z = days + 719468, era = (z >= 0 ? z : z - 146096) / 146097;
    I64 doe = z - era * 146097, yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    I64 doy = doe - (365 * yoe + yoe / 4 - yoe / 100), mp = (5 * doy + 2) / 153;
    d = int(doy - (153 * mp + 2) / 5 + 1);
    mo = int(mp < 10 ? mp + 3 : mp - 9);
    y = int(yoe + era * 400 + (mo <= 2));
    h = int(sod / 3600), mi = int(sod / 60 % 60), s = int(sod % 60);
}
static std::string utc(I64 ms) {
    int y, mo, d, h, mi, s;
    civil(ms, y, mo, d, h, mi, s);
    char b[32];
    snprintf(b, sizeof b, "%04d-%02d-%02dT%02d:%02d:%02dZ", y, mo, d, h, mi, s);
    return b;
}
static std::string timestamp(I64 ms) {
    if (ms % SECOND || chance(0.3))
        return std::to_string(ms);
    if (chance(1.0 / 7)) {
        static const int offsets[] = {-600, -300, -60, 0, 60, 120, 330, 345, 540, 720};
        int off = offsets[below(10)];
        int y, mo, d, h, mi, s;
        civil(ms + off * MINUTE, y, mo, d, h, mi, s);
        char b[40];
        snprintf(b, sizeof b, "%04d-%02d-%02dT%02d:%02d:%02d%c%02d:%02d", y, mo, d, h, mi, s, off < 0 ? '-' : '+',
                 std::abs(off) / 60, std::abs(off) % 60);
        return b;
    }
    return utc(ms);
}
static std::string bad_timestamp(I64 ms) {
    std::string t = utc(ms - ((ms % SECOND) + SECOND) % SECOND);
    switch (below(6)) {
    case 0: return "";
    case 1: t.back() = 'z'; return t;
    case 2: t[10] = ' '; return t;
    case 3: return std::to_string(ms) + ".0";
    case 4: t.replace(5, 2, "13"); return t;
    default: return t.substr(0, 19);
    }
}
static void put(std::string &line, const std::string &field) {
    if (!line.empty())
        line += ',';
    if (chance(0.2))
        line += '"' + field + '"';
    else
        line += field;
}
// Lines are collected, then shuffled and written, like the real files.
static void emit(std::vector<std::string> &out, std::string line) {
    if (chance(0.0005)) {
        switch (below(4)) {
        case 0: line += ",x"; break;
        case 1: line.insert(line.begin(), '"'); break;
        case 2: line = line.substr(0, line.rfind(',')); break;
        default: out.emplace_back();
        }
    }
    out.push_back(std::move(line));
}
static void write(const fs::path &path, const char *header, std::vector<std::string> &lines) {
    std::shuffle(lines.begin(), lines.end(), rng);
    FILE *f = fopen(path.c_str(), "w");
    fputs(header, f);
    for (auto &l : lines) {
        fputs(l.c_str(), f);
        fputc('\n', f);
    }
    fclose(f);
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: training_gen OUT EVENTS [SEED]\n");
        return 2;
    }
    fs::path out = argv[1];
    I64 events = atoll(argv[2]);
    rng.seed(argc > 3 ? strtoull(argv[3], nullptr, 10) : 7);
    for (auto d : {"events", "clicks", "links"})
        fs::create_directories(out / d);
    const I64 lo = 1811808000000LL; // 2027-06-01T00:00:00Z
    const I64 hi = lo + 14 * DAY;
    FILE *f = fopen((out / "manifest.json").c_str(), "w");
    fprintf(f, "{\n  \"window_start\": \"%s\",\n  \"window_end\": \"%s\"\n}\n", utc(lo).c_str(), utc(hi).c_str());
    fclose(f);
    const char *categories[] = {"apparel", "books", "electronics", "garden", "home", "music", "shoes", "toys"};
    const int products = 50000, campaigns = 2000;
    std::vector<int> price(products + 1);
    f = fopen((out / "products.csv").c_str(), "w");
    fputs("product_id,category\n", f);
    for (int p = 1; p <= products; ++p) {
        price[p] = 100 + int(below(400)) * int(1 + below(20));
        fprintf(f, "p%d,%s\n", p, categories[below(8)]);
    }
    fclose(f);
    const char *channels[] = {"affiliate", "display", "email", "search", "social"};
    f = fopen((out / "campaigns.csv").c_str(), "w");
    fputs("campaign_id,channel\n", f);
    for (int c = 1; c <= campaigns; ++c)
        fprintf(f, "c_%d,%s\n", c, channels[below(5)]);
    fclose(f);

    std::vector<std::string> ev, cl, ln;
    I64 event_index = 0, click_index = 0, person = 0;
    while (event_index < events) {
        // Zipf-like person sizes, a few aliases each, linked to earlier ones.
        I64 n = std::max<I64>(1, I64(8.0 / std::pow(std::uniform_real_distribution<double>(1e-4, 1)(rng), 0.75)));
        n = std::min<I64>(n, events - event_index);
        int aliases = chance(0.45) ? 1 : chance(0.55) ? 2 : chance(0.5) ? 3 : 4 + int(below(5));
        std::vector<std::string> ids;
        for (int a = 0; a < aliases; ++a)
            ids.push_back("u" + base36(mix40(uint64_t(person) * 512 + a)));
        for (int a = 1; a < aliases; ++a) {
            std::string line;
            put(line, ids[a]);
            put(line, ids[below(a)]);
            emit(ln, line);
        }
        ++person;
        I64 t = lo + I64(below(14 * 12)) * 2 * HOUR + I64(below(25 * 60)) * SECOND;
        std::vector<I64> starts{t};
        for (I64 k = 0; k < n; ++k, ++event_index) {
            if (k && chance(0.2)) {
                t += 2 * HOUR + I64(below(10 * HOUR));
                if (t >= hi - HOUR) // keep long-lived persons inside the window
                    t = lo + I64(below(14 * DAY / HOUR - 2)) * HOUR;
                starts.push_back(t);
            } else
                t += I64(below(240)) * SECOND + (chance(0.03) ? I64(below(999)) : 0);
            const std::string &user = ids[aliases == 1 || chance(0.5) ? 0 : below(aliases)];
            int kind = int(below(100));
            int product = 1 + int(std::min<I64>(products - 1, I64(std::pow(below(products) + 1.0, 0.9))));
            I64 ingest = (t / SECOND + 1) * SECOND + I64(below(600)) * SECOND;
            if (chance(0.01))
                ingest += DAY + I64(below(4 * DAY / SECOND)) * SECOND;
            std::string fields[7] = {
                "e" + base36(mix40(uint64_t(event_index) ^ 0x5555)), chance(0.002) ? "" : user,
                chance(0.005) ? bad_timestamp(t) : timestamp(t), utc(ingest),
                chance(0.001) ? "View" : kind < 3 ? "purchase" : kind < 13 ? "cart" : "view",
                chance(0.005) ? "p" + std::to_string(products + 1 + below(9999)) : "p" + std::to_string(product),
                kind < 3 ? (chance(0.05) ? "19.99" : std::to_string(price[product])) : chance(0.3) ? std::to_string(price[product]) : ""};
            int copies = chance(0.03) ? 1 + int(below(3)) : 0;
            for (int c = 0; c <= copies; ++c) {
                if (c)
                    fields[3] = utc(ingest + I64(1 + below(600)) * SECOND);
                std::string line;
                for (auto &x : fields)
                    put(line, x);
                emit(ev, line);
            }
        }
        if (chance(0.65)) {
            I64 clicks = std::max<I64>(1, n / 4);
            for (I64 k = 0; k < clicks; ++k, ++click_index) {
                I64 start = starts[below(starts.size())];
                I64 ts = start - std::min<I64>(7 * DAY + (chance(0.1) ? DAY : 0), I64(below(4 * DAY / SECOND)) * SECOND);
                std::string fields[4] = {"k" + base36(mix40(uint64_t(click_index) ^ 0xAAAA)),
                                         ids[aliases == 1 || chance(0.5) ? 0 : below(aliases)],
                                         chance(0.005) ? "c_" + std::to_string(campaigns + 1 + below(999))
                                                       : "c_" + std::to_string(1 + below(campaigns)),
                                         chance(0.005) ? bad_timestamp(ts) : timestamp(ts)};
                std::string line;
                for (auto &x : fields)
                    put(line, x);
                int copies = chance(0.02) ? 1 : 0;
                for (int c = 0; c <= copies; ++c)
                    emit(cl, line);
            }
        }
    }
    write(out / "events/part-00000.csv", "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n", ev);
    write(out / "clicks/part-00000.csv", "click_id,user_id,campaign_id,ts\n", cl);
    write(out / "links/part-00000.csv", "user_id_a,user_id_b\n", ln);
    return 0;
}
