// Synthetic surrogate for the Shopping Sessions generator (format only; not official).
// usage: surrogate_gen OUT EVENTS CLICKS PARTS SEED  (env LINKS_PER_100_USERS, default 25; LINK_STYLE=anon)
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>
#include <random>
#include <thread>
#include <filesystem>
#include <ctime>
namespace fs = std::filesystem;
using I64 = int64_t;
const I64 DAY = 86400000, LO = 1790812800000LL; // 2026-10-01
static std::string iso(I64 t, std::mt19937_64 &r) {
    int kind = r() % 10;
    if (kind == 0) return std::to_string(t);
    time_t s = t / 1000; struct tm tm; gmtime_r(&s, &tm);
    char b[64];
    if (kind == 1) {
        int oh = r() % 12, sign = r() % 2;
        time_t s2 = s + (sign ? 1 : -1) * oh * 3600; gmtime_r(&s2, &tm);
        snprintf(b, sizeof b, "%04d-%02d-%02dT%02d:%02d:%02d%c%02d:00", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec, sign ? '+' : '-', oh);
        return b;
    }
    snprintf(b, sizeof b, "%04d-%02d-%02dT%02d:%02d:%02dZ", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
             tm.tm_min, tm.tm_sec);
    return b;
}
int main(int argc, char **argv) {
    fs::path out = argv[1];
    I64 events = atoll(argv[2]), clicks = atoll(argv[3]);
    int parts = atoi(argv[4]); uint64_t seed = atoll(argv[5]);
    bool anon_style = getenv("LINK_STYLE") && std::string(getenv("LINK_STYLE")) == "anon"; I64 link_rate = getenv("LINKS_PER_100_USERS") ? atoll(getenv("LINKS_PER_100_USERS")) : 25; I64 users = events / 20, products = 50000, campaigns = 2000;
    for (auto d : {"events", "clicks", "links"}) fs::create_directories(out / d);
    FILE *f = fopen((out / "manifest.json").c_str(), "w");
    fprintf(f, "{\n  \"window_start\": \"2026-10-01T00:00:00Z\",\n  \"window_end\": \"2026-10-15T00:00:00Z\"\n}\n");
    fclose(f);
    const char *cats[] = {"shoes", "books", "electronics", "toys", "garden", "beauty", "sports", "grocery", "music", "home"};
    f = fopen((out / "products.csv").c_str(), "w"); fprintf(f, "product_id,category\n");
    for (I64 i = 0; i < products; ++i) fprintf(f, "p_%06lld,%s\n", (long long)i, cats[(i * 7919) % 10]);
    fclose(f);
    const char *ch[] = {"search", "social", "email", "display", "affiliate"};
    f = fopen((out / "campaigns.csv").c_str(), "w"); fprintf(f, "campaign_id,channel\n");
    for (I64 i = 0; i < campaigns; ++i) fprintf(f, "c_%05lld,%s\n", (long long)i, ch[i % 5]);
    fclose(f);
    auto uid = [](I64 u) { char b[32]; snprintf(b, sizeof b, "u_%08llx", (unsigned long long)(u * 2654435761ULL % 0xffffffffULL)); return std::string(b); };
    std::vector<std::thread> pool;
    for (int p = 0; p < parts; ++p) pool.emplace_back([&, p] {
        std::mt19937_64 r(seed * 1000 + p);
        char name[64]; snprintf(name, sizeof name, "part-%05d.csv", p);
        FILE *f = fopen((out / "events" / name).c_str(), "w");
        setvbuf(f, nullptr, _IOFBF, 1 << 20);
        fprintf(f, "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n");
        I64 n = events / parts, written = 0, eid = p * n;
        std::string prev;
        while (written < n) {
            I64 u = r() % users; std::string user = uid(u);
            I64 t = LO + (I64)(r() % (14 * DAY));
            int len = 1 + r() % 12;
            for (int k = 0; k < len && written < n; ++k, ++written) {
                t += r() % 600000;
                if (r() % 25 == 0) t += 3600000;
                int type = r() % 20; const char *ty = type < 14 ? "view" : type < 18 ? "cart" : "purchase";
                char price[16] = "";
                if (type >= 18) snprintf(price, sizeof price, "%d", 100 + (int)(r() % 50000));
                I64 ing = t + (r() % 100 == 0 ? 2 * DAY : r() % 600000);
                char id[48]; snprintf(id, sizeof id, "evt_%016llx", (unsigned long long)((++eid) * 0x9E3779B97F4A7C15ULL));
                std::string line = std::string(id) + "," + user + "," + iso(t, r) + "," + iso(ing, r) + "," + ty + ",p_" +
                                   std::to_string(100000 + r() % (products + 500)).substr(1) + "," + price + "\n";
                if (r() % 200 == 0) line = "bad,row\n";
                fputs(line.c_str(), f);
                if (r() % 50 == 0) { // duplicate
                    std::string d = std::string(id) + "," + user + "," + iso(t, r) + "," + iso(ing + 1000, r) + "," + ty + ",p_1," + price + "\n";
                    fputs(d.c_str(), f);
                }
            }
        }
        fclose(f);
        snprintf(name, sizeof name, "part-%05d.csv", p);
        f = fopen((out / "clicks" / name).c_str(), "w"); setvbuf(f, nullptr, _IOFBF, 1 << 20);
        fprintf(f, "click_id,user_id,campaign_id,ts\n");
        I64 cn = clicks / parts;
        for (I64 i = 0; i < cn; ++i) {
            I64 t = LO - 7 * DAY + (I64)(r() % (21 * DAY));
            char id[48]; snprintf(id, sizeof id, "clk_%016llx", (unsigned long long)((p * cn + i + 1) * 0xC2B2AE3D27D4EB4FULL));
            char line[160]; snprintf(line, sizeof line, "%s,%s,c_%05lld,%s\n", id, uid(r() % users).c_str(), (long long)(r() % (campaigns + 20)), iso(t, r).c_str());
            fputs(line, f);
            if (r() % 50 == 0) fputs(line, f);
        }
        fclose(f);
        f = fopen((out / "links" / name).c_str(), "w"); fprintf(f, "user_id_a,user_id_b\n");
        for (I64 i = 0; i < users * link_rate / 100 / parts; ++i) {
            I64 a = r() % users;
            // LINK_STYLE=anon: two thirds user-user links, one third anonymous IDs.
            // Default: mostly user-device links, one in ten user-user.
            std::string b = anon_style ? (r() % 3 ? uid(r() % users) : "anon_" + std::to_string(r() % (users * 4)))
                                       : (r() % 10 == 0 ? uid(r() % users) : "dev_" + std::to_string(p) + "_" + std::to_string(i));
            fprintf(f, "%s,%s\n", uid(a).c_str(), b.c_str());
        }
        fclose(f);
    });
    for (auto &t : pool) t.join();
}
