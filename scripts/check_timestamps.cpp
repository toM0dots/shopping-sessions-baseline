// Differential test: timestamp() and its F1/F3 fast paths against
// timestamp_general() on valid times, one-character mutations and random
// out-of-range fields. Build like scripts/check_fast_rows.cpp.
#define main native_main
#include "../pipeline/native.cpp"
#undef main
#include <random>
int main() {
    std::mt19937_64 r(3);
    long n = 0, bad = 0;
    const char *alpha = "0123456789-:TZ+ zt.e";
    auto check = [&](const std::string &t) {
        ++n;
        I64 a = timestamp(t), b = timestamp_general(t);
        if (a != b && bad++ < 10) printf("mismatch '%s': %lld vs %lld\n", t.c_str(), (long long)a, (long long)b);
    };
    for (int i = 0; i < 3000000; ++i) {
        I64 ms = I64(r() % 4000000000000ULL) - 300000000000LL;
        int y, mo, d, h, mi, sec; // valid F1 and F3 from a time
        I64 days = ms / 86400000 - (ms % 86400000 < 0), sod = (ms - days * 86400000) / 1000;
        I64 z = days + 719468, era = (z >= 0 ? z : z - 146096) / 146097, doe = z - era * 146097;
        I64 yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365, doy = doe - (365 * yoe + yoe / 4 - yoe / 100), mp = (5 * doy + 2) / 153;
        d = int(doy - (153 * mp + 2) / 5 + 1); mo = int(mp < 10 ? mp + 3 : mp - 9); y = int(yoe + era * 400 + (mo <= 2));
        h = int(sod / 3600); mi = int(sod / 60 % 60); sec = int(sod % 60);
        char buf[64];
        snprintf(buf, sizeof buf, "%04d-%02d-%02dT%02d:%02d:%02dZ", y % 10000, mo, d, h, mi, sec);
        std::string f1 = buf, f3 = std::to_string(ms < 0 ? -ms : ms);
        check(f1); check(f3);
        // mutations: one random character changed, or random digits/dates out of range
        for (std::string t : {f1, f3}) { t[r() % t.size()] = alpha[r() % 20]; check(t); }
        snprintf(buf, sizeof buf, "%04d-%02d-%02dT%02d:%02d:%02dZ", int(r() % 10000), int(r() % 15), int(r() % 34), int(r() % 26), int(r() % 62), int(r() % 62));
        check(buf);
        std::string digits13; for (int k = 0; k < 13; ++k) digits13 += char('0' + r() % 10); check(digits13);
    }
    printf("%ld strings, %ld mismatches\n", n, bad);
    return bad != 0;
}
