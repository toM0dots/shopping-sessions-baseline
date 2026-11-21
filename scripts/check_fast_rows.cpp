// Differential test: fast_rows (AVX-512 when available) against rows(), the
// reference splitter, on random chunks with quotes, CRs, empty lines, wrong
// field counts, long lines and missing final LFs.
// Build: c++ -O2 -std=c++17 -pthread -I<arrow include> scripts/check_fast_rows.cpp \
//        <libparquet> <libarrow> -o build/check_fast_rows
#define main native_main
#include "../pipeline/native.cpp"
#undef main
#include <random>
template <size_t N> long check(std::mt19937_64 &r, long rounds) {
    long bad = 0;
    for (long t = 0; t < rounds; ++t) {
        std::string text;
        int lines = r() % 40;
        for (int l = 0; l < lines; ++l) {
            int kind = r() % 10;
            size_t fields = kind == 0 ? r() % (N + 3) : N;
            for (size_t i = 0; i < fields; ++i) {
                if (i) text += ',';
                int w = r() % 12 == 0 ? 40 + r() % 90 : r() % 14;
                std::string v;
                for (int k = 0; k < w; ++k) v += "abcxyz0189 \r\t"[r() % 13];
                switch (r() % 12) {
                case 0: text += '"' + v + '"'; break;
                case 1: text += '"' + v; break;
                case 2: text += v + '"'; break;
                case 3: text += '"' + v.substr(0, v.size() / 2) + '"' + v.substr(v.size() / 2) + '"'; break;
                case 4: text += "\""; break;
                case 5: text += "\"\""; break;
                default: text += v;
                }
            }
            if (!(l == lines - 1 && r() % 3 == 0)) text += '\n';
            if (r() % 25 == 0) text += '\n';
        }
        std::vector<std::vector<std::string>> a, b;
        auto ga = [&](auto &f) { a.emplace_back(f.begin(), f.end()); };
        auto gb = [&](auto &f) { b.emplace_back(f.begin(), f.end()); };
        // Copy into a buffer with slack so over-reads stay inside our memory, like the mapped input.
        std::string buf = text + std::string(256, 'Q');
        SV view(buf.data(), text.size());
        rows<N>(view, false, ga);
        fast_rows<N>(view, gb);
        if (a != b) { if (bad++ < 3) printf("N=%zu mismatch on %zu bytes: %zu vs %zu rows\n", N, text.size(), a.size(), b.size()); }
    }
    return bad;
}
int main() {
    printf("avx512 path active: %d\n", int(has_avx512()));
    std::mt19937_64 r(11);
    long bad = check<7>(r, 40000) + check<4>(r, 20000) + check<2>(r, 20000);
    printf("%ld mismatches\n", bad);
    return bad != 0;
}
