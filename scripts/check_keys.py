#!/usr/bin/env python3
"""Check native packed/fallback key semantics against exact byte strings.

Requires a C++17 compiler; no Arrow libraries, reference Git commit or generated
dataset is needed. Extracts Key and its supporting hash/storage code from the
current source. ASan and UBSan are enabled by default. Run again with --scalar
to exercise the non-SSE2 encoder. Cases use a fixed seed and cover every prefix
byte, leading zeroes, all suffix byte values, long fallback IDs, equal-key hash
consistency, byte-wise ordering, and input buffers ending at a protected page.
"""
import argparse
import os
from pathlib import Path
import shlex
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--candidate", type=Path)
parser.add_argument("--compiler", default=os.environ.get("CXX", "c++"))
parser.add_argument("--scalar", action="store_true", help="Exercise the non-SSE2 fallback")
parser.add_argument("--no-sanitizers", action="store_true")
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
candidate = (args.candidate or root / "pipeline/native.cpp").read_text()
start = candidate.index("static inline uint64_t mum(")
end_marker = "static_assert(sizeof(Key) == 8);"
end = candidate.index(end_marker, start) + len(end_marker)
candidate = candidate[start:end]

HEADER = r'''
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#if defined(__SSE2__)
#include <emmintrin.h>
#endif
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>
using SV = std::string_view;
'''
CASES = r'''
static void require(bool condition, const char *property, size_t trial) {
    if (!condition) {
        std::cerr << "Key mismatch: " << property << " case=" << trial << '\n';
        std::exit(1);
    }
}
static bool byte_less(const std::string &a, const std::string &b) {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end(),
        [](char x, char y) { return uint8_t(x) < uint8_t(y); });
}
static bool packable(SV s) {
    if (s.size() < 2 || s.size() > 9)
        return false;
    for (size_t i = 1; i < s.size(); ++i) {
        unsigned char c = s[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')))
            return false;
    }
    return true;
}
static native::Key check_key(SV text, size_t trial) {
    auto key = native::Key::of(text);
    auto same = native::Key::of(text);
    char decoded[16];
    require(key.text(decoded) == text, "round trip", trial);
    require(key.size() == text.size(), "length", trial);
    require(key.packed() == packable(text), "packing domain", trial);
    require(key == same && !(key != same), "equal copies", trial);
    require(key.hash() == same.hash(), "equal copies hash equally", trial);
    return key;
}
int main() {
    native::Key empty;
    char empty_text[16];
    require(empty.empty() && empty.size() == 0 && empty.text(empty_text).empty(),
            "empty sentinel", 0);
    std::vector<std::string> strings;
    const std::string alphabet = "0123456789abcdefghijklmnopqrstuvwxyz";
    for (unsigned prefix = 0; prefix < 256; ++prefix) {
        std::string first(1, char(prefix));
        strings.push_back(first);
        for (char c : alphabet)
            strings.push_back(first + c);
        for (size_t length = 1; length <= 8; ++length) {
            strings.push_back(first + std::string(length, '0'));
            strings.push_back(first + std::string(length, 'z'));
            strings.push_back(first + std::string(length - 1, '0') + '1');
            strings.push_back(first + '1' + std::string(length - 1, '0'));
            std::string mixed;
            for (size_t i = 0; i < length; ++i)
                mixed.push_back(alphabet[(prefix * 7 + i * 11) % 36]);
            strings.push_back(first + mixed);
        }
        strings.push_back(first + "000000000");
        strings.push_back(first + "A");
        strings.push_back(first + "9a Z");
        strings.push_back(first + std::string(127, 'z'));
        strings.push_back(first + std::string("\xc3\xa9", 2));
    }
    // Mutate every suffix position through all bytes, including high-bit bytes
    // that signed SIMD comparisons must reject. Length ten tests the fallback.
    for (size_t length = 2; length <= 10; ++length)
        for (size_t at = 1; at < length; ++at)
            for (unsigned byte = 0; byte < 256; ++byte) {
                std::string text(length, '0');
                text[0] = 'u';
                text[at] = char(byte);
                strings.push_back(std::move(text));
            }
    std::mt19937_64 random(0x6260);
    for (unsigned trial = 0; trial < 50000; ++trial) {
        std::string text(1, char(random() % 256));
        size_t length = 1 + random() % 10;
        for (size_t i = 0; i < length; ++i)
            text.push_back(alphabet[random() % 36]);
        strings.push_back(std::move(text));
    }
    strings.push_back("");
    std::vector<native::Key> keys;
    keys.reserve(strings.size());
    for (size_t i = 0; i < strings.size(); ++i)
        keys.push_back(check_key(strings[i], i));
    constexpr size_t comparisons = 100000;
    for (size_t trial = 0; trial < comparisons; ++trial) {
        size_t a = random() % keys.size(), b = random() % keys.size();
        require((keys[a] < keys[b]) == byte_less(strings[a], strings[b]), "order", trial);
        require((keys[a] == keys[b]) == (strings[a] == strings[b]), "equality", trial);
        require((keys[a] != keys[b]) == (strings[a] != strings[b]), "inequality", trial);
        if (keys[a] == keys[b])
            require(keys[a].hash() == keys[b].hash(), "equal-key hashes", trial);
        if (keys[a].packed() && keys[b].packed())
            require((native::Key::order(keys[a].v) < native::Key::order(keys[b].v)) ==
                        byte_less(strings[a], strings[b]), "packed order", trial);
    }
    std::sort(strings.begin(), strings.end(), byte_less);
    std::sort(keys.begin(), keys.end());
    for (size_t i = 0; i < keys.size(); ++i) {
        char decoded[16];
        require(keys[i].text(decoded) == strings[i], "complete sorted order", i);
    }
    // Place the final byte directly before an inaccessible page. Encoders must
    // neither overread short IDs nor retain pointers to the source buffer.
    size_t page = size_t(sysconf(_SC_PAGESIZE));
    char *mapping = static_cast<char *>(mmap(nullptr, page * 2, PROT_READ | PROT_WRITE,
                                           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (mapping == MAP_FAILED || mprotect(mapping + page, page, PROT_NONE))
        return 2;
    std::vector<std::string> guarded_text;
    std::vector<native::Key> guarded_keys;
    for (size_t length = 0; length <= 65; ++length) {
        std::string text(length, 'z');
        if (length)
            text[0] = 'u';
        char *data = mapping + page - length;
        std::memcpy(data, text.data(), length);
        guarded_keys.push_back(check_key(SV(data, length), length));
        guarded_text.push_back(std::move(text));
    }
    munmap(mapping, page * 2);
    for (size_t i = 0; i < guarded_keys.size(); ++i) {
        char decoded[16];
        require(guarded_keys[i].text(decoded) == guarded_text[i], "source lifetime", i);
    }
    std::cout << "PASS " << keys.size() << " key cases; " << comparisons
              << " pair comparisons; complete sorted order; " << guarded_keys.size()
              << " guarded-buffer/lifetime cases\n";
}
'''
with tempfile.TemporaryDirectory(prefix="shopping-key-check-") as tmp:
    source = Path(tmp) / "check.cpp"
    binary = Path(tmp) / "check"
    source.write_text(HEADER + "namespace native {\n" + candidate + "}\n" + CASES)
    flags = ["-std=c++17", "-O1", "-g"]
    if not args.no_sanitizers:
        flags += ["-fsanitize=address,undefined", "-fno-sanitize-recover=all"]
    if args.scalar:
        flags += ["-U__SSE2__"]
    subprocess.run(shlex.split(args.compiler) + flags + [str(source), "-o", str(binary)], check=True)
    subprocess.run([str(binary)], check=True)
