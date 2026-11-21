#!/usr/bin/env python3
"""Fuzz native CSV/timestamp changes against the previous published parser.

Requires a C++17 compiler and the reference Git commit locally (fetch it first
for a shallow clone). No Arrow libraries or generated datasets are needed.
The fixed-seed cases include malformed quotes, all byte values, CR/LF, numeric
limits, calendar dates, offsets, and input buffers ending at a protected page.
"""
import argparse
import os
from pathlib import Path
import shlex
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--reference-ref", default="a7cd5c1d43390cfb3960f2a11a9e13a5d44c8f9f")
parser.add_argument("--candidate", type=Path)
parser.add_argument("--compiler", default=os.environ.get("CXX", "c++"))
parser.add_argument("--scalar", action="store_true", help="Exercise the non-SSE2 fallback")
parser.add_argument("--no-sanitizers", action="store_true")
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
# The optional source path is useful for isolated performance experiments.
candidate = (args.candidate or root / "pipeline/native.cpp").read_text()
reference = subprocess.check_output(
    ["git", "show", f"{args.reference_ref}:pipeline/native.cpp"], cwd=root, text=True)

def functions(source):
    start = source.index("template <size_t N, class F> void rows(")
    end = source.index("static int price(", start)
    return source[start:end]

reference = functions(reference)
# The old checked numeric parser can overflow transiently at INT64_MAX because
# it adds ASCII '0' before subtracting it. Parenthesize that reference-only
# expression to retain its intended arithmetic without contaminating UBSan.
reference = reference.replace("n = n * 10 + c - '0';", "n = n * 10 + (c - '0');")
candidate = functions(candidate)
HEADER = r'''
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#if defined(__SSE2__)
#include <emmintrin.h>
#endif
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>
using SV = std::string_view;
using I64 = int64_t;
constexpr I64 DAY = 86400000;
constexpr I64 BAD = std::numeric_limits<I64>::min();
'''
CASES = r'''
std::mt19937_64 rng(9831724);
using Result = std::vector<std::vector<std::string>>;
template<size_t N> bool check_rows(SV s, bool header) {
    Result a, b;
    old::rows<N>(s, header, [&](const auto &row) {a.emplace_back(); for (auto v:row) a.back().emplace_back(v);});
    updated::rows<N>(s, header, [&](const auto &row) {b.emplace_back(); for (auto v:row) b.back().emplace_back(v);});
    if(a != b) {std::cerr<<"CSV mismatch width="<<N<<" header="<<header<<" size="<<s.size()<<'\n'; return false;}
    return true;
}
bool check_time(const std::string &s) {
    auto a=old::timestamp(s), b=updated::timestamp(s);
    if(a!=b) {std::cerr<<"Timestamp mismatch: "<<s<<" old="<<a<<" new="<<b<<'\n'; return false;}
    return true;
}
int main() {
    std::string alphabet="abXYZ0123 ,\"\n\r\t";
    alphabet.push_back('\0'); alphabet.push_back(char(0x80)); alphabet.push_back(char(0xff));
    size_t csv_cases=0, stamp_cases=0;
    for(unsigned trial=0;trial<60000;++trial) {
        std::string s;
        if(trial%2) {
            size_t length=rng()%400;
            for(size_t i=0;i<length;++i) s+=alphabet[rng()%alphabet.size()];
        } else {
            const unsigned widths[]={2,4,7};
            unsigned width=widths[rng()%3];
            for(unsigned row=0;row<1+rng()%4;++row) {
                for(unsigned col=0;col<width;++col) {
                    if(col) s+=',';
                    unsigned quoted=rng()%4;
                    if(quoted) s+='"';
                    for(unsigned c=0;c<rng()%50;++c) s+=char('a'+rng()%26);
                    if(quoted==1) s+='"';
                    if(quoted==2) s+="\"\"";
                }
                if(rng()%3) s+='\n';
            }
        }
        for(bool header:{false,true}) {
            if(!check_rows<2>(s,header)||!check_rows<4>(s,header)||!check_rows<7>(s,header)) return 1;
            csv_cases+=3;
        }
    }
    // Place every short length against a protected page: vector loads must
    // stay in the input, including 15/16/17-byte and unterminated-row tails.
    size_t page = size_t(sysconf(_SC_PAGESIZE));
    char *mapping = static_cast<char *>(mmap(nullptr, page * 2, PROT_READ | PROT_WRITE,
                                           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (mapping == MAP_FAILED || mprotect(mapping + page, page, PROT_NONE)) return 2;
    for (size_t length = 0; length <= 257; ++length) {
        std::string input(length, 'a');
        const std::string pattern = "a,b,c,d,e,f,g\n\"a\",b,c,d,e,f,g";
        for (size_t i = 0; i < length; ++i) input[i] = pattern[i % pattern.size()];
        char *data = mapping + page - length;
        std::memcpy(data, input.data(), length);
        for (bool header : {false, true}) {
            SV view(data, length);
            if (!check_rows<2>(view,header) || !check_rows<4>(view,header) || !check_rows<7>(view,header)) return 1;
            csv_cases += 3;
        }
    }
    munmap(mapping, page * 2);
    for(const auto &s:std::vector<std::string>{"","0","0000000000000000000000000000000000000","9223372036854775807","9223372036854775808","9999999999999999999","000009223372036854775807","2024-02-29T00:00:00Z","1900-02-29T00:00:00Z","2000-02-29T23:59:59+23:59","0001-01-01T00:00:00Z","9999-12-31T23:59:59Z"}) {
        if(!check_time(s)) return 1; ++stamp_cases;
    }
    for(unsigned trial=0;trial<180000;++trial) {
        std::string s;
        if(trial%3==0) {
            for(unsigned i=0,n=rng()%90;i<n;++i) s+=char('0'+rng()%10);
            if(rng()%2 && !s.empty()) s[rng()%s.size()]=char(rng()%256);
        } else {
            char buf[64];
            snprintf(buf,sizeof(buf),"%04u-%02u-%02uT%02u:%02u:%02u%c%02u:%02u",unsigned(rng()%10001),unsigned(rng()%15),unsigned(rng()%34),unsigned(rng()%27),unsigned(rng()%64),unsigned(rng()%64),rng()%2?'+':'-',unsigned(rng()%27),unsigned(rng()%64));
            s=buf;
            if(rng()%2) {s.resize(20);s[19]='Z';}
            if(rng()%3==0) s[rng()%s.size()]=char(rng()%256);
        }
        if(!check_time(s))return 1; ++stamp_cases;
    }
    // Mutate every byte through the complete byte range, including a repeated
    // valid date immediately before each mutation to exercise date-cache hits.
    for(auto original:{std::string("2027-06-17T12:34:56Z"),std::string("2027-06-17T12:34:56+02:30")})
        for(size_t at=0;at<original.size();++at)for(unsigned c=0;c<256;++c) {
            if(!check_time(original))return 1;
            auto changed=original;changed[at]=char(c);
            if(!check_time(changed))return 1;
            stamp_cases+=2;
        }
    std::cout<<"PASS "<<csv_cases<<" CSV comparisons; "<<stamp_cases<<" timestamp comparisons\n";
}
'''
with tempfile.TemporaryDirectory(prefix="shopping-parser-check-") as tmp:
    source = Path(tmp) / "check.cpp"
    binary = Path(tmp) / "check"
    source.write_text(HEADER + "namespace old {\n" + reference + "}\n" +
                      "namespace updated {\n" + candidate + "}\n" + CASES)
    flags = ["-std=c++17", "-O1", "-g"]
    if not args.no_sanitizers:
        flags += ["-fsanitize=address,undefined", "-fno-sanitize-recover=all"]
    if args.scalar:
        flags += ["-U__SSE2__"]
    subprocess.run(shlex.split(args.compiler) + flags + [str(source), "-o", str(binary)], check=True)
    subprocess.run([str(binary)], check=True)
