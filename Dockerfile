# The fast engine is Linux amd64 only. Other platforms still build the C++
# engine below and receive an empty fast-engine directory.
FROM rust:1.100.0-alpine AS fast-builder
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY fast/Cargo.toml fast/Cargo.lock ./
COPY fast/.cargo .cargo
COPY fast/src src
COPY fast/LICENSE /usr/share/doc/shopping-sessions-fast/LICENSE
ENV XL_BB=14
RUN mkdir -p /fast-bin \
    && if [ "$(uname -m)" = x86_64 ]; then \
        cargo build --locked --release --target x86_64-unknown-linux-musl \
        && cp target/x86_64-unknown-linux-musl/release/pipeline /fast-bin/shopping-sessions-fast; \
    fi

# Build at image-build time, never inside the timed execution. Unused Arrow
# components are excluded; the native binary links Arrow/Parquet statically.
FROM debian:bookworm-slim AS builder
ARG BUILD_JOBS=8
RUN apt-get update && apt-get install -y --no-install-recommends g++ cmake make ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY scripts/build_minimal.cmake /app/scripts/build_minimal.cmake
# Separate dependency construction so normal pipeline edits can reuse this layer.
RUN cmake -DROOT=/app -DJOBS=${BUILD_JOBS} -DDEPS_ONLY=ON -P /app/scripts/build_minimal.cmake
COPY CMakeLists.txt /app/CMakeLists.txt
COPY pipeline/native.cpp /app/pipeline/native.cpp
COPY pipeline/dispatch.cpp /app/pipeline/dispatch.cpp
RUN g++ -Os -std=c++17 -static -s /app/pipeline/dispatch.cpp -o /app/shopping-sessions-dispatch
COPY scripts/training_gen.cpp /app/scripts/training_gen.cpp
# Profile-guided build: an instrumented binary runs on generated training data
# (never the scoring data), then the final binary is compiled with its profile.
# If generating or training fails for any reason, the final binary is built
# without a profile instead, so this step cannot fail a submission.
RUN set -e; \
    build() { cmake -S /app -B /app/build -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_PREFIX_PATH=/app/dev-tools/arrow-minimal -DPGO=$1 \
      && cmake --build /app/build --parallel ${BUILD_JOBS}; }; \
    build generate; \
    if g++ -O2 -std=c++17 /app/scripts/training_gen.cpp -o /tmp/training_gen \
        && /tmp/training_gen /tmp/train 1000000 \
        && DATA_DIR=/tmp/train OUTPUT_DIR=/tmp/train-out NCPU=${BUILD_JOBS} /app/build/shopping-sessions \
        && find /app/build -name '*.gcda' | grep -q .; \
    then build use || { echo "PGO build failed; building without a profile" >&2; build OFF; }; \
    else echo "PGO training failed; building without a profile" >&2; build OFF; fi; \
    rm -rf /tmp/train /tmp/train-out /tmp/training_gen

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /app/build/shopping-sessions /app/build/shopping-sessions
COPY --from=builder /app/shopping-sessions-dispatch /app/build/shopping-sessions-dispatch
COPY --from=fast-builder /fast-bin/ /app/build/
COPY --from=fast-builder /usr/share/doc/shopping-sessions-fast/LICENSE /usr/share/doc/shopping-sessions-fast/LICENSE
COPY --from=builder /app/dev-tools/arrow-apache-arrow-19.0.1/LICENSE.txt /usr/share/doc/arrow/LICENSE.txt
COPY --from=builder /app/dev-tools/arrow-apache-arrow-19.0.1/NOTICE.txt /usr/share/doc/arrow/NOTICE.txt
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and /output and /scratch are the only writable
# places. run.sh points every temp and cache directory at /scratch.
WORKDIR /app
CMD ["/app/run.sh"]
