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
RUN cmake -S /app -B /app/build -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_PREFIX_PATH=/app/dev-tools/arrow-minimal \
    && cmake --build /app/build --parallel ${BUILD_JOBS}

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /app/build/shopping-sessions /app/build/shopping-sessions
COPY --from=builder /app/dev-tools/arrow-apache-arrow-19.0.1/LICENSE.txt /usr/share/doc/arrow/LICENSE.txt
COPY --from=builder /app/dev-tools/arrow-apache-arrow-19.0.1/NOTICE.txt /usr/share/doc/arrow/NOTICE.txt
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and /output and /scratch are the only writable
# places. run.sh points every temp and cache directory at /scratch.
WORKDIR /app
CMD ["/app/run.sh"]
