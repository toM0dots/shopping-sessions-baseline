# Build at image-build time, never inside the timed execution. The scorer is
# linux/amd64; make build supplies the platform without host-specific ISA flags.
FROM python:3.12-slim-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends g++ \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --only-binary=:all: -r /app/requirements.txt
COPY pipeline/ /app/pipeline/
COPY scripts/build_native.py /app/scripts/build_native.py
RUN python /app/scripts/build_native.py

FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
# Arrow's wheel bundles its native dependencies. No Python process is started
# at runtime; keep the same library paths used by the builder's rpath.
COPY --from=builder /usr/local/lib/python3.12/site-packages/pyarrow/ /usr/local/lib/python3.12/site-packages/pyarrow/
COPY --from=builder /app/build/shopping-sessions /app/build/shopping-sessions
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and /output and /scratch are the only writable
# places. run.sh points every temp and cache directory at /scratch.
WORKDIR /app
CMD ["/app/run.sh"]
