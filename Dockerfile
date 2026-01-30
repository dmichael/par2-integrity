FROM python:3.13-alpine3.21

# Build par2cmdline-turbo from source
RUN apk add --no-cache --virtual .build-deps \
        git g++ make automake autoconf \
    && git clone --depth 1 https://github.com/animetosho/par2cmdline-turbo.git /tmp/par2 \
    && cd /tmp/par2 \
    && aclocal && automake --add-missing && autoconf \
    && ./configure \
    && make -j$(nproc) \
    && make install \
    && cd / \
    && rm -rf /tmp/par2 \
    && apk del .build-deps

# Runtime C++ libs (needed by par2) and tini for PID 1
RUN apk add --no-cache libstdc++ libgcc tini

# Copy application code
COPY par2integrity/ /app/par2integrity/
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

WORKDIR /app

# Ensure parity directories exist
RUN mkdir -p /parity/_db /parity/_logs /parity/by_hash

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["tini", "--", "/app/entrypoint.sh"]
