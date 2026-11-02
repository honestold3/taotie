# syntax=docker/dockerfile:1

# 构建镜像默认用 bullseye（glibc 2.31），兼容性较好；
# 若生产节点更老（如 CentOS 7 / glibc 2.17），需要换基础镜像或改用 musl 静态编译。
ARG BUILDER_IMAGE=rust:1.99-slim-bullseye
ARG RUNTIME_IMAGE=debian:bullseye-slim

FROM ${BUILDER_IMAGE} AS builder

# librdkafka 通过 cmake 内置编译，需要 cmake / C 编译器 / zlib 头文件。
# libcurl4-openssl-dev 是必需的：librdkafka 2.12.1 的 rdkafka_conf.c 用 #ifdef 判断
# WITH_OAUTHBEARER_OIDC，而 config.h 总会把它定义成 0，导致无条件 include <curl/curl.h>。
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake gcc g++ zlib1g-dev libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM ${RUNTIME_IMAGE}

# 二进制动态依赖 glibc 与 libz
RUN apt-get update \
    && apt-get install -y --no-install-recommends zlib1g passwd \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system taotie \
    && useradd --system --gid taotie --no-create-home taotie

COPY --from=builder /src/target/release/taotie /usr/local/bin/taotie

WORKDIR /opt/taotie
ENV TAOTIE_CONFIG=/etc/taotie/config.toml
EXPOSE 8080
USER taotie
ENTRYPOINT ["/usr/local/bin/taotie"]
