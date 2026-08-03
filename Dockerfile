# syntax=docker/dockerfile:1.7

FROM rust:1.88.0-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS quorum-builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .claude/skills/quorum/SKILL.md ./.claude/skills/quorum/SKILL.md
COPY quorum-core ./quorum-core
COPY quorum ./quorum
RUN cargo build --locked --release --bin quorum

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS codex-fetcher
ARG CODEX_VERSION=0.146.0
ARG CODEX_SHA256=3c89125af1d7c98abec8beb551292ef99daca52e204e5852a9139feae2c467e5
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates=20230311+deb12u1 curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/codex
RUN curl --fail --location --show-error \
      "https://github.com/openai/codex/releases/download/rust-v${CODEX_VERSION}/codex-package-x86_64-unknown-linux-musl.tar.gz" \
      --output /tmp/codex.tar.gz \
    && printf '%s  %s\n' "${CODEX_SHA256}" /tmp/codex.tar.gz | sha256sum --check --strict \
    && tar --extract --gzip --file /tmp/codex.tar.gz --directory /opt/codex \
    && curl --fail --location --show-error \
      "https://raw.githubusercontent.com/openai/codex/rust-v${CODEX_VERSION}/LICENSE" \
      --output /opt/codex/LICENSE \
    && curl --fail --location --show-error \
      "https://raw.githubusercontent.com/openai/codex/rust-v${CODEX_VERSION}/NOTICE" \
      --output /opt/codex/NOTICE \
    && rm /tmp/codex.tar.gz

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818
ARG GIT_VERSION=1:2.39.5-0+deb12u3
ARG GH_VERSION=2.23.0+dfsg1-1
RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
      ca-certificates=20230311+deb12u1 \
      git="${GIT_VERSION}" \
      gh="${GH_VERSION}" \
      openssh-client \
    && rm -rf /var/lib/apt/lists/*

COPY --from=quorum-builder /src/target/release/quorum /usr/local/bin/quorum
COPY --from=codex-fetcher /opt/codex /opt/codex
COPY LICENSE /usr/share/doc/quorum/LICENSE
RUN install --directory --owner=10001 --group=10001 \
      /home/quorum \
      /data /data/quorum /data/repos /data/worktrees \
    && printf 'quorum:x:10001:\n' >> /etc/group \
    && printf 'quorum:x:10001:10001:Quorum runtime:/home/quorum:/bin/sh\n' >> /etc/passwd \
    && install --directory /usr/share/doc/codex \
    && cp /opt/codex/LICENSE /opt/codex/NOTICE /usr/share/doc/codex/

LABEL org.opencontainers.image.source="https://github.com/ag2trust/quorum" \
      org.opencontainers.image.licenses="MIT AND Apache-2.0" \
      org.opencontainers.image.description="Self-hostable Quorum runtime"

ENV HOME=/home/quorum \
    QUORUM_HOME=/data/quorum \
    PATH=/opt/codex/bin:/usr/local/bin:/usr/bin:/bin
VOLUME ["/data"]
WORKDIR /data/repos
USER 10001:10001

# QIMG-002 will replace this diagnostic default with daemon/web supervision.
CMD ["quorum", "--help"]
