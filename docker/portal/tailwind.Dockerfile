FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc AS download
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && case "${TARGETARCH}" in \
      amd64) asset=x64; checksum=73f0e5459054e5cfaa8ab6f3b940f3fbe0f13cc7fd83bc24e7c655033c203400 ;; \
      arm64) asset=arm64; checksum=8f48dcb72be3b351c10563c5329b4638ba8516820dc3b3a1609625a166e87cbd ;; \
      *) echo "Unsupported Tailwind architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && curl --fail --location --silent --show-error \
      "https://github.com/tailwindlabs/tailwindcss/releases/download/v4.3.0/tailwindcss-linux-${asset}" \
      -o /tailwindcss \
    && printf '%s  /tailwindcss\n' "${checksum}" | sha256sum --check \
    && chmod 0555 /tailwindcss

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /output /source/portal_static /source/portal_styles /source/portal_templates \
    && touch /output/.keep \
    && chown -R 10001:10001 /output
COPY --from=download /tailwindcss /usr/local/bin/tailwindcss
COPY --chmod=0555 docker/portal/tailwind-watch.sh /usr/local/bin/tailwind-watch
USER 10001:10001
ENV HOME=/tmp
WORKDIR /source
ENTRYPOINT ["/usr/local/bin/tailwind-watch"]
