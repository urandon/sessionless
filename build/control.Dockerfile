# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

ARG GO_BUILDER_IMAGE=golang:1.26.4-alpine@sha256:3ad57304ad93bbec8548a0437ad9e06a455660655d9af011d58b993f6f615648
ARG NODE_BUILDER_IMAGE=node:24.19.0-alpine@sha256:d32cdf619f63fe0471182d08996dd516c6275bb5fd31ae06e55a570bd9e1ad43
ARG DISTROLESS_STATIC_IMAGE=gcr.io/distroless/static-debian12:nonroot@sha256:1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a
ARG NPM_VERSION=11.17.0

FROM ${GO_BUILDER_IMAGE} AS go-source

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

FROM go-source AS component-build

ARG TARGET=control-api
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILT_AT=unknown
ARG SOURCE_DATE_EPOCH=0
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false -trimpath \
    -ldflags="-s -w \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Version=${VERSION} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Commit=${COMMIT} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.BuiltAt=${BUILT_AT}" \
    -o /out/component "./cmd/${TARGET}"

FROM ${NODE_BUILDER_IMAGE} AS web-assets

ARG NPM_VERSION
ARG COMMIT=unknown
WORKDIR /src/web
RUN npm install --global --fetch-retries=5 --fetch-retry-mintimeout=1000 \
      --fetch-retry-maxtimeout=10000 "npm@${NPM_VERSION}" \
    && test "$(npm --version)" = "${NPM_VERSION}"
COPY web/package.json web/package-lock.json ./
RUN npm ci --fetch-retries=5 --fetch-retry-mintimeout=1000 --fetch-retry-maxtimeout=10000
COPY web/ ./
RUN SESSIONLESS_WEB_VERSION="${COMMIT}" npm run build

FROM go-source AS web-bff-build

COPY --from=web-assets /src/web/build/ ./internal/webstatic/dist/
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILT_AT=unknown
ARG SOURCE_DATE_EPOCH=0
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false -trimpath \
    -ldflags="-s -w \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Version=${VERSION} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Commit=${COMMIT} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.BuiltAt=${BUILT_AT}" \
    -o /out/component "./cmd/web-bff"

FROM ${DISTROLESS_STATIC_IMAGE} AS runtime-base

USER nonroot:nonroot
ENTRYPOINT ["/component"]

FROM runtime-base AS web-bff-runtime

COPY --from=web-bff-build /out/component /component
EXPOSE 8083

# Keep the generic runtime last so direct builds retain the Dockerfile's
# historical default. The documented image pipeline selects both targets
# explicitly.
FROM runtime-base AS runtime

COPY --from=component-build /out/component /component
EXPOSE 8080
