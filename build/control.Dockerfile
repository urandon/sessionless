# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.4
ARG NODE_VERSION=24.19.0
ARG NPM_VERSION=11.17.0

FROM golang:${GO_VERSION}-alpine AS go-source

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
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Version=${VERSION} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Commit=${COMMIT} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.BuiltAt=${BUILT_AT}" \
    -o /out/component "./cmd/${TARGET}"

FROM node:${NODE_VERSION}-alpine AS web-assets

ARG NPM_VERSION
WORKDIR /src/web
RUN npm install --global "npm@${NPM_VERSION}" && test "$(npm --version)" = "${NPM_VERSION}"
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM go-source AS web-bff-build

COPY --from=web-assets /src/web/build/ ./internal/webstatic/dist/
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILT_AT=unknown
ARG SOURCE_DATE_EPOCH=0
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Version=${VERSION} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Commit=${COMMIT} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.BuiltAt=${BUILT_AT}" \
    -o /out/component "./cmd/web-bff"

FROM gcr.io/distroless/static-debian12:nonroot AS runtime-base

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
