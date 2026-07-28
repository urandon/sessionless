# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.4
FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILT_AT=unknown
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Version=${VERSION} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Commit=${COMMIT} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.BuiltAt=${BUILT_AT}" \
    -o /out/worker ./cmd/worker-codex

# The harness runtime deliberately has its own image boundary. Issue MVP-11
# will add the selected subscription-backed CLI and its filesystem contract.
FROM gcr.io/distroless/base-debian12:nonroot

COPY --from=build /out/worker /worker
USER nonroot:nonroot
ENTRYPOINT ["/worker"]
