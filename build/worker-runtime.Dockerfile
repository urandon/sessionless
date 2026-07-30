# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.4
FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
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
    -o /out/worker ./cmd/worker-runtime

# The harness runtime deliberately has its own image boundary. The current
# credential-free deterministic adapter proves lifecycle semantics; issue
# MVP-10 adds selected subscription-backed CLI adapters to this boundary.
FROM gcr.io/distroless/base-debian12:nonroot

COPY --from=build /out/worker /worker
USER nonroot:nonroot
ENTRYPOINT ["/worker"]
