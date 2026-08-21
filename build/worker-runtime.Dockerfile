# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

ARG GO_BUILDER_IMAGE
ARG DISTROLESS_BASE_IMAGE
FROM ${GO_BUILDER_IMAGE} AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILT_AT=unknown
ARG SOURCE_DATE_EPOCH=0
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false -trimpath \
    -ldflags="-s -w \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Version=${VERSION} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Commit=${COMMIT} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.BuiltAt=${BUILT_AT}" \
    -o /out/worker ./cmd/worker-runtime

# The harness runtime deliberately has its own image boundary. The current
# credential-free deterministic adapter proves lifecycle semantics; issue
# MVP-10 adds selected subscription-backed CLI adapters to this boundary.
FROM ${DISTROLESS_BASE_IMAGE}

COPY --from=build /out/worker /worker
USER nonroot:nonroot
ENTRYPOINT ["/worker"]
