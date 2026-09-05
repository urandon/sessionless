# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

ARG GO_BUILDER_IMAGE=golang:1.26.8-alpine@sha256:ce864e7223ac17b1775e6fd0b4c0db580c2eb50e7953a427916379e4b92a1628
ARG DISTROLESS_BASE_IMAGE=gcr.io/distroless/base-debian12:nonroot@sha256:b12529fbbd0bb15eea8905f69d83148679e0b4d7d434c8808100792029b1caae
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
