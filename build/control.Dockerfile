# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.4
FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG TARGET=control-api
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILT_AT=unknown
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Version=${VERSION} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.Commit=${COMMIT} \
      -X gitcode.com/urandon/sessionless/internal/buildinfo.BuiltAt=${BUILT_AT}" \
    -o /out/component "./cmd/${TARGET}"

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/component /component
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/component"]
