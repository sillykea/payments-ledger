FROM golang:1.26-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /ledger ./cmd/ledger \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /loadtest ./cmd/loadtest \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /eventsink ./cmd/eventsink

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /ledger /ledger
COPY --from=build /loadtest /loadtest
COPY --from=build /eventsink /eventsink
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/ledger"]
