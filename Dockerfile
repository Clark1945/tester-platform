# 多階段建置：同一個 Dockerfile 用 --build-arg CMD=api|receiver|... 產生各服務的映像檔
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG CMD=api
RUN CGO_ENABLED=0 go build -o /out/app ./cmd/${CMD}

FROM alpine:3.20
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
