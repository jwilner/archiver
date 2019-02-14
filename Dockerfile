FROM golang:1.11-alpine AS builder

ENV GO111MODULE=on

RUN apk add --no-cache git gcc musl-dev

WORKDIR /go/src/archiver
COPY . .

RUN go build

RUN ls

FROM scratch

COPY --from=builder /go/src/archiver/archiver .

CMD ["archiver"]
