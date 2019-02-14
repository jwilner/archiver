package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"time"
)

var (
	dsnAMQP = defaultEnv("AMQP_DSN", "amqp://")
)

type Entry struct {
	Key, Nonce string
	TS         time.Time
	Meta       map[string]string
	Payload    []byte
}

func defaultEnv(name, def string) string {
	if val, ok := os.LookupEnv(name); ok {
		return val
	}
	return def
}

func main() {
	log.Fatal(run())
}

func run() error {
	ctx, cncl := context.WithCancel(context.Background())
	defer cncl()

	conn, err := amqp.Dial(dsnAMQP)
	if err != nil {
		return errors.Wrap(err, "amqp.Dial")
	}

	errs := make(chan error)

	l := NewAMQPListener(conn, NewMutexWriter(time.Hour))
	go func() {
		if err := l.Run(ctx); err != nil {
			errs <- err
		}
	}()

	b := NewBufferHandler("/tmp", 20)
	go func() {
		if err := b.Run(ctx); err != nil {
			errs <- err
		}
	}()

	sigs := make(chan os.Signal)
	signal.Notify(sigs, os.Interrupt)

	select {
	case err := <-errs:
		return errors.Wrap(err, "listener failed")
	case sig := <-sigs:
		log.Printf("handling %v", sig)
		return nil
	}

}
