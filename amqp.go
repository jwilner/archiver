package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"log"
	"sync"
)

type Writer interface {
	Write(ctx context.Context, entry *Entry) error
}

func NewAMQPListener(conn *amqp.Connection, writer Writer) *AMQPListener {
	return &AMQPListener{
		conn:   conn,
		writer: writer,
		qName:  "bob",
	}
}

type AMQPListener struct {
	conn *amqp.Connection
	done chan struct{}

	qName  string
	writer Writer
}

func (c *AMQPListener) Run(ctx context.Context) error {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	ch, err := c.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "conn.Channel")
	}
	defer func() {
		if err := ch.Close(); err != nil {
			log.Printf("channel.Close failed: %v", err)
		}
	}()

	if _, err := ch.QueueDeclare(c.qName, false, false, false, false, amqp.Table{}); err != nil {
		return err
	}

	ds, err := ch.Consume(c.qName, "", false, false, false, false, amqp.Table{})
	if err != nil {
		return err
	}

	for {
		select {
		case d := <-ds:
			log.Printf("Received message")
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := handle(ctx, c.writer, &d); err != nil {
					log.Printf("error handling msg: %v", err)
				}
			}()
		case <-ctx.Done():
			log.Printf("Context closed")
			return nil
		}
	}
}

func handle(ctx context.Context, writer Writer, d *amqp.Delivery) error {
	err := writer.Write(ctx, &Entry{
		Key:     d.MessageId,
		TS:      d.Timestamp,
		Payload: d.Body,
	})
	if err != nil {
		reQ := false
		type tempError interface {
			Temporary() bool
		}
		if e, ok := err.(tempError); ok && e.Temporary() {
			reQ = true
		}
		return d.Nack(false, reQ)
	}

	return d.Ack(false)
}
