package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"os"
	"sync"
	"time"
)

func NewMutexWriter(bufDur time.Duration) *MutexWriter {
	return &MutexWriter{
		l: pathLocker{
			m: make(map[string]*lock),
		},
		bufSecs: int64(bufDur / time.Second),
	}
}

type MutexWriter struct {
	bufSecs int64
	l       pathLocker
}

func (w *MutexWriter) Write(ctx context.Context, entry *Entry) error {
	h := bsHash(entry.Key)

	now := time.Now().Unix()
	bckt := now + w.bufSecs - (now % w.bufSecs) + h%w.bufSecs

	p := fmt.Sprintf("%v/%v", entry.Key, bckt)

	w.l.Lock(p)
	defer w.l.Unlock(p)

	f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return errors.Wrapf(err, "unable to open: %v", p)
	}
	defer func() {
		if err := f.Close(); f != nil {
			log.Printf("unable to close %v: %v", p, err)
		}
	}()
	_, err = f.Write(entry.Payload)

	return err
}

func bsHash(s string) (h int64) {
	for _, c := range s {
		h = h*31 + int64(c)
	}
	return h
}

// One lock guards a map of locks, where each inner lock corresponds to a path
type pathLocker struct {
	mx sync.Mutex
	m  map[string]*lock
}

func (p *pathLocker) Lock(path string) {
	p.mx.Lock()
	l, ok := p.m[path]
	if !ok {
		l = &lock{}
		p.m[path] = l
	}
	l.refs++
	p.mx.Unlock()

	l.Lock()
}

func (p *pathLocker) Unlock(path string) {
	p.mx.Lock()
	l := p.m[path]
	if l == nil {
		p.mx.Unlock()
		panic(fmt.Sprintf("should not be nil: %v", path))
	}
	l.refs--
	if l.refs == 0 {
		delete(p.m, path)
	}
	p.mx.Unlock()

	l.Unlock()
}

type lock struct {
	sync.Mutex
	refs int64
}
