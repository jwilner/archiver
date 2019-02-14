package main

import (
	"context"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"
)

const (
	layoutFs = "20060102T150405"
)

type BufferHandler struct {
	dir     string
	fileQ   fileQueue
	workers int
}

func NewBufferHandler(dir string, workers int) *BufferHandler {
	b := &BufferHandler{
		dir:     dir,
		fileQ:   newFileQueue(),
		workers: workers,
	}
	return b
}

func (b *BufferHandler) Run(ctx context.Context) error {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cncl := context.WithCancel(ctx)
	defer cncl()

	errs := make(chan error)

	go func() {
		defer wg.Done()
		poll(
			ctx,
			errs,
			b.fileQ.Push(),
			b.dir,
			10*time.Second,
		)
	}()

	for i := 0; i < b.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runUpload(
				ctx,
				errs,
				b.fileQ.Pop(),
			)
		}()
	}

	select {
	case <-ctx.Done():
		return nil
	case <-errs:
		return nil
	}
}

func poll(
	ctx context.Context,
	errs chan<- error,
	items chan<- []item,
	dir string,
	freq time.Duration,
) {

	for {
		if is, err := findFiles(dir); err != nil {
			errs <- err
			return
		} else if len(is) > 0 {
			select {
			case items <- is:
			case <-ctx.Done():
				return
			}
		}

		select {
		case <-time.After(freq):
		case <-ctx.Done():
			return
		}
	}
}

func findFiles(dir string) ([]item, error) {
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "ioutil.ReadDir failed on %v", dir)
	}

	var is []item
	for _, fi := range fis {
		dirName := fi.Name()
		if t, err := time.Parse(layoutFs, dirName); err != nil {
			continue
		} else {
			fis, err := ioutil.ReadDir(path.Join(dir, dirName))
			if err != nil {
				continue
			}
			for _, fi := range fis {
				is = append(is, item{Expiration: t, Path: path.Join(dir, dirName, fi.Name())})
			}
		}
	}

	return is, nil
}

func runUpload(ctx context.Context, errs chan<- error, paths <-chan string) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-paths:
			if err := upload(ctx, p); err != nil {
				errs <- err
				return
			}
		}
	}
}

func upload(ctx context.Context, path string) error {
	_, err := os.Open(path)
	if err != nil {
		return err
	}

	return nil
}
