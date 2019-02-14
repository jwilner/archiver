package main

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type item struct {
	Expiration time.Time
	Path       string
}

func newFileQueue() fileQueue {
	return fileQueue{
		m:    make(map[string]bool),
		push: make(chan []item),
		pop:  make(chan string),
	}
}

type fileQueue struct {
	q     pQueue
	m     map[string]bool
	mutex sync.Mutex

	push chan []item
	pop  chan string
}

func (pm *fileQueue) Run(ctx context.Context) {
	pushAll := func(is []item) {
		for _, i := range is {
			if !pm.m[i.Path] {
				heap.Push(&pm.q, i)
				pm.m[i.Path] = true
			}
		}
	}

	for {
		if pm.q.Len() > 0 {
			select {
			case <-ctx.Done():
				return
			case is := <-pm.push:
				pushAll(is)
			case pm.pop <- pm.q[0].Path:
				i := heap.Pop(&pm.q).(item)
				delete(pm.m, i.Path)
			}
		} else {
			select {
			case <-ctx.Done():
				return
			case is := <-pm.push:
				pushAll(is)
			}
		}
	}
}

func (pm *fileQueue) Push() chan<- []item {
	return pm.push
}

func (pm *fileQueue) Pop() <-chan string {
	return pm.pop
}

type pQueue []item

func (p pQueue) Len() int {
	return len(p)
}

func (p pQueue) Less(i, j int) bool {
	return p[i].Expiration.Before(p[j].Expiration)
}

func (p pQueue) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p *pQueue) Push(x interface{}) {
	*p = append(*p, x.(item))
}

func (p *pQueue) Pop() interface{} {
	old := *p
	n := len(old)
	x := old[n-1]
	*p = old[0 : n-1]
	return x
}
