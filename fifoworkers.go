// Copyright 2017 Gabriel Pérez Salazar. All rights reserved.
// Use of this source code is governed by a MIT license that
// can be found in the LICENSE file.

// Run tasks in parallel and return in FIFO

package fifoworkers

import (
	"runtime"
	"sync"
	"sync/atomic"
)

type Pool struct {
	sync.Mutex

	f       func(a interface{}) interface{}
	t       []interface{}
	queueCh chan *task
	ended   bool
	C       chan interface{}
	moveCh  chan bool

	index int64
	count int64
}

type task struct {
	i int64
	f interface{}
}

type Config struct {
	workers    int
	queueLimit int
}

var (
	queueLimit = 5000
)

// New create and return pool
func New(c Config, f func(a interface{}) interface{}) *Pool {

	if c.queueLimit == 0 {
		c.queueLimit = queueLimit
	}

	if c.workers == 0 {
		c.workers = runtime.NumCPU()
	}

	p := &Pool{
		f:       f,
		t:       make([]interface{}, c.workers),
		queueCh: make(chan *task, c.queueLimit),
		C:       make(chan interface{}, c.queueLimit),
		moveCh:  make(chan bool, c.queueLimit),
		index:   0,
		count:   -1,
		ended:   false,
	}

	go p.move()

	for o := 0; o < c.workers; o++ {
		go p.run()
	}

	return p
}

// End the incoming tasks
func (p *Pool) End() {
	p.Lock()
	defer p.Unlock()

	p.ended = true
	close(p.queueCh)
}

// Add a task to the pool
func (p *Pool) Add(a interface{}) {
	p.queueCh <- &task{
		i: atomic.AddInt64(&p.count, 1),
		f: a,
	}
}

func (p *Pool) run() {
	for t := range p.queueCh {
		p.store(t.i, p.f(t.f))
	}
}

func (p *Pool) move() {
	defer func() {
		//log.Println("MOVE END")
		close(p.C)
	}()

	for {
		<-p.moveCh
		if p.t[p.index] == nil {
			continue
		}

		p.C <- p.t[p.index]

		p.t[p.index] = nil

		//log.Printf("++ Move to %d, count %d, queue %d, ended %t",
		//	p.index, p.count, len(p.ch), p.ended)
		if len(p.queueCh) == 0 && p.ended && p.index == p.count {
			return
		}

		p.index++

		if len(p.t) > int(p.index) {
			go func() {
				p.moveCh <- true
			}()
		}

	}
}

func (p *Pool) store(i int64, t interface{}) {
	p.Lock()
	defer p.Unlock()

	if i >= int64(len(p.t)) {
		for v := int64(len(p.t)); v <= i; v++ {
			p.t = append(p.t, nil)
		}
	}

	p.t[i] = t

	if i == p.index {
		p.moveCh <- true
	}
}
