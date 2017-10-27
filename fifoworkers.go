// Copyright 2017 Gabriel Pérez Salazar. All rights reserved.
// Use of this source code is governed by a MIT license that
// can be found in the LICENSE file.

// Run tasks in parallel and return in FIFO

package fifoworkers

import (
	"log"
	"runtime"
	"sync"
)

type Pool struct {
	sync.Mutex

	f       func(a interface{}) interface{}
	t       []interface{}
	queueCh chan *task
	ended   bool
	C       chan interface{}
	moveCh  chan bool

	index int
	count int
}

type task struct {
	i int
	f interface{}
}

type Config struct {
	Workers    int
	QueueLimit int
}

var (
	queueLimit = 5000
)

// New create and return pool
func New(c Config, f func(a interface{}) interface{}) *Pool {

	if c.QueueLimit == 0 {
		c.QueueLimit = queueLimit
	}

	if c.Workers == 0 {
		c.Workers = runtime.NumCPU()
	}

	p := &Pool{
		f:       f,
		t:       make([]interface{}, 0),
		queueCh: make(chan *task, c.QueueLimit),
		C:       make(chan interface{}, c.QueueLimit),
		moveCh:  make(chan bool, c.QueueLimit),
		index:   0,
		count:   -1,
		ended:   false,
	}

	go p.move()

	for o := 0; o < c.Workers; o++ {
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
	p.Lock()
	defer p.Unlock()

	p.t = append(p.t, nil)
	p.count++

	p.queueCh <- &task{
		i: p.count,
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

		log.Printf("++ Move to %d, count %d, queue %d, ended %t",
			p.index, p.count, len(p.queueCh), p.ended)
		if len(p.queueCh) == 0 && p.ended && p.index == p.count {
			return
		}

		p.index++

		go func() {
			p.moveCh <- true
		}()

	}
}

func (p *Pool) store(i int, t interface{}) {
	p.Lock()
	defer p.Unlock()

	p.t[i] = t

	if i == p.index {
		p.moveCh <- true
	}
}
