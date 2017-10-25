package fifoworkers

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

type localTask struct {
	id   int
	name string
	t    time.Duration
}

func TestRun50tasks1Worker(t *testing.T) {
	runPool(50, t, Config{
		Workers:    1,
		QueueLimit: 1000,
	})
}

func TestRun50tasks5Worker(t *testing.T) {
	runPool(50, t, Config{
		Workers:    5,
		QueueLimit: 1000,
	})
}

func TestRun50tasks50Worker(t *testing.T) {
	runPool(50, t, Config{
		Workers:    50,
		QueueLimit: 1000,
	})
}

func runPool(tasks int, t *testing.T, cfg Config) {

	buf := &bytes.Buffer{}

	pool := New(cfg, func(a interface{}) interface{} {
		args := a.(localTask)
		return task1(args.id, args.name, args.t)
	})

	go func() {
		for i := 0; i < tasks; i++ {
			var t time.Duration
			if i%2 == 0 {
				t = 300 * time.Millisecond
			} else {
				t = 50 * time.Millisecond
			}
			s := fmt.Sprintf("test #%d time %s\n", i, t)
			buf.WriteString(s)

			pool.Add(localTask{
				id:   i,
				t:    t,
				name: s,
			})
		}
		pool.End()
	}()

	bufResult := &bytes.Buffer{}
	for c := range pool.C {
		bufResult.WriteString(c.(string))
	}

	if bytes.Compare(buf.Bytes(), bufResult.Bytes()) == 0 {
		return
	}

	t.Errorf("Input and result is not the same")

}

func task1(id int, name string, t time.Duration) string {
	rand.Seed(time.Now().UTC().UnixNano())

	<-time.After(t)
	return name
}
