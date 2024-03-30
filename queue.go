package main

import (
	"context"
	"sync"
)

var (
	ctx       context.Context
	cancel    func()
	queueOnce sync.Once
	queue     = make(chan *Command, 50)
)

type Command struct {
	buf   []byte
	reply chan []byte
}

func StartQueue() {
	queueOnce.Do(func() {
		c, cc := context.WithCancel(context.Background())
		ctx = c
		cancel = cc
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case cmd := <-queue:
					out := ParseRespData(cmd.buf, ParseRespProto)
					cmd.reply <- out
				}
			}
		}()
	})
}

func StopQueue() {
	cancel()
}

func QueueCommand(cmd *Command) {
	queue <- cmd
}
