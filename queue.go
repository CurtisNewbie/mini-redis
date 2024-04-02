package main

import (
	"context"
	"sync"
)

const (
	ClientCommand = 0
	ServerCommand = 1
)

var (
	ctx       context.Context
	cancel    func()
	queueOnce sync.Once
	queue     = make(chan *Command, 500)
)

type Command struct {
	typ    int
	buf    []byte
	reply  chan []byte
	action func()
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
					if cmd.typ == ClientCommand {
						out := ParseRespData(cmd.buf, ParseRespProto)
						if cmd.reply != nil {
							cmd.reply <- out
						}
					} else if cmd.typ == ServerCommand {
						cmd.action()
					}
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
