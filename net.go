package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
)

const (
	Tcp         = "tcp"
	DefaultPort = 6379
)

type TcpConnHandler func(conn net.Conn)
type TcpDataHandler struct {
	OnData   func(remoteAddr net.Addr, buf []byte, reply chan []byte)
	OnClosed func(remote net.Addr)
	Chan     chan []byte
}

var (
	ConnCount int64 = 0
)

func Listen(proto string, host string, port int, handler func(conn net.Conn)) error {
	listener, err := net.Listen(proto, fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return fmt.Errorf("failed to listen on %v %v:%v, %w", proto, host, port, err)
	}
	defer listener.Close()
	fmt.Printf("Server is listening on port %v\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		go handler(conn)
	}
}

func TcpConnAdaptor(delegate TcpDataHandler) TcpConnHandler {
	return func(conn net.Conn) {
		c := atomic.AddInt64(&ConnCount, 1)
		Debugf("Connection count: %d\n", c)

		defer conn.Close()
		defer delegate.OnClosed(conn.RemoteAddr())
		defer func() { atomic.AddInt64(&ConnCount, -1) }()

		ch := make(chan []byte, 1)
		buffer := make([]byte, 1024)

		for {
			n, err := conn.Read(buffer)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				fmt.Println("Error:", err)
				return
			}
			delegate.OnData(conn.RemoteAddr(), buffer[:n], ch)
			res := <-ch
			if res != nil {
				if *debug {
					Debugf("Respond: %s", string(res))
				}
				conn.Write(res)
			}
		}
	}
}

func LogConnCount() {
	c := atomic.LoadInt64(&ConnCount)
	Debugf("Connection count: %d", c)
}
