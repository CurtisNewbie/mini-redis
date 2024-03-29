package main

import (
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	Tcp         = "tcp"
	DefaultPort = 6379
)

type TcpConnHandler func(conn net.Conn)
type TcpDataHandler struct {
	OnData   func(remoteAddr net.Addr, buf []byte)
	OnClosed func()
}

func Listen(proto string, host string, port int, handler func(conn net.Conn)) error {
	listener, err := net.Listen(proto, fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return fmt.Errorf("failed to listen on %v %v:%v, %w", proto, host, port, err)
	}
	defer listener.Close()
	fmt.Println("Server is listening on port 8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		go handler(conn)
	}
}

func TcpConnAdatpr(delegate TcpDataHandler) TcpConnHandler {
	return func(conn net.Conn) {
		defer conn.Close()
		defer delegate.OnClosed()

		buffer := make([]byte, 1024)
		for {
			n, err := conn.Read(buffer)
			if err != nil {
				if errors.Is(err, io.EOF) {
					fmt.Println("Connection closed")
				}
				return
			}
			delegate.OnData(conn.RemoteAddr(), buffer[:n])
		}
	}
}
