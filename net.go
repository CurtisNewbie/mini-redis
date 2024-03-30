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
	OnData   func(remoteAddr net.Addr, buf []byte) []byte
	OnClosed func()
}

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
		defer conn.Close()
		defer delegate.OnClosed()

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
			res := delegate.OnData(conn.RemoteAddr(), buffer[:n])
			if res != nil {
				Debugf("Respond: %s", string(res))
				conn.Write(res)
			}
		}
	}
}
