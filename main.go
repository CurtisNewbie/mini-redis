package main

import (
	"fmt"
	"net"
)

func main() {
	err := Listen("tcp", "localhost", DefaultPort, TcpConnAdatpr(TcpDataHandler{
		OnData: func(remote net.Addr, buf []byte) {
			fmt.Printf("Received from %v:\n%s\n", remote.String(), buf)
		},
		OnClosed: func() {
			fmt.Println("Connection closed")
		},
	}))
	if err != nil {
		panic(err)
	}
}
