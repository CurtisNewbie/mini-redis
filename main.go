package main

import (
	"fmt"
	"net"
)

func main() {
	err := Listen("tcp", "localhost", DefaultPort, TcpConnAdaptor(
		TcpDataHandler{
			OnData: func(remote net.Addr, buf []byte) []byte {
				fmt.Printf("Received from %v:\n%s\n", remote.String(), buf)
				return ParseRespData(buf, ParseRespProto)
			},
			OnClosed: func() {
				fmt.Println("Connection closed")
			},
		}),
	)
	if err != nil {
		panic(err)
	}
}
