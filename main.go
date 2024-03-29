package main

import (
	"fmt"
	"net"

	"github.com/curtisnewbie/mini-redis/resp"
)

func main() {
	err := resp.Listen("tcp", "localhost", resp.DefaultPort, resp.TcpConnAdaptor(resp.TcpDataHandler{
		OnData: func(remote net.Addr, buf []byte) []byte {
			fmt.Printf("Received from %v:\n%s\n", remote.String(), buf)
			return resp.ParseRespData(buf, resp.ParseRespProto)
		},
		OnClosed: func() {
			fmt.Println("Connection closed")
		},
	}))
	if err != nil {
		panic(err)
	}
}
