package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
)

var profile = flag.Bool("profile", false, "enable cpu/memory profiling")
var debug = flag.Bool("debug", false, "enable debug log")

func main() {
	flag.Parse()

	if *profile {
		myMux := http.NewServeMux()
		myMux.HandleFunc("/debug/pprof/", pprof.Index)
		myMux.HandleFunc("/debug/pprof/{action}", pprof.Index)
		myMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)

		go func() {
			if err := http.ListenAndServe(":8080", myMux); err != nil {
				panic(err)
			}
		}()
	}

	err := Listen("tcp", "localhost", DefaultPort, TcpConnAdaptor(
		TcpDataHandler{
			OnData: func(remote net.Addr, buf []byte) []byte {
				Debugf("Received from %v:\n%s", remote.String(), buf)
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
