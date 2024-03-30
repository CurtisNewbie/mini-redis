package main

import (
	"flag"
	"net"
	"net/http"
	"net/http/pprof"
)

var profile = flag.Bool("profile", false, "enable cpu/memory profiling")
var debug = flag.Bool("debug", false, "enable debug log")

func main() {
	flag.Parse()

	if *profile {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/{action}", pprof.Index)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)

		go func() {
			if err := http.ListenAndServe(":8080", mux); err != nil {
				panic(err)
			}
		}()
	}

	StartQueue()

	err := Listen("tcp", "localhost", DefaultPort, TcpConnAdaptor(
		TcpDataHandler{
			OnData: func(remote net.Addr, buf []byte, reply chan []byte) {
				Debugf("Received from %v:\n%s", remote.String(), buf)
				QueueCommand(&Command{buf: buf, reply: reply})
			},
			OnClosed: func(remote net.Addr) {
				Debugf("Connection for %v closed\n", remote.String())
				LogConnCount()
			},
		}),
	)
	if err != nil {
		panic(err)
	}
}
