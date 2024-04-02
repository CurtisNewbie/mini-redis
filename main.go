package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
)

var host = flag.String("host", "localhost", "host")
var port = flag.Int("port", DefaultPort, "port")
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
	ScheduleExpire()
	fmt.Printf("mini-redis %v\n", Version)

	err := Listen("tcp", *host, *port, TcpConnAdaptor(
		TcpDataHandler{
			OnData: func(remote net.Addr, buf []byte, reply chan []byte) {
				if *debug {
					Debugf("Received from %v:\n%s", remote.String(), buf)
				}
				QueueCommand(&Command{typ: ClientCommand, buf: buf, reply: reply})
			},
			OnClosed: func(remote net.Addr) {
				if *debug {
					Debugf("Connection for %v closed\n", remote.String())
				}
				LogConnCount()
			},
		}),
	)
	if err != nil {
		panic(err)
	}
}
