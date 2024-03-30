package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
)

var enableCpuProfile = flag.Bool("cpuprofile", false, "enable CPU profiling")
var debug = flag.Bool("debug", false, "enable debug log")

func main() {
	flag.Parse()

	if *enableCpuProfile {
		f, err := os.Create("cpu.prof")
		if err != nil {
			panic(err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			panic(err)
		}
		sigch := make(chan os.Signal, 2)
		signal.Notify(sigch, os.Interrupt, syscall.SIGTERM) // subscribe to system signals
		onKill := func(c chan os.Signal) {
			select {
			case <-c:
				fmt.Println("Exiting")
				defer os.Exit(0)
				defer f.Close()
				defer pprof.StopCPUProfile()
			}
		}
		go onKill(sigch)
	}

	err := Listen("tcp", "localhost", DefaultPort, TcpConnAdaptor(
		TcpDataHandler{
			OnData: func(remote net.Addr, buf []byte) []byte {
				if *debug {
					fmt.Printf("Received from %v:\n%s\n", remote.String(), buf)
				}
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
