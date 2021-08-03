package main

import (
	"fastdb"
	"fastdb/cmd"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func init() {
	logo, _ := ioutil.ReadFile("../../logo.txt")
	fmt.Println(string(logo))
}

var config = flag.String("config", "", "the config file for fastdb")

func main() {

	flag.Parse()

	// Set the config.
	var cfg fastdb.Config
	if *config == "" {
		log.Println("no config set, using the default config.")
		cfg = fastdb.DefaultConfig()
	}
	//fmt.Println(cfg)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	server, err := 
	if err != nil {
		log.Printf("create rosedb server err: %+v\n", err)
		return
	}

	go server.Listen(cfg.Addr)

	// server.ln.Close()
	<-sig
	log.Println("rosedb is ready to exit, bye...")

}
