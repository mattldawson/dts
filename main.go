package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dts/config"
	"dts/core"
	"dts/services"
)

//go:generate mkdir -p services/docs
//go:generate redoc-cli bundle docs/openapi.yaml
//go:generate cp docs/openapi.yaml services/docs/openapi.yaml
//go:generate mv redoc-static.html services/docs/index.html

// The above logic generates openapi_doc.go as part of the docs package, and
// gives it an endpoint prefix of "docs". To enable these endpoints, you must
// use the "docs" build: go build -tags docs

// Prints usage info.
func usage() {
	fmt.Fprintf(os.Stderr, "%s: usage:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "%s <config_file>\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "See README.md for details on config files.\n")
	os.Exit(1)
}

func main() {

	// The only argument is the configuration filename.
	if len(os.Args) < 2 {
		usage()
	}
	configFile := os.Args[1]

	// Read the configuration file.
	log.Printf("Reading configuration from '%s'...\n", configFile)
	file, err := os.Open(configFile)
	if err != nil {
		log.Panicf("Couldn't open %s: %s\n", configFile, err.Error())
	}
	defer file.Close()

	b, err := ioutil.ReadAll(file)
	if err != nil {
		log.Panicf("Couldn't read configuration data: %s\n", err.Error())
	}

	// Initialize our configuration and create the service.
	initErr := core.Init(b)
	if initErr != nil {
		log.Panicf("Couldn't initialize the configuration: %s\n", initErr.Error())
	}
	service, err := services.NewDTSPrototype()
	if err != nil {
		log.Panicf("Couldn't create the service: %s\n", err.Error())
	}

	// Start the service in a goroutine so it doesn't block.
	go func() {
		err = service.Start(config.Service.Port)
		if err != nil {
			log.Println(err.Error())
		}
	}()

	// Intercept the SIGINT, SIGHUP, SIGTERM, and SIGQUIT signals, shutting down
	// the service as gracefully as possible if they are encountered.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGINT,
		syscall.SIGHUP,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	// Block till we receive one of the above signals.
	<-sigChan

	// Create a deadline to wait for.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Wait for connections to close until the deadline elapses.
	service.Shutdown(ctx)
	log.Println("Shutting down")
	os.Exit(0)
}
