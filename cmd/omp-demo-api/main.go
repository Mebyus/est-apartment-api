package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ozonmp/omp-demo-api/internal/app/retranslator"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	cfg := retranslator.Config{
		ChannelSize:     512,
		ConsumerCount:   2,
		ConsumeSize:     10,
		ConsumeInterval: 10 * time.Second,
		ProducerCount:   28,
		WorkerCount:     2,
		CleanupSize:     10,
		CleanupInterval: time.Second,
	}

	retranslator := retranslator.NewRetranslator(cfg)
	retranslator.Start()

	// make a graceful shutdown
	receivedSignal := <-sigs
	retranslator.Close()
	log.Printf("Stopped by signal: (%d) %v", receivedSignal, receivedSignal)
}
