package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/ozonmp/omp-demo-api/internal/app/retranslator"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	cfg := retranslator.Config{
		ChannelSize:   512,
		ConsumerCount: 2,
		ConsumeSize:   10,
		ProducerCount: 28,
		WorkerCount:   2,
	}

	retranslator := retranslator.NewRetranslator(cfg)
	retranslator.Start()

	// make a graceful shutdown
	<-sigs
	retranslator.Close()
}
