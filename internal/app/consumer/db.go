package consumer

import (
	"log"
	"sync"
	"time"

	"github.com/ozonmp/omp-demo-api/internal/app/repo"
	"github.com/ozonmp/omp-demo-api/internal/model"
)

type Consumer interface {
	Start()
	Close()
}

type consumer struct {
	n      uint64
	events chan<- model.ApartmentEvent

	repo repo.EventRepo

	batchSize uint64
	interval  time.Duration

	done chan bool
	wg   *sync.WaitGroup
}

type Config struct {
	ConsumersNumber uint64
	ChannelSize     uint64
	Repo            repo.EventRepo
	BatchSize       uint64
	Interval        time.Duration
}

func NewDBConsumer(config Config) (Consumer, <-chan model.ApartmentEvent) {
	wg := &sync.WaitGroup{}
	done := make(chan bool)
	events := make(chan model.ApartmentEvent, config.ChannelSize)

	return &consumer{
			n:         config.ConsumersNumber,
			batchSize: config.BatchSize,
			interval:  config.Interval,
			repo:      config.Repo,
			events:    events,
			wg:        wg,
			done:      done,
		},
		events
}

func (c *consumer) consume() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.interval)
	for {
		select {
		case <-ticker.C:
			events, err := c.repo.Lock(c.batchSize)
			if err != nil {
				log.Printf("Failed to acquire repo lock: %v", err)
				continue
			}
			for _, event := range events {
				c.events <- event
			}
		case <-c.done:
			return
		}
	}
}

func (c *consumer) Start() {
	for i := uint64(0); i < c.n; i++ {
		c.wg.Add(1)
		go c.consume()
	}
}

func (c *consumer) Close() {
	close(c.done)
	c.wg.Wait()
	close(c.events)
}
