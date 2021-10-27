package producer

import (
	"log"
	"sync"

	"github.com/ozonmp/omp-demo-api/internal/app/sender"
	"github.com/ozonmp/omp-demo-api/internal/model"
)

type Producer interface {
	Start()
	Close()
}

type producer struct {
	n         uint64
	batchSize uint64

	sender           sender.EventSender
	events           <-chan model.ApartmentEvent
	eventIDsToUnlock chan<- uint64
	eventIDsToRemove chan<- uint64

	wg   *sync.WaitGroup
	done chan bool
}

type Config struct {
	ProducersNumber uint64
	Sender          sender.EventSender
	Events          <-chan model.ApartmentEvent
	BatchSize       uint64
}

type ByproductChannels struct {
	EventIDsToUnlock <-chan uint64
	EventIDsToRemove <-chan uint64
}

func NewKafkaProducer(config Config) (Producer, ByproductChannels) {
	wg := &sync.WaitGroup{}
	done := make(chan bool)
	eventIDsToUnlock := make(chan uint64)
	eventIDsToRemove := make(chan uint64)

	return &producer{
			n:                config.ProducersNumber,
			batchSize:        config.BatchSize,
			sender:           config.Sender,
			events:           config.Events,
			eventIDsToUnlock: eventIDsToUnlock,
			eventIDsToRemove: eventIDsToRemove,
			wg:               wg,
			done:             done,
		},
		ByproductChannels{
			EventIDsToUnlock: eventIDsToUnlock,
			EventIDsToRemove: eventIDsToRemove,
		}
}

func (p *producer) handleCreate(event model.ApartmentEvent) {
	err := p.sender.Send(event)
	if err != nil {
		log.Printf("Failed to send event %d: %v", event.ID, err)
		p.eventIDsToUnlock <- event.ID
		return
	}
	p.eventIDsToRemove <- event.ID
}

func (p *producer) handleDefault(event model.ApartmentEvent) {
	p.eventIDsToUnlock <- event.ID
}

func (p *producer) handle(event model.ApartmentEvent) {
	switch event.Type {
	case model.Created:
		p.handleCreate(event)
	default:
		// TODO
		// Maybe we should make a mechanism to only receive those
		// events which we can properly process. Right now we are
		// only able to unlock back events in repo. This will
		// will feed endless loop of useless locking and unclocking
		// events with statuses that we don't know how to handle.
		p.handleDefault(event)
	}
}

func (p *producer) produce() {
	defer p.wg.Done()
	for {
		select {
		case event, ok := <-p.events:
			if !ok {
				return
			}
			p.handle(event)
		case <-p.done:
			// we still need to handle events that were enqueued
			// before we started closing procedures
			//
			// TODO
			// Potentially it's a huge problem if events channel is not closed at this point.
			// In that case goroutine will get stuck in endless waiting. Here we can add a
			// timeout timer via select, that will lead to dropping some unhandled events though.
			for event := range p.events {
				p.handle(event)
			}
			return
		}
	}
}

func (p *producer) Start() {
	for i := uint64(0); i < p.n; i++ {
		p.wg.Add(1)
		go p.produce()
	}
}

func (p *producer) Close() {
	close(p.done)
	p.wg.Wait()
	close(p.eventIDsToUnlock)
	close(p.eventIDsToRemove)
}
