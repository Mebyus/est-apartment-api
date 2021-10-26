package producer

import (
	"log"
	"sync"
	"time"

	"github.com/ozonmp/omp-demo-api/internal/app/repo"
	"github.com/ozonmp/omp-demo-api/internal/app/sender"
	"github.com/ozonmp/omp-demo-api/internal/model"

	"github.com/gammazero/workerpool"
)

type Producer interface {
	Start()
	Close()
}

type producer struct {
	n         uint64
	interval  time.Duration
	batchSize uint64

	sender           sender.EventSender
	repo             repo.EventRepo
	events           <-chan model.ApartmentEvent
	eventIDsToUnlock chan uint64
	eventIDsToRemove chan uint64

	workerPool *workerpool.WorkerPool

	wg   *sync.WaitGroup
	done chan bool
}

type Config struct {
	ProducersNumber uint64
	Repo            repo.EventRepo
	Sender          sender.EventSender
	Events          <-chan model.ApartmentEvent
	WorkerPool      *workerpool.WorkerPool
	Interval        time.Duration
	BatchSize       uint64
}

func NewKafkaProducer(config Config) Producer {
	wg := &sync.WaitGroup{}
	done := make(chan bool)
	eventIDsToUnlock := make(chan uint64)
	eventIDsToRemove := make(chan uint64)

	return &producer{
		n:                config.ProducersNumber,
		interval:         config.Interval,
		batchSize:        config.BatchSize,
		sender:           config.Sender,
		repo:             config.Repo,
		events:           config.Events,
		workerPool:       config.WorkerPool,
		eventIDsToUnlock: eventIDsToUnlock,
		eventIDsToRemove: eventIDsToRemove,
		wg:               wg,
		done:             done,
	}
}

func (p *producer) flushUnlock(eventIDs []uint64) {
	p.workerPool.Submit(func() {
		err := p.repo.Unlock(eventIDs)
		if err != nil {
			log.Printf("Failed to unlock events %v: %v", eventIDs, err)
		}
	})
}

func (p *producer) unlock() {
	defer p.wg.Done()
	var ids []uint64
	ticker := time.NewTicker(p.interval)
	for {
		select {
		case <-ticker.C:
			p.flushUnlock(ids)
			ids = nil
		case id := <-p.eventIDsToUnlock:
			ids = append(ids, id)
			if uint64(len(ids)) == p.batchSize {
				p.flushUnlock(ids)
				ids = nil
			}
		case <-p.done:
			p.flushUnlock(ids)
			return
		}
	}
}

func (p *producer) flushRemove(eventIDs []uint64) {
	p.workerPool.Submit(func() {
		err := p.repo.Remove(eventIDs)
		if err != nil {
			log.Printf("Failed to remove events %v: %v", eventIDs, err)
		}
	})
}

func (p *producer) remove() {
	defer p.wg.Done()
	var ids []uint64
	ticker := time.NewTicker(p.interval)
	for {
		select {
		case <-ticker.C:
			p.flushRemove(ids)
			ids = nil
		case id := <-p.eventIDsToRemove:
			ids = append(ids, id)
			if uint64(len(ids)) == p.batchSize {
				p.flushRemove(ids)
				ids = nil
			}
		case <-p.done:
			p.flushRemove(ids)
			return
		}
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
		case event := <-p.events:
			p.handle(event)
		case <-p.done:
			return
		}
	}
}

func (p *producer) Start() {
	p.wg.Add(1)
	go p.unlock()

	p.wg.Add(1)
	go p.remove()

	for i := uint64(0); i < p.n; i++ {
		p.wg.Add(1)
		go p.produce()
	}
}

func (p *producer) Close() {
	// TODO
	// Current implementation has syncronization problems in
	// closing phase. It is worth to consider moving cleanup
	// facility to a separate object
	close(p.done)
	p.wg.Wait()
}
