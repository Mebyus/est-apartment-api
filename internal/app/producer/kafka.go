package producer

import (
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
	n       uint64
	timeout time.Duration

	sender sender.EventSender
	repo   repo.EventRepo
	events <-chan model.ApartmentEvent

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
}

func NewKafkaProducer(config Config) Producer {
	wg := &sync.WaitGroup{}
	done := make(chan bool)

	return &producer{
		n:          config.ProducersNumber,
		sender:     config.Sender,
		repo:       config.Repo,
		events:     config.Events,
		workerPool: config.WorkerPool,
		wg:         wg,
		done:       done,
	}
}

func (p *producer) handle(event model.ApartmentEvent) {
	if err := p.sender.Send(&event); err != nil {
		p.workerPool.Submit(func() {
			// ...
		})
	} else {
		p.workerPool.Submit(func() {
			// ...
		})
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
	for i := uint64(0); i < p.n; i++ {
		p.wg.Add(1)
		go p.produce()
	}
}

func (p *producer) Close() {
	close(p.done)
	p.wg.Wait()
}
