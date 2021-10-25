package producer

import (
	"sync"
	"time"

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
	events <-chan model.ApartmentEvent

	workerPool *workerpool.WorkerPool

	wg   *sync.WaitGroup
	done chan bool
}

type Config struct {
	ProducersNumber uint64
	Sender          sender.EventSender
	Events          <-chan model.ApartmentEvent
	WorkerPool      *workerpool.WorkerPool
}

// todo for students: add repo
func NewKafkaProducer(config Config) Producer {
	wg := &sync.WaitGroup{}
	done := make(chan bool)

	return &producer{
		n:          config.ProducersNumber,
		sender:     config.Sender,
		events:     config.Events,
		workerPool: config.WorkerPool,
		wg:         wg,
		done:       done,
	}
}

func (p *producer) produce() {
	defer p.wg.Done()
	for {
		select {
		case event := <-p.events:
			if err := p.sender.Send(&event); err != nil {
				p.workerPool.Submit(func() {
					// ...
				})
			} else {
				p.workerPool.Submit(func() {
					// ...
				})
			}
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
