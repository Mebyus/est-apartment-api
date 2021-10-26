package cleaner

import (
	"log"
	"math"
	"sync"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/ozonmp/omp-demo-api/internal/app/repo"
)

type Cleaner interface {
	Start()
	Close()
}

type cleaner struct {
	n         uint64
	interval  time.Duration
	batchSize uint64

	repo             repo.EventRepo
	eventIDsToUnlock <-chan uint64
	eventIDsToRemove <-chan uint64

	workerPool *workerpool.WorkerPool

	wg   *sync.WaitGroup
	done chan bool
}

type Config struct {
	Repo             repo.EventRepo
	EventIDsToUnlock <-chan uint64
	EventIDsToRemove <-chan uint64
	WorkerCount      uint64
	Interval         time.Duration
	BatchSize        uint64
}

func NewCleaner(config Config) Cleaner {
	wg := &sync.WaitGroup{}
	done := make(chan bool)
	if config.WorkerCount > math.MaxInt {
		panic("number of workers is too big")
	}
	workerPool := workerpool.New(int(config.WorkerCount))

	return &cleaner{
		n:         config.WorkerCount,
		interval:  config.Interval,
		batchSize: config.BatchSize,

		repo:             config.Repo,
		eventIDsToUnlock: config.EventIDsToUnlock,
		eventIDsToRemove: config.EventIDsToRemove,

		workerPool: workerPool,
		wg:         wg,
		done:       done,
	}
}

func (c *cleaner) flushUnlock(eventIDs []uint64) {
	if len(eventIDs) == 0 {
		return
	}
	c.workerPool.Submit(func() {
		err := c.repo.Unlock(eventIDs)
		if err != nil {
			log.Printf("Failed to unlock events %v: %v", eventIDs, err)
		}
	})
}

func (c *cleaner) unlock() {
	defer c.wg.Done()
	var ids []uint64
	ticker := time.NewTicker(c.interval)
	for {
		select {
		case <-ticker.C:
			c.flushUnlock(ids)
			ids = nil
		case id, ok := <-c.eventIDsToUnlock:
			if !ok {
				c.flushUnlock(ids)
				return
			}
			ids = append(ids, id)
			if uint64(len(ids)) == c.batchSize {
				c.flushUnlock(ids)
				ids = nil
			}
		case <-c.done:
			c.flushUnlock(ids)
			return
		}
	}
}

func (c *cleaner) flushRemove(eventIDs []uint64) {
	if len(eventIDs) == 0 {
		return
	}
	c.workerPool.Submit(func() {
		err := c.repo.Remove(eventIDs)
		if err != nil {
			log.Printf("Failed to remove events %v: %v", eventIDs, err)
		}
	})
}

func (p *cleaner) remove() {
	defer p.wg.Done()
	var ids []uint64
	ticker := time.NewTicker(p.interval)
	for {
		select {
		case <-ticker.C:
			p.flushRemove(ids)
			ids = nil
		case id, ok := <-p.eventIDsToRemove:
			if !ok {
				p.flushRemove(ids)
				return
			}
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

func (c *cleaner) Start() {
	c.wg.Add(1)
	go c.unlock()

	c.wg.Add(1)
	go c.remove()
}

func (c *cleaner) Close() {
	close(c.done)
	c.wg.Wait()
	c.workerPool.StopWait()
}
