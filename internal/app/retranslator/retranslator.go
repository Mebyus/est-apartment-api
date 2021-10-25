package retranslator

import (
	"time"

	"github.com/ozonmp/omp-demo-api/internal/app/consumer"
	"github.com/ozonmp/omp-demo-api/internal/app/producer"
	"github.com/ozonmp/omp-demo-api/internal/app/repo"
	"github.com/ozonmp/omp-demo-api/internal/app/sender"
	"github.com/ozonmp/omp-demo-api/internal/model"

	"github.com/gammazero/workerpool"
)

type Retranslator interface {
	Start()
	Close()
}

type Config struct {
	ChannelSize uint64

	ConsumerCount   uint64
	ConsumeSize     uint64
	ConsumeInterval time.Duration

	ProducerCount uint64
	WorkerCount   int

	Repo   repo.EventRepo
	Sender sender.EventSender
}

type retranslator struct {
	events     chan model.SubdomainEvent
	consumer   consumer.Consumer
	producer   producer.Producer
	workerPool *workerpool.WorkerPool
}

func NewRetranslator(cfg Config) Retranslator {
	events := make(chan model.SubdomainEvent, cfg.ChannelSize)
	workerPool := workerpool.New(cfg.WorkerCount)
	consumerConfig := consumer.Config{
		ConsumersNumber: cfg.ConsumerCount,
		BatchSize:       cfg.ConsumeSize,
		Interval:        cfg.ConsumeInterval,
		Repo:            cfg.Repo,
		Events:          events,
	}
	producerConfig := producer.Config{
		ProducersNumber: cfg.ProducerCount,
		Sender:          cfg.Sender,
		Events:          events,
		WorkerPool:      workerPool,
	}

	consumer := consumer.NewDBConsumer(consumerConfig)
	producer := producer.NewKafkaProducer(producerConfig)

	return &retranslator{
		events:     events,
		consumer:   consumer,
		producer:   producer,
		workerPool: workerPool,
	}
}

func (r *retranslator) Start() {
	r.producer.Start()
	r.consumer.Start()
}

func (r *retranslator) Close() {
	r.consumer.Close()
	r.producer.Close()
	r.workerPool.StopWait()
}
