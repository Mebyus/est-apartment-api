package retranslator

import (
	"time"

	"github.com/ozonmp/omp-demo-api/internal/app/cleaner"
	"github.com/ozonmp/omp-demo-api/internal/app/consumer"
	"github.com/ozonmp/omp-demo-api/internal/app/producer"
	"github.com/ozonmp/omp-demo-api/internal/app/repo"
	"github.com/ozonmp/omp-demo-api/internal/app/sender"
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

	CleanupSize     uint64
	CleanupInterval time.Duration

	ProducerCount uint64
	WorkerCount   uint64

	Repo   repo.EventRepo
	Sender sender.EventSender
}

type retranslator struct {
	consumer consumer.Consumer
	producer producer.Producer
	cleaner  cleaner.Cleaner
}

func NewRetranslator(cfg Config) Retranslator {
	consumerConfig := consumer.Config{
		ConsumersNumber: cfg.ConsumerCount,
		BatchSize:       cfg.ConsumeSize,
		Interval:        cfg.ConsumeInterval,
		Repo:            cfg.Repo,
	}
	consumer, events := consumer.NewDBConsumer(consumerConfig)

	producerConfig := producer.Config{
		ProducersNumber: cfg.ProducerCount,
		Sender:          cfg.Sender,
		Events:          events,
		BatchSize:       cfg.CleanupSize,
	}
	producer, byproductChannels := producer.NewKafkaProducer(producerConfig)

	cleanerConfig := cleaner.Config{
		Repo:             cfg.Repo,
		WorkerCount:      cfg.WorkerCount,
		Interval:         cfg.CleanupInterval,
		BatchSize:        cfg.CleanupSize,
		EventIDsToUnlock: byproductChannels.EventIDsToUnlock,
		EventIDsToRemove: byproductChannels.EventIDsToRemove,
	}
	cleaner := cleaner.NewCleaner(cleanerConfig)

	return &retranslator{
		consumer: consumer,
		producer: producer,
		cleaner:  cleaner,
	}
}

func (r *retranslator) Start() {
	r.producer.Start()
	r.consumer.Start()
	r.cleaner.Start()
}

func (r *retranslator) Close() {
	r.consumer.Close()
	r.producer.Close()
	r.cleaner.Close()
}
