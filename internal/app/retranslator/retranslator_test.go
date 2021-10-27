package retranslator

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ozonmp/omp-demo-api/internal/mocks"
	"github.com/ozonmp/omp-demo-api/internal/model"
)

type idGenerator struct {
	next uint64
	mu   sync.Mutex
}

func (g *idGenerator) generate() (id uint64) {
	g.mu.Lock()
	id = g.next
	g.next++
	g.mu.Unlock()
	return
}

func TestEmptyStartAndClose(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	repo.EXPECT().Lock(gomock.Any()).AnyTimes()

	cfg := Config{
		ChannelSize: 512,

		ConsumerCount: 2,
		ProducerCount: 2,
		WorkerCount:   2,

		ConsumeSize: 10,
		CleanupSize: 10,

		ConsumeInterval: time.Second,
		CleanupInterval: time.Second,

		Repo:   repo,
		Sender: sender,
	}

	retranslator := NewRetranslator(cfg)
	retranslator.Start()
	retranslator.Close()
}

func TestOnlyCreateEventsWithNoErrors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	generator := idGenerator{
		next: 1,
	}

	repo.EXPECT().Lock(gomock.Any()).AnyTimes().DoAndReturn(func(n uint64) (events []model.ApartmentEvent, err error) {
		for i := uint64(0); i < n; i++ {
			events = append(events, model.ApartmentEvent{
				ID:   generator.generate(),
				Type: model.Created,
			})
		}

		// imitate time consuming call to repo
		time.Sleep(200 * time.Millisecond)
		return
	})

	sender.EXPECT().Send(gomock.Any()).AnyTimes().DoAndReturn(func(apartment model.ApartmentEvent) error {
		// imitate time consuming call to sender
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	repo.EXPECT().Remove(gomock.Any()).AnyTimes().DoAndReturn(func(eventIDs []uint64) error {
		// imitate time consuming call to repo
		time.Sleep(150 * time.Millisecond)
		return nil
	})

	repo.EXPECT().Unlock(gomock.Any()).AnyTimes().Do(func(eventIDs []uint64) {
		panic("unexpected call to repo.Unlock()")
	})

	cfg := Config{
		ChannelSize: 16,

		ConsumerCount: 10,
		ProducerCount: 10,
		WorkerCount:   10,

		ConsumeSize: 5,
		CleanupSize: 5,

		ConsumeInterval: 100 * time.Millisecond,
		CleanupInterval: 100 * time.Millisecond,

		Repo:   repo,
		Sender: sender,
	}

	retranslator := NewRetranslator(cfg)
	retranslator.Start()

	// work for some time and stop
	time.Sleep(5 * time.Second)
	retranslator.Close()
}

func TestDifferentEventsWithErrors(t *testing.T) {
	t.Parallel()

	const eventTypeFactor = 6
	const sendErrorFactor = 11

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	generator := idGenerator{
		next: 1,
	}

	repo.EXPECT().Lock(gomock.Any()).AnyTimes().DoAndReturn(func(n uint64) (events []model.ApartmentEvent, err error) {
		for i := uint64(0); i < n; i++ {
			id := generator.generate()
			eventType := model.Created
			if id%eventTypeFactor == 0 {
				eventType = model.Updated
			}
			events = append(events, model.ApartmentEvent{
				ID:   id,
				Type: eventType,
			})
		}

		// imitate time consuming call to repo
		time.Sleep(200 * time.Millisecond)
		return
	})

	sender.EXPECT().Send(gomock.Any()).AnyTimes().DoAndReturn(func(apartment model.ApartmentEvent) error {
		// imitate time consuming call to sender
		time.Sleep(50 * time.Millisecond)

		if apartment.ID%sendErrorFactor == 0 {
			return fmt.Errorf("failed to send event")
		}
		return nil
	})

	repo.EXPECT().Remove(gomock.Any()).AnyTimes().DoAndReturn(func(eventIDs []uint64) error {
		// imitate time consuming call to repo
		time.Sleep(150 * time.Millisecond)

		for _, id := range eventIDs {
			if id%eventTypeFactor == 0 || id%sendErrorFactor == 0 {
				panic(fmt.Sprintf("tried to remove event with wrong id = %d", id))
			}
		}
		return nil
	})

	repo.EXPECT().Unlock(gomock.Any()).AnyTimes().DoAndReturn(func(eventIDs []uint64) error {
		// imitate time consuming call to repo
		time.Sleep(100 * time.Millisecond)

		for _, id := range eventIDs {
			if !(id%eventTypeFactor == 0 || id%sendErrorFactor == 0) {
				panic(fmt.Sprintf("tried to unlock event with wrong id = %d", id))
			}
		}
		return nil
	})

	cfg := Config{
		ChannelSize: 16,

		ConsumerCount: 10,
		ProducerCount: 10,
		WorkerCount:   10,

		ConsumeSize: 5,
		CleanupSize: 5,

		ConsumeInterval: 100 * time.Millisecond,
		CleanupInterval: 100 * time.Millisecond,

		Repo:   repo,
		Sender: sender,
	}

	retranslator := NewRetranslator(cfg)
	retranslator.Start()

	// work for some time and stop
	time.Sleep(5 * time.Second)
	retranslator.Close()
}
