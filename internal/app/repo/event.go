package repo

import (
	"github.com/ozonmp/omp-demo-api/internal/model"
)

type EventRepo interface {
	Lock(n uint64) ([]model.ApartmentEvent, error)
	Unlock(eventIDs []uint64) error

	Add(event []model.ApartmentEvent) error
	Remove(eventIDs []uint64) error
}
