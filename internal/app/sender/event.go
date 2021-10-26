package sender

import (
	"github.com/ozonmp/omp-demo-api/internal/model"
)

type EventSender interface {
	Send(apartment model.ApartmentEvent) error
}
