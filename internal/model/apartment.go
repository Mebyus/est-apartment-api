package model

type Apartment struct {
	ID    uint64
	Title string
	Price uint64
}

type ApartmentEvent struct {
	ID     uint64
	Type   EventType
	Status EventStatus
	Entity *Apartment
}
