package model

type Subdomain struct {
	ID uint64
}

type SubdomainEvent struct {
	ID     uint64
	Type   EventType
	Status EventStatus
	Entity *Subdomain
}
