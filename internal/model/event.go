package model

type EventType uint8

type EventStatus uint8

const (
	Created EventType = iota
	Updated
	Removed
)

const (
	Deferred EventStatus = iota
	Processed
)
