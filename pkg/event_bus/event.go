package event_bus

type Event interface {
	GetType() string
}
