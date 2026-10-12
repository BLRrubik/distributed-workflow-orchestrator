package event_bus

type Handler func(e Event)

type Subscriber struct {
	id      string
	handler Handler
	queue   chan Event
	close   chan struct{}
}

func NewSubscriber(id string, handler Handler) *Subscriber {
	return &Subscriber{
		id:      id,
		handler: handler,
		queue:   make(chan Event, 128),
		close:   make(chan struct{}),
	}
}
