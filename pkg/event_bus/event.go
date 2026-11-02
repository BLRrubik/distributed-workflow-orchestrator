package eventbus

type Event interface {
	GetType() string
}
