package dispatcher

import (
	"testing"
	"time"
)

func TestNewDispatcher(t *testing.T) {
	d := New()
	if d.subscribers == nil {
		t.Error("Dispatcher subscribers is nil")
	}
	if d.subscribersMu == nil {
		t.Error("Dispatcher subscribersMu is nil")
	}
}

func TestAddSubscriber(t *testing.T) {
	d := New()
	d.AddSubscriber()
	if len(d.subscribers) != 1 {
		t.Error("Dispatcher subscribers length is not 1")
	}
}

func TestCloseSubscriber(t *testing.T) {
	d := New()
	_, closeCh := d.AddSubscriber()
	close(closeCh)

	deadline := time.Now().Add(time.Second)
	for {
		d.subscribersMu.Lock()
		count := len(d.subscribers)
		d.subscribersMu.Unlock()
		if count == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Dispatcher subscribers length is not 0")
		}
		time.Sleep(time.Millisecond)
	}
}
