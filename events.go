package relp

import "time"
import "log"
import (
	"errors"
	"fmt"
)

type generalClientEvent struct {
	client  *Client
	event   string
	message string
	error   error
}

// String renders event as string
func (e *generalClientEvent) String() string {
	return fmt.Sprintf("ClientEvent[%v,%v,%v]",
		e.event, e.message, e.error)
}

// Client returns the client sending this message
func (e *generalClientEvent) Client() *Client {
	return e.client
}

// Error returns the error contained within this event
func (e *generalClientEvent) Error() error {
	return e.error
}

// Event returns a concise string name of the event
func (e *generalClientEvent) Event() string {
	return e.event
}

// Message returns a more detailed string description of the event
func (e *generalClientEvent) Message() string {
	return e.message
}

// send an event on the listener channel
func (c *Client) notify(event ClientEvent) error {
	if c.eventChan == nil {
		return nil
	}

	select {
	case c.eventChan <- event:
		return nil
	case <-time.After(100 * time.Millisecond):
		log.Print("notify() timed out after 100ms")
		return errors.New("notify() timed out after 100ms")
	}
}

// send a standard error object on the notification channel
func (c *Client) notifyError(err error) {
	_ = c.notify(&generalClientEvent{
		client:  c,
		event:   "error",
		message: err.Error(),
		error:   err,
	})
}

// notify the user of the Client that it has been closed
func (c *Client) notifyClose() error {
	return c.notifyEvent("serverclose", "server closed connection")
}

// send a general event given a string event name adn message
func (c *Client) notifyEvent(event string, message string) error {
	return c.notify(&generalClientEvent{
		client:  c,
		event:   event,
		message: message,
		error:   nil,
	})
}
