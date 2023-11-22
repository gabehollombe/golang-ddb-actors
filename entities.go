package main

type ActorID = string

type Message = string
type ProcessFunc func(*Actor, Message) []Message

type Actor struct {
	ID          ActorID
	Inbox       []Message
	State       map[string]interface{}
	ProcessFunc ProcessFunc
}

func NewActor(id ActorID, processFn ProcessFunc, state map[string]interface{}) Actor {
	return Actor{
		ID:          id,
		Inbox:       make([]Message, 0),
		ProcessFunc: processFn,
		State:       state,
	}
}

func (a *Actor) addMessage(m Message) {
	a.Inbox = append(a.Inbox, m)
}

func (a *Actor) processInbox() []Message {
	outs := make([]Message, 0)

	// Process all messages in inbox
	for _, m := range a.Inbox {
		res := a.ProcessFunc(a, m)
		outs = append(outs, res...)
	}

	// Return any messages to send out
	return outs
}