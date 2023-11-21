package main

import "fmt"

type ActorID = string

type ActorRepository struct {
	Records map[ActorID]Actor
}

func (r *ActorRepository) save(a Actor) {
	r.Records[a.ID] = a
}
func (r *ActorRepository) get(id ActorID) Actor {
	return r.Records[id]
}

type Message = string
type ProcessFunc func(*Actor, Message) []Message

type Actor struct {
	ID          ActorID
	Inbox       []Message
	State       map[string]interface{}
	ProcessFunc ProcessFunc
}

func NewActor(id ActorID, processFn ProcessFunc, state map[string]interface{}) *Actor {
	return &Actor{
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
	for _, m := range a.Inbox {
		res := a.ProcessFunc(a, m)
		outs = append(outs, res...)
	}
	return outs
}

func main() {
	// identityFunc := func(a *Actor, m Message) []Message { return []Message{m} }

	messageCountFunc := func(a *Actor, m Message) []Message {
		a.State["count"] = (a.State["count"]).(int) + 1
		return []Message{}
	}

	a := NewActor("a", messageCountFunc, map[string]interface{}{"count": 0})
	a.addMessage("hello")
	a.addMessage("goodbye")
	outs := a.processInbox()
	fmt.Printf("Actor state: %v, Outs: %v", a.State, outs)
}
