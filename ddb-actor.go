package main

import "fmt"

type ActorID = string

type ActorRepository struct {
	Records map[ActorID]Actor
}

func NewActorRepository() ActorRepository {
	return ActorRepository{
		Records: make(map[string]Actor),
	}
}

func (r *ActorRepository) save(a Actor) {
	r.Records[a.ID] = a
	fmt.Printf("Repo saved: %+v \n", a)
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

func main() {
	// identityFunc := func(a *Actor, m Message) []Message { return []Message{m} }

	messageCountFunc := func(a *Actor, m Message) []Message {
		a.State["count"] = (a.State["count"]).(int) + 1
		return []Message{}
	}

	// New Actor
	a := NewActor("a", messageCountFunc, map[string]interface{}{"count": 0})

	// Add actor to repo
	repo := NewActorRepository()
	repo.save(a)

	// Send some messages
	a.addMessage("hello")
	a.addMessage("goodbye")
	repo.save(a)

	// Ask the actor to do some work

	a.processInbox()

	// Persist the new state of the actor along with the handle messages removed (TODO and eventually the OUT messages)
	a.Inbox = nil
	repo.save(a)

	// fmt.Printf("Actor state: %+v, Outs: %+v \n", a.State, outs)
	// fmt.Printf("Repo: %+v \n", repo)
}
