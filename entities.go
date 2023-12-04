package main

type ActorID = string
type MessageID = string

type Message struct {
	ID   MessageID
	Body string
}

type MessageProcessor interface {
	processMessages() ([]Message, map[string]interface{})
}

type Actor interface {
	MessageProcessor
}

type ActorBase struct {
	ID    ActorID
	Inbox []Message
	State map[string]interface{}
	Type  string
}

type CountingActor struct {
	ActorBase
}

func NewCountingActor(id ActorID) CountingActor {
	state := map[string]interface{}{"count": 0}
	return CountingActor{
		ActorBase: NewActorBase(id, state, []Message{}, "counting", CountingActor{}),
	}
}
func NewCountingActorFromBase(a ActorBase) CountingActor {
	return CountingActor{
		ActorBase: NewActorBase(a.ID, a.State, a.Inbox, "counting", CountingActor{}),
	}
}

// Returns list of messages processed and a new updated state for persistence
func (a CountingActor) processMessages() ([]Message, map[string]interface{}) {
	processedMessages := make([]Message, 0)
	workingState := a.State
	for _, m := range a.Inbox {
		// NOTE: all numbers are marshalled/unmarshalled as float64 by the dynamodb SDK. The consumer needs to cast to int or float as needed.
		workingState["count"] = (workingState["count"]).(float64) + 1
		processedMessages = append(processedMessages, m)

	}
	return processedMessages, workingState
}

func NewActorBase(id ActorID, state map[string]interface{}, messages []Message, actorType string, typeDelegate MessageProcessor) ActorBase {
	return ActorBase{
		ID:    id,
		Inbox: messages,
		State: state,
		Type:  actorType,
	}
}
