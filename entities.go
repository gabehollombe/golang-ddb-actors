package main

type ActorID = string
type MessageID = string

type Message struct {
	ID   MessageID
	To   ActorID
	Body string
}

type MessageProcessor interface {
	processMessage(Message) (Message, map[string]interface{})
	processMessages() ([]Message, map[string]interface{})
}

type Actor interface {
	MessageProcessor
}

type ActorBase struct {
	ID    ActorID
	Inbox []Message
	State map[string]interface{}
}

type CountingActor struct {
	ActorBase
}

func NewCountingActor(id ActorID) CountingActor {
	state := map[string]interface{}{"_type": "counting", "count": 0}
	return CountingActor{
		ActorBase: NewActorBase(id, state, []Message{}),
	}
}
func NewCountingActorFromBase(a ActorBase) CountingActor {
	return CountingActor{
		ActorBase: NewActorBase(a.ID, a.State, a.Inbox),
	}
}

// Returns the message if it processed it. Otherwise returns nil. TODO: come up with better error handling?
func (a CountingActor) processMessage(message Message) (Message, map[string]interface{}) {
	workingState := a.State
	// NOTE: all numbers are marshalled/unmarshalled as float64 by the dynamodb SDK. The consumer needs to cast to int or float as needed.
	workingState["count"] = (workingState["count"]).(float64) + 1
	return message, workingState
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

func NewActorBase(id ActorID, state map[string]interface{}, messages []Message) ActorBase {
	return ActorBase{
		ID:    id,
		Inbox: messages,
		State: state,
	}
}
