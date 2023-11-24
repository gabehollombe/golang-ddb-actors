package main

type ActorID = string
type MessageID = string

type Message struct {
	ID   MessageID
	Body string
}
type ProcessFunc func(*ActorBase, Message) []Message

//	type MessageReceiver interface {
//		addMessage(Message)
//	}
type MessageProcessor interface {
	processMessages() ([]Message, map[string]interface{})
}

type Actor interface {
	// MessageReceiver
	MessageProcessor
}

type ActorBase struct {
	ID           ActorID
	Inbox        []Message
	State        map[string]interface{}
	Type         string
	TypeDelegate MessageProcessor
}

func (a *ActorBase) processMessages() ([]Message, map[string]interface{}) {
	return a.TypeDelegate.processMessages()
}

type CountingActor struct {
	ActorBase ActorBase
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

// Returns list of messages processed
func (a CountingActor) processMessages() ([]Message, map[string]interface{}) {
	processedMessages := make([]Message, 0)
	workingState := a.ActorBase.State
	for _, m := range a.ActorBase.Inbox {
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

// func (a *ActorBase) addMessage(m Message) {
// 	a.Inbox = append(a.Inbox, m)
// }

// func (a *ActorBase) processInbox() []Message {
// 	outs := make([]Message, 0)

// 	// Process all messages in inbox
// 	for _, m := range a.Inbox {
// 		res := a.ProcessFunc(a, m)
// 		outs = append(outs, res...)
// 	}

// 	// Return any messages to send out
// 	return outs
// }
