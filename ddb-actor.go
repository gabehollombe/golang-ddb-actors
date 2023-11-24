package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func main() {
	/*
		Until we write some tests, this serves as a simple harness for the concept...
	*/

	// Init a local DynamoDB config
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000"}, nil
			})),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
				Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
	)
	if err != nil {
		panic(err)
	}

	// Get a DynamoDB Actor repo
	ddb := NewActorRepositoryDdb("actors", cfg)

	// Useful for making sure we work on the same actor name in this test harness
	actorName := "a"

	// Make a new counting actor. It's job is just to count the number of messages it receives and keep track of that in its state.
	a := NewCountingActor(actorName)
	_, err = ddb.save(a.ActorBase)
	if err != nil {
		panic(err)
	}

	// Send some messages to the actor. We do this via the repo, and not directly in memory, because we are trying
	// to simulate the actor being on a different machine. A worker would pick up this actor's current state and all its unprocessed messages,
	// then handle the messages (in memory) and send the updated state (and acked message IDs) back to the repo.
	ddb.addMessage(a.ActorBase.ID, Message{Body: "hello"})
	ddb.addMessage(a.ActorBase.ID, Message{Body: "goodbye"})

	// Now we simulate a worker picking up the actor's state and messages from the repo.
	// TODO: eventually with something like ddb.getActorsWithMessages()
	// This should return a list of actors with messages in their inbox.
	// for now we just get the actor by name to keep things simpler...
	act, err := ddb.get(actorName)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Before starting work, actor looks like this from DB: %+v\n", act)

	// Tell the actor to process its messages.
	processedMessages, updatedState := act.processMessages()
	processedMessageIds := make([]MessageID, len(processedMessages))
	for i, m := range processedMessages {
		processedMessageIds[i] = m.ID
	}

	// Acknowledge the messages that were processed and submit updated state.
	_, err = ddb.finishedWork(a.ActorBase.ID, updatedState, processedMessageIds)
	if err != nil {
		panic(err)
	}

	// Here's another actor Get after our acknowledgement so we can see that updated state was persisted and messages were removed
	act, err = ddb.get(actorName)
	if err != nil {
		panic(err)
	}
	fmt.Printf("After completing work, actor looks like this from DB: %+v\n", act)
}
