package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func main() {
	// identityFunc := func(a *ActorBase, m Message) []Message { return []Message{m} }

	// Init local DDB config
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

	// Set up DDB repo
	ddb := NewActorRepositoryDdb("actors", cfg)

	// New ActorBase
	actorName := "a"
	a := NewCountingActor(actorName)
	_, err = ddb.save(a.ActorBase)
	if err != nil {
		panic(err)
	}

	// Send some messages (via the repo)
	ddb.addMessage(a.ActorBase.ID, Message{Body: "hello"})
	ddb.addMessage(a.ActorBase.ID, Message{Body: "goodbye"})

	// TODO: ddb.getActorsWithMessages()
	// This will return a list of actors with messages in their inbox.
	// for now we just get this one explicitly...
	act, err := ddb.get(actorName)
	if err != nil {
		panic(err)
	}
	fmt.Printf("FETCHED actor before their work: %+v\n", act)

	// Dispatch the messages to the actor.
	processedMessages, updatedState := act.processMessages()
	processedMessageIds := make([]MessageID, len(processedMessages))
	for i, m := range processedMessages {
		processedMessageIds[i] = m.ID
	}

	// Have actor acknowledge work on their messages
	fmt.Printf("Updated state: %v \n", updatedState)
	_, err = ddb.finishedWork(a.ActorBase.ID, updatedState, processedMessageIds)
	if err != nil {
		panic(err)
	}

	// Get after save to see that save is working
	act, err = ddb.get(actorName)
	if err != nil {
		panic(err)
	}
	fmt.Printf("FETCHED actor after their work: %+v\n", act)

	// _, err = ddb.addMessage(act.ID, Message{Body: "hello"})
	// if err != nil {
	// 	panic(err)
	// }

	// act, err = ddb.get(actorName)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("fetched actor: %+v\n", act)

	// fmt.Printf("ActorBase state: %+v, Outs: %+v \n", a.State, outs)
	// fmt.Printf("Repo: %+v \n", repo)

	// ddb.finishedWork(a.ID)
}
