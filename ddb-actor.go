package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func main() {
	// identityFunc := func(a *Actor, m Message) []Message { return []Message{m} }

	messageCountFunc := func(a *Actor, m Message) []Message {
		a.State["count"] = (a.State["count"]).(int) + 1
		return []Message{}
	}

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

	// New Actor
	actorName := "a"
	a := NewActor(actorName, messageCountFunc, map[string]interface{}{"count": 2})

	// Poke the Actor a bit...
	// Send some messages
	// a.addMessage("hello")
	// a.addMessage("goodbye")
	// // Ask the actor to do some work
	// a.processInbox()

	// Save actor to DDB
	_, err = ddb.save(a)
	if err != nil {
		panic(err)
	}

	// Get after save to see that save is working
	a, err = ddb.get("a")
	if err != nil {
		panic(err)
	}
	fmt.Printf("fetched actor: %+v\n", a)

	ok, err := ddb.addMessage(a.ID, "hello")
	if !ok {
		panic(err)
	}

	a, err = ddb.get("a")
	if err != nil {
		panic(err)
	}
	fmt.Printf("fetched actor: %+v\n", a)

	// fmt.Printf("Actor state: %+v, Outs: %+v \n", a.State, outs)
	// fmt.Printf("Repo: %+v \n", repo)

	// ddb.finishedWork(a.ID)
}
