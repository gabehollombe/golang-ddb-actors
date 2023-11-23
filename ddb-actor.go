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

	// New Actor
	a := NewActor("alex", messageCountFunc, map[string]interface{}{"count": 2})

	// Add actor to repo
	// repo := NewActorRepositoryInMem()
	// repo.save(a)

	// Send some messages
	a.addMessage("hello")
	a.addMessage("goodbye")
	// repo.save(a)

	// // Ask the actor to do some work

	// a.processInbox()

	// // Persist the new state of the actor along with the handle messages removed (TODO and eventually the OUT messages)
	// a.Inbox = nil
	// repo.save(a)

	// Try DDB
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
	ddb := NewActorRepositoryDdb("actors", cfg)
	_, err = ddb.dsave(a)
	if err != nil {
		panic(err)
	}
	// ddb.scan2()

	found, err := ddb.dget("alex")
	if err != nil {
		panic(err)
	}
	fmt.Printf("dfetched actor: %+v\n", found)

	// fmt.Printf("Actor state: %+v, Outs: %+v \n", a.State, outs)
	// fmt.Printf("Repo: %+v \n", repo)
}
