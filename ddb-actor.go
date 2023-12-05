package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func main() {
	/*
		Until we write some tests, this serves as a simple harness for the concept...
	*/

	// Init a local AWS config
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:4566"}, nil
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

	sqsQueueUrl := "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/ActorMessages.fifo"
	queue := NewSqsFifoQueueClient(sqsQueueUrl, cfg)

	// Get a DynamoDB Actor repo
	ddb := NewActorRepositoryDdb("actors", cfg)

	// Useful for making sure we work on the same actor name in this test harness
	actorName := "a"

	// Make a new counting actor. It's job is just to count the number of messages it receives and keep track of that in its state.
	a := NewCountingActor(actorName)
	_, err = ddb.save(a.ID, a.State)
	if err != nil {
		panic(err)
	}

	// Send some messages to the actor. We do this via the queue, and not directly in memory, because we are trying
	// to simulate the actor being on a different machine. A worker would pick up this actor's current state and all its unprocessed messages,
	// then handle the messages (in memory) and send the updated state (and acked message IDs) back to the repo.
	queue.putMessage("hello", actorName, "dedupe-5")
	queue.putMessage("goodbye", actorName, "dedupe-6")

	// Now we simulate a worker picking up all of the actors with messages in their inbox
	// and processing those messages. The worker will then send the updated state
	// and acked message IDs back to the repo.
	messages, err := queue.getMessages()
	if err != nil {
		panic(err)
	}

	for _, msg := range messages {
		log.Printf("Processing message: %+v\n", msg)
		act, err := ddb.get(msg.To)
		if err != nil {
			panic(err)
		}

		// No need to lock the actor because we are using an SQS Fifo queue

		fmt.Printf("Before starting work, actor looks like this from DB: %+v\n", act)

		// Tell the actor to process its messages.
		processedMessage, updatedState := act.processMessage(msg)

		// Submit the updated state and, if that succeeds, remove the messages from the queue
		_, err = ddb.save(a.ID, updatedState)
		if err != nil {
			panic(err)
		}

		_, err = queue.acknowledgeMessage(processedMessage.ID)
		if err != nil {
			panic(err)
		}
	}

	// Here's another actor Get after our acknowledgement so we can see that updated state was persisted and messages were removed
	act, err := ddb.get(actorName)
	if err != nil {
		panic(err)
	}
	fmt.Printf("After completing work, actor looks like this from DB: %+v\n", act)
}
