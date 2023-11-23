package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type ActorRepository interface {
	save(a Actor)
	get(ActorID) Actor
}

type ActorRepositoryInMem struct {
	Records map[ActorID]Actor
}

func NewActorRepositoryInMem() ActorRepositoryInMem {
	return ActorRepositoryInMem{
		Records: make(map[string]Actor),
	}
}

func (r *ActorRepositoryInMem) save(a Actor) {
	r.Records[a.ID] = a
	fmt.Printf("Repo saved: %+v \n", a)

}

func (r *ActorRepositoryInMem) get(id ActorID) Actor {
	return r.Records[id]
}

// type ActorDTO struct {
// 	PK         string
// 	SK         string
// 	State      map[string]interface{}
// 	InboxCount int
// }

// func (a *Actor) toDTO() ActorDTO {
// 	return ActorDTO{
// 		PK: fmt.Sprintf("actor#%v", a.ID),
// 		SK: fmt.Sprintf("actor#%v", a.ID),
// 		State: a.State,
// 	}
// }

type ActorRepositoryDdb struct {
	TableName      string
	DynamoDbClient *dynamodb.Client
}

func NewActorRepositoryDdb(tableName string, sdkConfig aws.Config) ActorRepositoryDdb {
	return ActorRepositoryDdb{
		TableName:      tableName,
		DynamoDbClient: dynamodb.NewFromConfig(sdkConfig),
	}
}

func (r *ActorRepositoryDdb) getKey(id ActorID) map[string]types.AttributeValue {
	pk, err := attributevalue.Marshal(fmt.Sprintf("actor#%v", id))
	if err != nil {
		panic(err)
	}
	sk, err := attributevalue.Marshal(fmt.Sprintf("actor#%v", id))
	if err != nil {
		panic(err)
	}
	return map[string]types.AttributeValue{"pk": pk, "sk": sk}
}

func (r *ActorRepositoryDdb) makeUpdateItemInput(id string, state map[string]interface{}, inbox_count_adjustment int) (*dynamodb.UpdateItemInput, error) {
	update := expression.UpdateBuilder{}
	update = update.Set(expression.Name("state"), expression.Value(state))
	update = update.Add(expression.Name("inbox_count"), expression.Value(inbox_count_adjustment))

	// This is so we know if the item exists or not. If it doesn't we'll get a ConditionalCheckFailed exception.
	condition := expression.And(
		expression.Name("pk").Equal(expression.Value(fmt.Sprintf("actor#%v", id))),
		expression.Name("sk").Equal(expression.Value(fmt.Sprintf("actor#%v", id))),
	)

	expr, err := expression.
		NewBuilder().
		WithUpdate(update).
		WithCondition(condition).
		Build()

	if err != nil {
		log.Printf("Couldn't build expression for update. Here's why: %v\n", err)
		return nil, err
	}
	// log.Printf("Expression: %+v\n", expr)

	key := r.getKey(id)
	return &dynamodb.UpdateItemInput{
		TableName:                 aws.String(r.TableName),
		Key:                       key,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              types.ReturnValueUpdatedNew,
	}, nil
}

func (r *ActorRepositoryDdb) save(a Actor) (Actor, error) {
	// var err error
	// var response *dynamodb.UpdateItemOutput
	// var attributeMap map[string]map[string]interface{}
	updateItemInput, err := r.makeUpdateItemInput(a.ID, a.State, 0)
	fmt.Printf("UpdateItemInput: %+v\n", updateItemInput)
	if err != nil {
		return a, err
	}
	_, err = r.DynamoDbClient.UpdateItem(context.TODO(), updateItemInput)
	if err == nil {
		log.Printf("DDB Repo saved via update: %+v \n", a)
		return a, nil
	}

	// We have an error.
	// If it's not a CCF, we don't know what to do with it.
	var ccf *types.ConditionalCheckFailedException
	if !errors.As(err, &ccf) {
		log.Printf("Couldn't update actor %v+. Here's why: %v\n", a, err)
		return a, err
	}

	// This error is a CCF, which means the item doesn't exist yet. Let's create it...
	log.Print("Item doesn't exist yet. Creating...")

	// IMPORTANT: We only want to manage some attributes for the actor here (e.g. state, inbox_count).
	// So we put those in directly, not via the Actor struct.
	state, err := attributevalue.Marshal(a.State)
	if err != nil {
		panic(err)
	}
	item := make(map[string]types.AttributeValue)
	item["pk"] = &types.AttributeValueMemberS{Value: "actor#" + a.ID}
	item["sk"] = &types.AttributeValueMemberS{Value: "actor#" + a.ID}
	item["inbox_count"] = &types.AttributeValueMemberN{Value: "0"}
	item["state"] = state
	_, err = r.DynamoDbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(r.TableName), Item: item,
	})
	if err != nil {
		log.Printf("Couldn't add item to table. Here's why: %v\n", err)
		return a, err
	}
	return a, nil

	// err = attributevalue.UnmarshalMap(response.Attributes, &attributeMap)
	// if err != nil {
	// 	log.Printf("Couldn't unmarshall update response. Here's why: %v\n", err)
	// }
}

func (r *ActorRepositoryDdb) get(id ActorID) (Actor, error) {
	actor := Actor{}
	response, err := r.DynamoDbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key: r.getKey(id), TableName: aws.String(r.TableName),
	})
	if err != nil {
		log.Printf("Couldn't get Actor ID: %v. Here's why: %v\n", id, err)
	} else {
		err = attributevalue.UnmarshalMap(response.Item, &actor)
		if err != nil {
			log.Printf("Couldn't unmarshal response. Here's why: %v\n", err)
		}
	}
	return actor, err
}

func (r *ActorRepositoryDdb) addMessage(actorId ActorID, message Message) (bool, error) {
	// We'll use the current time in nanoseconds as the message ID
	// if this ends up existing, we'll try again until we succeed in the insert
	messageId := time.Now().UnixNano()

	// Start a transaction
	txOptions := &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					Item: map[string]types.AttributeValue{
						"pk":      &types.AttributeValueMemberS{Value: fmt.Sprintf("actor#%v", actorId)},
						"sk":      &types.AttributeValueMemberS{Value: fmt.Sprintf("message#%v", messageId)},
						"message": &types.AttributeValueMemberS{Value: message},
					},
					ConditionExpression: aws.String("attribute_not_exists(pk) AND attribute_not_exists(sk)"),
					TableName:           aws.String(r.TableName),
				},
			},
			{
				Update: &types.Update{
					TableName: aws.String(r.TableName),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("actor#%v", actorId)},
						"sk": &types.AttributeValueMemberS{Value: fmt.Sprintf("actor#%v", actorId)},
					},
					UpdateExpression: aws.String("ADD inbox_count :inc"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":inc": &types.AttributeValueMemberN{Value: "1"},
					},
				},
			},
		},
	}

	// Retry if we get a TransactionCanceledException
	// Only retry up to 3 times
	maxRetries := 3
	retryCount := 0
	for retryCount < maxRetries {
		_, err := r.DynamoDbClient.TransactWriteItems(context.TODO(), txOptions)
		if err == nil {
			return true, nil
		}

		if err != nil {
			var tce *types.TransactionCanceledException

			// If it's not a TCE, we don't know what to do with it.
			if !errors.As(err, &tce) {
				return false, err
			}

			// It's a TCE. Let's retry...
			log.Printf("Transaction canceled. Retrying...")
			retryCount++
		}
	}

	return false, errors.New("max retries exceeded")
}

func (r *ActorRepositoryDdb) getLastMessageID(id ActorID) (int, error) {
	// TODO: Implement the logic to get the last message ID for the actor
	// You can use the DynamoDB client to query the message table and retrieve the last message ID
	// If there is no last message ID, return 0

	return 0, nil
}

func (r *ActorRepositoryDdb) addMessageToTable(id ActorID, messageID int, message Message) error {
	// TODO: Implement the logic to add the message to the message table
	// You can use the DynamoDB client to put the message item with the actor ID as the PK and the message ID as the SK

	return nil
}

func (r *ActorRepositoryDdb) incrementInboxCount(id ActorID) error {
	// TODO: Implement the logic to increment the actor's inbox count by one
	// You can use the DynamoDB client to update the actor item and increment the inbox count attribute

	return nil
}

func (r *ActorRepositoryDdb) finishedWork(id ActorID, updatedState map[string]interface{}, consumedMessageIds []string) (bool, error) {
	// In a transaction...
	// 1. one PutItem with updated state and inbox_count -= length(consumedMessageIds)
	// 2. one DeleteItem for each message id in consumedMessageIds

	makeDeleteItems := func() []types.TransactWriteItem {
		var deleteItems []types.TransactWriteItem
		for _, messageId := range consumedMessageIds {
			deleteItems = append(deleteItems, types.TransactWriteItem{
				Delete: &types.Delete{
					Key: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("message#%v", messageId)},
						"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("message#%v", messageId)},
					},
					TableName: aws.String(r.TableName),
				},
			})
		}
		return deleteItems
	}

	// Create a transaction write request
	// PutItem operation to update the state and inbox count
	// marshalledState, err := attributevalue.Marshal(updatedState)
	// if err != nil {
	// 	panic(err)
	// }
	updateItemInput, err := r.makeUpdateItemInput(id, updatedState, -len(consumedMessageIds))
	if err != nil {
		return false, err
	}

	updateInput := &types.Update{
		TableName:                 updateItemInput.TableName,
		Key:                       updateItemInput.Key,
		UpdateExpression:          updateItemInput.UpdateExpression,
		ConditionExpression:       updateItemInput.ConditionExpression,
		ExpressionAttributeNames:  updateItemInput.ExpressionAttributeNames,
		ExpressionAttributeValues: updateItemInput.ExpressionAttributeValues,
	}

	transactItems := []types.TransactWriteItem{
		types.TransactWriteItem{
			Update: updateInput,
		}}
	transactItems = append(transactItems, makeDeleteItems()...)

	writeRequest := &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	}

	// Execute the transaction write request
	_, err = r.DynamoDbClient.TransactWriteItems(context.TODO(), writeRequest)
	if err != nil {
		log.Printf("Couldn't execute transaction write request. Here's why: %v\n", err)
		return false, err
	}

	return true, nil
}

// func (r *ActorRepositoryDdb) scan() {
// 	log.Println("scanning...")
// 	res, err := r.DynamoDbClient.Scan(context.TODO(), &dynamodb.ScanInput{
// 		TableName: aws.String(r.TableName),
// 	})
// 	if err != nil {
// 		log.Printf("Couldn't scan. Here's why: %v\n", err)
// 	} else {
// 		log.Printf("%v+", res)
// 		// err = attributevalue.UnmarshalListOfMaps(res.Items, &movies)
// 		// if err != nil {
// 		// 	log.Printf("Couldn't unmarshal query response. Here's why: %v\n", err)
// 		// }
// 	}
// }
