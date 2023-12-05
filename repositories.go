package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type FifoQueue interface {
	putMessage(body string, groupId string, deduplicationId string) (bool, error)
	getMessages() ([]Message, error)
}

type SqsFifoQueueClient struct {
	queueUrl  string
	sqsClient *sqs.Client
}

func NewSqsFifoQueueClient(queueUrl string, sdkConfig aws.Config) SqsFifoQueueClient {
	return SqsFifoQueueClient{
		queueUrl:  queueUrl,
		sqsClient: sqs.NewFromConfig(sdkConfig),
	}
}

func (q *SqsFifoQueueClient) putMessage(body string, groupId string, deduplicationId string) (bool, error) {
	// Define the send message input
	sendMsgInput := &sqs.SendMessageInput{
		QueueUrl:               aws.String(q.queueUrl),
		MessageBody:            aws.String(body),
		MessageGroupId:         aws.String(groupId),
		MessageDeduplicationId: aws.String(deduplicationId),
	}

	// Execute the send message operation
	_, err := q.sqsClient.SendMessage(context.TODO(), sendMsgInput)
	if err != nil {
		log.Printf("Couldn't send message. Here's why: %v\n", err)
		return false, err
	}

	return true, nil
}

func (q *SqsFifoQueueClient) getMessages() ([]Message, error) {
	// Define the receive message input
	receiveMsgInput := &sqs.ReceiveMessageInput{
		QueueUrl:                aws.String(q.queueUrl),
		MaxNumberOfMessages:     10,
		ReceiveRequestAttemptId: aws.String(strconv.FormatInt(time.Now().UnixNano(), 10)),
		AttributeNames: []sqsTypes.QueueAttributeName{
			sqsTypes.QueueAttributeName(sqsTypes.MessageSystemAttributeNameMessageGroupId),
		},
	}

	// Execute the receive message operation
	resp, err := q.sqsClient.ReceiveMessage(context.TODO(), receiveMsgInput)
	if err != nil {
		log.Printf("Couldn't receive messages. Here's why: %v\n", err)
		return nil, err
	}

	// Parse the received messages into a slice of Message structs
	messages := make([]Message, 0, len(resp.Messages))
	for _, msg := range resp.Messages {
		message := Message{
			ID:   aws.ToString(msg.ReceiptHandle),
			To:   msg.Attributes["MessageGroupId"],
			Body: aws.ToString(msg.Body),
		}
		// log.Printf("Received message: %v+\n", message)
		log.Printf("Attrs: %v+\n", msg.Attributes)
		messages = append(messages, message)
	}

	log.Printf("Received messages: %v+\n", messages)
	return messages, nil
}

func (q *SqsFifoQueueClient) acknowledgeMessage(receiptHandle string) (bool, error) {
	// Define the delete message input
	deleteMsgInput := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.queueUrl),
		ReceiptHandle: aws.String(receiptHandle),
	}

	// Execute the delete message operation
	_, err := q.sqsClient.DeleteMessage(context.TODO(), deleteMsgInput)
	if err != nil {
		log.Printf("Couldn't delete message. Here's why: %v\n", err)
		return false, err
	}

	return true, nil
}

type ActorRepository interface {
	save(a ActorBase)
	get(ActorID) ActorBase
}

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
	pk, err := attributevalue.Marshal(id)
	if err != nil {
		panic(err)
	}
	return map[string]types.AttributeValue{"id": pk}
}

func (r *ActorRepositoryDdb) makeUpdateItemInput(id string, state map[string]interface{}) (*dynamodb.UpdateItemInput, error) {
	update := expression.UpdateBuilder{}
	update = update.Set(expression.Name("state"), expression.Value(state))

	// This is so we know if the item exists or not. If it doesn't we'll get a ConditionalCheckFailed exception.
	condition := expression.Name("id").Equal(expression.Value(id))

	expr, err := expression.
		NewBuilder().
		WithUpdate(update).
		WithCondition(condition).
		Build()

	if err != nil {
		log.Printf("Couldn't build expression for update. Here's why: %v\n", err)
		return nil, err
	}

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

func (r *ActorRepositoryDdb) save(id string, state map[string]interface{}) (bool, error) {
	updateItemInput, err := r.makeUpdateItemInput(id, state)
	if err != nil {
		return false, err
	}
	_, err = r.DynamoDbClient.UpdateItem(context.TODO(), updateItemInput)
	if err == nil {
		log.Printf("DDB Repo saved via update: %v %v+\n", id, state)
		return false, nil
	}

	// We have an error.
	// If it's not a CCF, we don't know what to do with it.
	var ccf *types.ConditionalCheckFailedException
	if !errors.As(err, &ccf) {
		log.Printf("Couldn't update actor %v+. Here's why: %v\n", id, err)
		return false, err
	}

	// This error is a CCF, which means the item doesn't exist yet. Let's create it...
	log.Print("Item doesn't exist yet. Creating...")

	// IMPORTANT: We only want to manage some attributes for the actor here (e.g. state, inbox_count).
	// So we put those in directly, not via the ActorBase struct.
	// TODO: Refactor this so that update and puts both use some common base for figuring out which fields go in from ActorBase to Dynamo
	//  	 Right now this is duplicated between here and in makeUpdateItemInput.
	marshalledState, err := attributevalue.Marshal(state)
	if err != nil {
		return false, err
	}
	item := make(map[string]types.AttributeValue)
	item["id"] = &types.AttributeValueMemberS{Value: id}
	item["state"] = marshalledState
	_, err = r.DynamoDbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(r.TableName),
		Item:      item,
	})
	if err != nil {
		log.Printf("Couldn't add item to table. Here's why: %v\n", err)
		return false, err
	}
	return true, nil
}

func (r *ActorRepositoryDdb) get(id ActorID) (Actor, error) {
	// load the actor
	key := r.getKey(id)
	actorResponse, err := r.DynamoDbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key:       key,
		TableName: aws.String(r.TableName),
	})
	if err != nil {
		log.Printf("Couldn't get ActorBase ID: %v. Here's why: %v\n", id, err)
		return nil, err
	}
	if actorResponse.Item == nil {
		return nil, errors.New("actor not found")
	}
	actorBase := ActorBase{}
	err = attributevalue.UnmarshalMap(actorResponse.Item, &actorBase)
	if err != nil {
		log.Printf("Couldn't unmarshal response. Here's why: %v\n", err)
		return nil, err
	}

	var actor Actor
	actorType := actorBase.State["_type"]
	switch actorType {
	case "counting":
		// TODO: how will we handle versioning of actor types?
		actor = NewCountingActorFromBase(actorBase)
	default:
		return nil, fmt.Errorf("unknown actor type: %v", actorType)
	}

	return actor, err
}

func (r *ActorRepositoryDdb) addMessage(actorId ActorID, message Message) (bool, error) {
	// We'll use the current time in nanoseconds as the message ID
	// if this ends up existing, we'll try again until we succeed in the insert
	messageId := time.Now().UnixNano()

	txOptions := &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					Item: map[string]types.AttributeValue{
						"pk":   &types.AttributeValueMemberS{Value: fmt.Sprintf("actor#%v", actorId)},
						"sk":   &types.AttributeValueMemberS{Value: fmt.Sprintf("message#%v", messageId)},
						"body": &types.AttributeValueMemberS{Value: message.Body},
						"id":   &types.AttributeValueMemberS{Value: fmt.Sprint(messageId)},
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

func (r *ActorRepositoryDdb) finishedWork(actorId ActorID, updatedState map[string]interface{}, consumedMessageIds []string) (bool, error) {
	// In a transaction...
	// 1. one PutItem with updated state and inbox_count -= length(consumedMessageIds)
	// 2. one DeleteItem for each message id in consumedMessageIds

	makeDeleteItems := func() []types.TransactWriteItem {
		var deleteItems []types.TransactWriteItem
		for _, messageId := range consumedMessageIds {
			deleteItems = append(deleteItems, types.TransactWriteItem{
				Delete: &types.Delete{
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("actor#%v", actorId)},
						"sk": &types.AttributeValueMemberS{Value: fmt.Sprintf("message#%v", messageId)},
					},
					TableName: aws.String(r.TableName),
				},
			})
		}
		return deleteItems
	}

	updateItemInput, err := r.makeUpdateItemInput(actorId, updatedState)
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
		{
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

func (r *ActorRepositoryDdb) getActorIdsWithMessages() ([]ActorID, error) {
	// IMPORTANT: DDB scans only return 1MB of results at once, so we need to paginate somewhere so we know we get all the data
	// For now, we'll do that here, but we might want to make this a separate method on the repo so the caller can decide how to handle pagination

	var actorIds = make([]ActorID, 0)
	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		// Do the scan
		scanInput := &dynamodb.ScanInput{
			TableName:        aws.String(r.TableName),
			IndexName:        aws.String("sort_by_inbox_count"),
			FilterExpression: aws.String("inbox_count > :zero"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":zero": &types.AttributeValueMemberN{Value: "0"},
			},
			ExclusiveStartKey: lastEvaluatedKey,
		}
		scanOutput, err := r.DynamoDbClient.Scan(context.TODO(), scanInput)
		if err != nil {
			log.Printf("Couldn't scan for actors with inbox counts. Here's why: %v\n", err)
			return nil, err
		}

		// Parse the items in the response
		var actorBase ActorBase
		for _, item := range scanOutput.Items {
			err = attributevalue.UnmarshalMap(item, &actorBase)
			if err != nil {
				log.Printf("Couldn't unmarshal items. Here's why: %v\n", err)
				return nil, err
			}
			actorIds = append(actorIds, actorBase.ID)
		}

		// If LastEvaluatedKey is not nil, we need to paginate
		if scanOutput.LastEvaluatedKey != nil {
			lastEvaluatedKey = scanOutput.LastEvaluatedKey
		} else {
			break
		}
	}

	return actorIds, nil
}

func (r *ActorRepositoryDdb) lockActor(actorId ActorID, workerNonce string, expiresAt time.Time) (bool, error) {
	// Define the update item input
	updateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(r.TableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("actor#%v", actorId)},
			"sk": &types.AttributeValueMemberS{Value: fmt.Sprintf("actor#%v", actorId)},
		},
		UpdateExpression:    aws.String("SET worker_nonce = :workerNonce, expires_at = :expiresAt"),
		ConditionExpression: aws.String("attribute_not_exists(lock_expires_at) OR lock_expires_at >= :now"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":workerNonce": &types.AttributeValueMemberS{Value: workerNonce},
			":now":         &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Unix(), 10)},
			":expiresAt":   &types.AttributeValueMemberN{Value: strconv.FormatInt(expiresAt.Unix(), 10)},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	}

	// Execute the update item operation
	_, err := r.DynamoDbClient.UpdateItem(context.TODO(), updateInput)
	if err == nil {
		return true, nil
	}

	var ccf *types.ConditionalCheckFailedException
	if !errors.As(err, &ccf) {
		log.Printf("Couldn't lock actor. Here's why: %v\n", err)
		return false, err
	}

	log.Printf("Couldn't lock because conditional check failed: %v", err)
	return false, err
}
