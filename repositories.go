package main

import (
	"context"
	"errors"
	"fmt"
	"log"

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

type ActorDTO struct {
	PK         string
	SK         string
	State      map[string]interface{}
	InboxCount int
}

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

// func (r *ActorRepositoryDdb) create(a Actor) (Actor, error) {
// }

func (r *ActorRepositoryDdb) save(a Actor) (Actor, error) {
	// var err error
	// var response *dynamodb.UpdateItemOutput
	// var attributeMap map[string]map[string]interface{}
	update := expression.
		Set(expression.Name("state"), expression.Value(a.State))

	condition := expression.And(
		expression.Name("pk").Equal(expression.Value(fmt.Sprintf("actor#%v", a.ID))),
		expression.Name("sk").Equal(expression.Value(fmt.Sprintf("actor#%v", a.ID))),
	)

	expr, err := expression.
		NewBuilder().
		WithUpdate(update).
		WithCondition(condition).
		Build()
	if err != nil {
		log.Printf("Couldn't build expression for update. Here's why: %v\n", err)
		return a, err
	}
	log.Printf("Expression: %+v\n", expr)

	key := r.getKey(a.ID)
	_, err = r.DynamoDbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName:                 aws.String(r.TableName),
		Key:                       key,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              types.ReturnValueUpdatedNew,
	})
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
