package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"text/template"

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

type PartiQLRunner struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

func (runner PartiQLRunner) run(query string) ([]map[string]interface{}, error) {
	return runner.runWithParams(query, map[string]interface{}{})
}
func (runner PartiQLRunner) runWithParams(query string, params map[string]interface{}) ([]map[string]interface{}, error) {
	var output []map[string]interface{}

	attrParams := make(map[string]types.AttributeValue)
	for k, v := range params {
		mParam, err := attributevalue.Marshal(v)
		if err != nil {
			panic(err)
		}
		attrParams[k] = mParam
	}

	tmpl, err := template.New("query").Parse(query)
	if err != nil {
		panic(err)
	}
	buf := &bytes.Buffer{}
	err = tmpl.Execute(buf, attrParams)
	if err != nil {
		panic(err)
	}
	templatedQuery := buf.String()
	fmt.Printf("Running: %+v \n", templatedQuery)

	response, err := runner.DynamoDbClient.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
		Statement: aws.String(templatedQuery),
	})
	if err != nil {
		log.Printf("Error running PartiQL query: %v\n", err)
	} else {
		err = attributevalue.UnmarshalListOfMaps(response.Items, &output)
		if err != nil {
			log.Printf("Couldn't unmarshal PartiQL response. Here's why: %v\n", err)
		}
	}
	return output, err
}

type ActorRepositoryDdb struct {
	TableName      string
	DynamoDbClient *dynamodb.Client
	PartiQLRunner  PartiQLRunner
}

func NewActorRepositoryDdb(tableName string, sdkConfig aws.Config) ActorRepositoryDdb {
	return ActorRepositoryDdb{
		TableName:      tableName,
		DynamoDbClient: dynamodb.NewFromConfig(sdkConfig),
		PartiQLRunner: PartiQLRunner{
			DynamoDbClient: dynamodb.NewFromConfig(sdkConfig),
			TableName:      tableName,
		},
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

func (r *ActorRepositoryDdb) dsave(a Actor) (Actor, error) {
	var err error
	// var response *dynamodb.UpdateItemOutput
	var attributeMap map[string]map[string]interface{}
	update := expression.Set(expression.Name("state"), expression.Value(a.State))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		log.Printf("Couldn't build expression for update. Here's why: %v\n", err)
	} else {
		key := r.getKey(a.ID)

		response, err := r.DynamoDbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
			TableName:                 aws.String(r.TableName),
			Key:                       key,
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			UpdateExpression:          expr.Update(),
			ReturnValues:              types.ReturnValueUpdatedNew,
		})
		if err != nil {
			log.Printf("Couldn't update actor %v+. Here's why: %v\n", a, err)
		} else {
			err = attributevalue.UnmarshalMap(response.Attributes, &attributeMap)
			if err != nil {
				log.Printf("Couldn't unmarshall update response. Here's why: %v\n", err)
			}
		}
	}
	return a, err
}

func (r *ActorRepositoryDdb) psave(a Actor) (Actor, error) {
	// state, err := json.Marshal(a.State)
	// if err != nil {
	// 	return a, err
	// }

	query := `
		UPDATE 	"actors"
		SET state='{{.State}}'
		WHERE pk='{{.ID}}' and sk='{{.ID}}'
	`
	stateParam, err := attributevalue.Marshal(a.State)
	if err != nil {
		panic(err)
	}

	params := map[string]interface{}{
		"State": stateParam,
		"ID":    "actor#" + a.ID,
	}

	_, err = r.PartiQLRunner.runWithParams(query, params)
	if err == nil {
		log.Printf("DDB Repo saved via update: %+v \n", a)
		return a, nil
	}

	// We have an error.
	// If it's not a CCF, we don't know what to do with it.
	var ccf *types.ConditionalCheckFailedException
	if !errors.As(err, &ccf) {
		return a, err
	}

	// This error is a CCF, which means the item doesn't exist yet. Let's create it...
	log.Print("Item doesn't exist yet. Creating...")
	query = `
		INSERT INTO 	"actors"
		VALUE { 
			'pk': '{{.ID}}',
			'sk': '{{.ID}}',
			'state': '{{.State}}',
			'inbox_count': 0
		}
	`
	params = map[string]interface{}{
		"State": stateParam,
		"ID":    "actor#" + a.ID,
	}
	_, err = r.PartiQLRunner.runWithParams(query, params)
	if err != nil {
		return a, err
	}
	log.Printf("DDB Repo saved via create: %+v \n", a)
	return a, nil
}

func (r *ActorRepositoryDdb) dget(id ActorID) (Actor, error) {
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

func (r *ActorRepositoryDdb) pget(id ActorID) (Actor, error) {
	actorQuery := `
		SELECT * 
		FROM "actors" 
		WHERE pk='{{.ID}}' and sk='{{.ID}}'
	`
	actorParams := map[string]interface{}{
		"ID": "actor#" + id,
	}
	actorRes, err := r.PartiQLRunner.runWithParams(actorQuery, actorParams)
	if err != nil {
		return Actor{}, err
	}
	if len(actorRes) > 1 {
		return Actor{}, errors.New("more than one actor found")
	}

	log.Printf("%v+", actorRes)

	foo := actorRes[0]["state"]
	fmt.Printf("%v+", foo)
	state, ok := actorRes[0]["state"].(map[string]interface{})
	if !ok {
		return Actor{}, errors.New("invalid state type")
	}

	return NewActor(id, nil, state), nil

	// return NewActor(id, nil, actorRes["state"].(interface{}))
}

func (r *ActorRepositoryDdb) dscan() {
	log.Println("scanning...")
	res, err := r.DynamoDbClient.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName: aws.String(r.TableName),
	})
	if err != nil {
		log.Printf("Couldn't scan. Here's why: %v\n", err)
	} else {
		log.Printf("%v+", res)
		// err = attributevalue.UnmarshalListOfMaps(res.Items, &movies)
		// if err != nil {
		// 	log.Printf("Couldn't unmarshal query response. Here's why: %v\n", err)
		// }
	}
}
