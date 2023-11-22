package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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
	var output []map[string]interface{}
	response, err := runner.DynamoDbClient.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
		Statement: aws.String(query),
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

func (r *ActorRepositoryDdb) save(a Actor) {
	fmt.Printf("Repo saved: %+v \n", a)
}
func (r *ActorRepositoryDdb) get(id ActorID) Actor {
	return Actor{}
}
func (r *ActorRepositoryDdb) scan() {
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

func (r *ActorRepositoryDdb) scan2() {
	log.Println("Pscanning...")
	res, err := r.PartiQLRunner.run("select * from actors")
	if err != nil {
		log.Printf("Couldn't p scan. Here's why: %v\n", err)
	} else {
		log.Printf("%v+", res)
		// err = attributevalue.UnmarshalListOfMaps(res.Items, &movies)
		// if err != nil {
		// 	log.Printf("Couldn't unmarshal query response. Here's why: %v\n", err)
		// }
	}
}
