package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"text/template"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
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

	tmpl, err := template.New("query").Parse(query)
	if err != nil {
		panic(err)
	}
	buf := &bytes.Buffer{}
	err = tmpl.Execute(buf, params)
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

func (r *ActorRepositoryDdb) save(a Actor) (Actor, error) {
	state, err := json.Marshal(a.State)
	if err != nil {
		return a, err
	}

	query := `
		UPDATE 	"actors"
		SET state='{{.State}}'
		WHERE pk='{{.ID}}' and sk='{{.ID}}'
	`
	params := map[string]interface{}{
		"State": string(state),
		"ID":    "actor#" + a.ID,
	}

	_, err = r.PartiQLRunner.runWithParams(query, params)
	var ccf *types.ConditionalCheckFailedException
	if errors.As(err, &ccf) {
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
			"State": string(state),
			"ID":    "actor#" + a.ID,
		}
		_, err = r.PartiQLRunner.runWithParams(query, params)
		if err != nil {
			panic(err)
		} else {
			return a, nil
		}
	} else {
		if err != nil {
			panic(err)
		}
	}

	// fmt.Printf("Repo saved: %+v \n", a)
	return a, nil
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
	res, err := r.PartiQLRunner.run("select * from actors where pk=")
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
