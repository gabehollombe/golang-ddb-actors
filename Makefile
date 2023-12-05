start-localstack:
	localstack start -d

localstack-ready: start-localstack
	localstack wait -t 10 && echo "Localstack ready"


create-table: localstack-ready
	aws dynamodb create-table \
	--table-name actors \
	--attribute-definitions AttributeName=id,AttributeType=S \
	--key-schema AttributeName=id,KeyType=HASH \
	--provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
	--endpoint-url http://localhost:4566 \
	--profile localstack

create-queue: localstack-ready
	aws sqs create-queue \
	--queue-name ActorMessages.fifo \
	--attributes FifoQueue=true,ContentBasedDeduplication=true \
	--endpoint-url http://localhost:4566 \
	--profile localstack

restart-localstack: 
	localstack stop
	localstack start -d

setup: restart-localstack create-table create-queue

run: create-table create-queue
	echo "Running application"
	go run ddb-actor.go
	localstack stop