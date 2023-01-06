package dynamo

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Dynamo struct {
	client *dynamodb.Client
	playerStats
}

type Cursor struct {
	PageLimit        int32
	LastEvaluatedKey map[string]types.AttributeValue
	ScanIndexForward bool
}

func New(client *dynamodb.Client) *Dynamo {
	return &Dynamo{
		client: client,
		playerStats: playerStats{
			dbClient: client,
		},
	}
}
