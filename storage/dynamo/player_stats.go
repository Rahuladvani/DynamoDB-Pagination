package dynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	pk                        = "pk"
	sk                        = "sk"
	playerStatsTable          = "player_stats_v1"
	goals                     = "goals"
	nationalTeamAttributeName = "national_team"
	identifierSeparator       = "#"
	gsi                       = "GSI1"
)

type playerStats struct {
	dbClient *dynamodb.Client
}

type StatsRecord struct {
	PartitionKey string `dynamodbav:"pk"`    // generic name, expected value is country
	SortKey      string `dynamodbav:"sk"`    // generic name, expected value is <NationalTeam>#<FirstName>#<LastName>
	Goals        int    `dynamodbav:"goals"` // sort key attribute for the gsi
	Assists      int    `dynamodbav:"assists"`
	Appearances  int    `dynamodbav:"appearances"`
	Country      string `dynamodbav:"country"`
	NationalTeam string `dynamodbav:"national_team"`
	FirstName    string `dynamodbav:"first_name"`
	LastName     string `dynamodbav:"last_name"`
}

func (p *playerStats) buildKey(country string, nationalTeam string, firstName string, lastName string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		pk: &types.AttributeValueMemberS{Value: country},
		sk: &types.AttributeValueMemberS{Value: p.buildSortKey(nationalTeam, firstName, lastName)},
	}
}

func (p *playerStats) buildListPlayersQueryExpression(country string, nationalTeam string) (expression.Expression, error) {
	var keyCond expression.KeyConditionBuilder
	var builder expression.Builder
	keyCond = expression.Key(pk).Equal(expression.Value(country)).And(expression.Key(sk).BeginsWith(fmt.Sprintf("%s", nationalTeam)))
	builder = expression.NewBuilder().WithKeyCondition(keyCond)
	expr, err := builder.Build()
	return expr, err
}

func (p *playerStats) buildListPlayersWithGoalsFilterQueryExpression(country string, nationalTeam string, goalThreshold int) (expression.Expression, error) {
	var keyCond expression.KeyConditionBuilder
	var builder expression.Builder
	var filter expression.ConditionBuilder
	keyCond = expression.Key(pk).Equal(expression.Value(country)).And(expression.Key(sk).BeginsWith(fmt.Sprintf("%s", nationalTeam)))
	filter = expression.Name(goals).GreaterThanEqual(expression.Value(goalThreshold))
	builder = expression.NewBuilder().WithKeyCondition(keyCond).WithFilter(filter)
	expr, err := builder.Build()
	return expr, err
}

func (p *playerStats) buildListPlayersWithGoalsSortedFilterQueryExpression(country string, nationalTeam string, goalThreshold int) (expression.Expression, error) {
	var keyCond expression.KeyConditionBuilder
	var builder expression.Builder
	var filter expression.ConditionBuilder
	keyCond = expression.Key(pk).Equal(expression.Value(country)).And(expression.Key(goals).GreaterThanEqual(expression.Value(goalThreshold)))
	filter = expression.Name(nationalTeamAttributeName).Equal(expression.Value(nationalTeam))
	builder = expression.NewBuilder().WithKeyCondition(keyCond).WithFilter(filter)
	expr, err := builder.Build()
	return expr, err
}

func (p *playerStats) buildExclusiveStartKey(lastEvaluatedItem map[string]types.AttributeValue) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		pk: lastEvaluatedItem[pk],
		sk: lastEvaluatedItem[sk],
	}
}

func (p *playerStats) buildExclusiveStartKeyForGSI(lastEvaluatedItem map[string]types.AttributeValue) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		pk:    lastEvaluatedItem[pk],
		sk:    lastEvaluatedItem[sk],
		goals: lastEvaluatedItem[goals],
	}
}

func (p *playerStats) buildSortKey(nationalTeam string, firstName string, lastName string) string {
	return fmt.Sprintf("%s%s%s%s%s", nationalTeam, identifierSeparator, firstName, identifierSeparator, lastName)
}

func (p *playerStats) ScanStatsTable(ctx context.Context, cursor *Cursor) ([]*StatsRecord, error) {
	var records []*StatsRecord
	scanTableInput := &dynamodb.ScanInput{
		TableName: aws.String(playerStatsTable),
		Limit:     aws.Int32(cursor.PageLimit),
	}
	resp, err := p.dbClient.Scan(ctx, scanTableInput)
	if err != nil {
		return nil, err
	}
	err = attributevalue.UnmarshalListOfMaps(resp.Items, &records)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (p *playerStats) GetPlayerStats(ctx context.Context, country string, nationalTeam string, firstName string, lastName string) (*StatsRecord, error) {
	var playerRecord *StatsRecord
	resp, err := p.dbClient.GetItem(ctx, &dynamodb.GetItemInput{
		Key:       p.buildKey(country, nationalTeam, firstName, lastName),
		TableName: aws.String(playerStatsTable),
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Item) == 0 {
		return nil, fmt.Errorf("record not found")
	}
	err = attributevalue.UnmarshalMap(resp.Item, &playerRecord)
	if err != nil {
		return nil, err
	}
	return playerRecord, nil
}

func (p *playerStats) PutPlayerStats(ctx context.Context, playerRecord *StatsRecord) error {
	av, err := attributevalue.MarshalMap(playerRecord)
	if err != nil {
		return err
	}
	putItemInput := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(playerStatsTable),
	}
	_, err = p.dbClient.PutItem(ctx, putItemInput)
	if err != nil {
		return err
	}
	return nil
}

func (p *playerStats) ListPlayers(ctx context.Context, country string, nationalTeam string) ([]*StatsRecord, error) {
	var records []*StatsRecord
	expr, err := p.buildListPlayersQueryExpression(country, nationalTeam)
	if err != nil {
		return nil, err
	}
	queryInput := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		TableName:                 aws.String(playerStatsTable),
	}
	resp, err := p.dbClient.Query(ctx, queryInput)
	if err != nil {
		return nil, err
	}
	err = attributevalue.UnmarshalListOfMaps(resp.Items, &records)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (p *playerStats) ListAllPlayers(ctx context.Context, country string, nationalTeam string) ([]*StatsRecord, error) {
	var collectiveResult []map[string]types.AttributeValue
	var records []*StatsRecord
	expr, err := p.buildListPlayersQueryExpression(country, nationalTeam)
	if err != nil {
		return nil, err
	}
	queryInput := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		TableName:                 aws.String(playerStatsTable),
	}
	paginator := dynamodb.NewQueryPaginator(p.dbClient, queryInput)
	for {
		if !paginator.HasMorePages() {
			break
		}
		singlePage, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		collectiveResult = append(collectiveResult, singlePage.Items...)
	}
	err = attributevalue.UnmarshalListOfMaps(collectiveResult, &records)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (p *playerStats) ListLimitedPlayers(ctx context.Context, country string, nationalTeam string, cursor *Cursor) ([]*StatsRecord, error) {
	var records []*StatsRecord
	expr, err := p.buildListPlayersQueryExpression(country, nationalTeam)
	if err != nil {
		return nil, err
	}
	queryInput := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		TableName:                 aws.String(playerStatsTable),
		Limit:                     aws.Int32(cursor.PageLimit),
	}
	if cursor.LastEvaluatedKey != nil {
		queryInput.ExclusiveStartKey = cursor.LastEvaluatedKey
	}
	resp, err := p.dbClient.Query(ctx, queryInput)
	if err != nil {
		return nil, err
	}
	cursor.LastEvaluatedKey = resp.LastEvaluatedKey
	err = attributevalue.UnmarshalListOfMaps(resp.Items, &records)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (p *playerStats) ListPlayersByGoalsThreshold(ctx context.Context, country string, nationalTeam string, goalThreshold int, cursor *Cursor) ([]*StatsRecord, error) {
	var collectiveResult []map[string]types.AttributeValue
	var records []*StatsRecord
	expr, err := p.buildListPlayersWithGoalsFilterQueryExpression(country, nationalTeam, goalThreshold)
	if err != nil {
		return nil, err
	}
	queryInput := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		TableName:                 aws.String(playerStatsTable),
		Limit:                     aws.Int32(cursor.PageLimit),
		ScanIndexForward:          aws.Bool(cursor.ScanIndexForward),
	}
	if cursor.LastEvaluatedKey != nil {
		queryInput.ExclusiveStartKey = cursor.LastEvaluatedKey
	}
	paginator := dynamodb.NewQueryPaginator(p.dbClient, queryInput)
	for {
		if !paginator.HasMorePages() {
			fmt.Println("no more records in the partition")
			cursor.LastEvaluatedKey = nil
			break
		}
		singlePage, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		pendingItems := int(cursor.PageLimit) - len(collectiveResult)
		if int(singlePage.Count) >= pendingItems {
			collectiveResult = append(collectiveResult, singlePage.Items[:pendingItems]...)
			cursor.LastEvaluatedKey = p.buildExclusiveStartKey(singlePage.Items[pendingItems-1])
			break
		}
		collectiveResult = append(collectiveResult, singlePage.Items...)
	}
	err = attributevalue.UnmarshalListOfMaps(collectiveResult, &records)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (p *playerStats) ListPlayersByGoalsThresholdSorted(ctx context.Context, country string, nationalTeam string, goalThreshold int, cursor *Cursor) ([]*StatsRecord, error) {
	var collectiveResult []map[string]types.AttributeValue
	var records []*StatsRecord
	expr, err := p.buildListPlayersWithGoalsSortedFilterQueryExpression(country, nationalTeam, goalThreshold)
	if err != nil {
		return nil, err
	}
	queryInput := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		IndexName:                 aws.String(gsi),
		TableName:                 aws.String(playerStatsTable),
		Limit:                     aws.Int32(cursor.PageLimit),
		ScanIndexForward:          aws.Bool(cursor.ScanIndexForward),
	}
	if cursor.LastEvaluatedKey != nil {
		queryInput.ExclusiveStartKey = cursor.LastEvaluatedKey
	}
	paginator := dynamodb.NewQueryPaginator(p.dbClient, queryInput)
	for {
		if !paginator.HasMorePages() {
			cursor.LastEvaluatedKey = nil
			break
		}
		singlePage, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		pendingItems := int(cursor.PageLimit) - len(collectiveResult)
		if int(singlePage.Count) >= pendingItems {
			collectiveResult = append(collectiveResult, singlePage.Items[:pendingItems]...)
			cursor.LastEvaluatedKey = p.buildExclusiveStartKeyForGSI(singlePage.Items[pendingItems-1])
			break
		}
		collectiveResult = append(collectiveResult, singlePage.Items...)
	}
	err = attributevalue.UnmarshalListOfMaps(collectiveResult, &records)
	if err != nil {
		return nil, err
	}
	return records, nil
}
