package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"pagination/storage"
	"pagination/storage/dynamo"
)

type statsHandler struct {
	storageClient storage.Storage
}

const (
	AwsRegion               = "us-east-1"
	DynamoLocalUrl          = "http://localhost:8000"
	menNationalTeam         = "MNT"
	womenNationalTeam       = "WNT"
	testPlayerFirstName     = "first-name-1"
	testPlayerLastName      = "last-name-1"
	testPlayerCountry1      = "country-1"
	testPlayerCountry2      = "country-2"
	userEnforcedRecordLimit = 4
	goalThreshold           = 10
)

func main() {

	cfg, err := config.LoadDefaultConfig(context.TODO(), func(o *config.LoadOptions) error {
		o.Region = AwsRegion
		return nil
	})

	if err != nil {
		os.Exit(1)
	}

	svc := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.EndpointResolver = dynamodb.EndpointResolverFromURL(DynamoLocalUrl)
	})
	storage := dynamo.New(svc)
	stats := statsHandler{storageClient: storage}

	stats.insertSeedData()

	fmt.Println("getting single player stats")
	stats.GetPlayerStats()
	fmt.Println("listing player stats")
	stats.ListPlayersWithoutPagination()
	fmt.Println("listing player stats while handling internal pagination")
	stats.ListAllPlayers()
	fmt.Println("listing limited number player stats")
	stats.ListLimitedPlayers()
	fmt.Println("listing limited number player stats and apply goals scored filter")
	stats.ListPlayersByGoalsThreshold()
	fmt.Println("listing limited number player stats with goals filter in descending order wrt goals scored")
	stats.ListPlayersByGoalsThresholdSorted()
}

func (s *statsHandler) insertSeedData() {
	cursor := &dynamo.Cursor{PageLimit: 1, ScanIndexForward: true}
	resp, err := s.storageClient.ScanStatsTable(context.TODO(), cursor)
	if err != nil {
		fmt.Println("failed to scan stats table", err)
		os.Exit(1)
	}
	if len(resp) != 0 {
		fmt.Println("not inserting seed data as stats table already contains items")
		return
	}

	records := loadSeedDataInMemory()
	for _, record := range records {
		err := s.storageClient.PutPlayerStats(context.TODO(), &record)
		if err != nil {
			fmt.Println("failed to insert seed data into stats table", err)
			os.Exit(1)
		}
	}
	fmt.Println("loaded stats table with seed info")
}

func (s *statsHandler) ListPlayersWithoutPagination() {
	resp, err := s.storageClient.ListPlayers(context.TODO(), testPlayerCountry1, menNationalTeam)
	if err != nil {
		fmt.Println("failed while listing player stats without pagination support : ", err)
		os.Exit(1)
	} else {
		printRecords(resp)
	}
}

func (s *statsHandler) ListAllPlayers() {
	resp, err := s.storageClient.ListAllPlayers(context.TODO(), testPlayerCountry2, womenNationalTeam)
	if err != nil {
		fmt.Println("failed while listing player stats with pagination support : ", err)
		os.Exit(1)
	} else {
		printRecords(resp)
	}
}

func (s *statsHandler) ListLimitedPlayers() {
	cursor := &dynamo.Cursor{PageLimit: userEnforcedRecordLimit, ScanIndexForward: true}

	// Iterating over the result pages
	pageCount := 0
	isFirstPage := true
	for {
		if cursor.LastEvaluatedKey == nil && !isFirstPage {
			break
		}
		pageCount += 1
		resp, err := s.storageClient.ListLimitedPlayers(context.TODO(), testPlayerCountry2, womenNationalTeam, cursor)
		if err != nil {
			fmt.Println("failed while listing limit specified number of player stats : ", err)
			os.Exit(1)
		} else {
			fmt.Println("Page Number : ", pageCount)
			printRecords(resp)
		}
		isFirstPage = false
	}
}

func (s *statsHandler) ListPlayersByGoalsThreshold() {
	cursor := &dynamo.Cursor{PageLimit: 2, ScanIndexForward: true}

	// Iterating over the result pages
	pageCount := 0
	isFirstPage := true
	for {
		if cursor.LastEvaluatedKey == nil && !isFirstPage {
			break
		}
		pageCount += 1
		resp, err := s.storageClient.ListPlayersByGoalsThreshold(context.TODO(), testPlayerCountry1, menNationalTeam, goalThreshold, cursor)
		if err != nil {
			fmt.Println("failed while listing limit specified number of player stats with goals filter : ", err)
			os.Exit(1)
		} else {
			fmt.Println("Page Number : ", pageCount)
			printRecords(resp)
		}
		isFirstPage = false
	}
}

func (s *statsHandler) ListPlayersByGoalsThresholdSorted() {
	// ScanIndexForward = false for top scorers in descending order
	cursor := &dynamo.Cursor{PageLimit: 1, ScanIndexForward: false}
	pageCount := 0

	// Iterating over the result pages
	isFirstPage := true
	for {
		if cursor.LastEvaluatedKey == nil && !isFirstPage {
			break
		}
		pageCount += 1
		resp, err := s.storageClient.ListPlayersByGoalsThresholdSorted(context.TODO(), testPlayerCountry2, womenNationalTeam, goalThreshold, cursor)
		if err != nil {
			fmt.Println("failed while listing limit specified number of player stats with goals filter in sorted order : ", err)
			os.Exit(1)
		} else {
			fmt.Println("Page Number : ", pageCount)
			printRecords(resp)
		}
		isFirstPage = false
	}
}

func (s *statsHandler) GetPlayerStats() {
	var dbOp dynamo.StatsRecord
	resp, err := s.storageClient.GetPlayerStats(context.TODO(), testPlayerCountry1, menNationalTeam, testPlayerFirstName, testPlayerLastName)
	if err != nil {
		fmt.Println("failed while fetching player stats : ", err)
		os.Exit(1)
	}
	if resp != nil {
		dbOp = *resp
		fmt.Println(dbOp)
	}
}
