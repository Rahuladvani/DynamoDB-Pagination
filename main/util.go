package main

import (
	"fmt"
	"pagination/storage/dynamo"
)

func PrintRecords(records []*dynamo.StatsRecord) {
	for _, record := range records {
		if record != nil {
			fmt.Printf("%+v\n", *record)
		}
	}
}

func LoadSeedDataInMemory() []dynamo.StatsRecord {
	seedRecords := []dynamo.StatsRecord{
		{
			PartitionKey: "Portugal",
			SortKey:      "MNT#Cristiano#Ronaldo",
			Goals:        118,
			Assists:      43,
			Appearances:  196,
			Country:      "Portugal",
			NationalTeam: "MNT",
			FirstName:    "Cristiano",
			LastName:     "Ronaldo",
		},
		{
			PartitionKey: "Argentina",
			SortKey:      "MNT#Lionel#Messi",
			Goals:        98,
			Assists:      55,
			Appearances:  172,
			Country:      "Argentina",
			NationalTeam: "MNT",
			FirstName:    "Lionel",
			LastName:     "Messi",
		},
		{
			PartitionKey: "India",
			SortKey:      "MNT#Sunil#Chhetri",
			Goals:        84,
			Assists:      11,
			Appearances:  131,
			Country:      "India",
			NationalTeam: "MNT",
			FirstName:    "Sunil",
			LastName:     "Chhetri",
		},
		{
			PartitionKey: "USA",
			SortKey:      "WNT#Megan#Rapinoe",
			Goals:        63,
			Assists:      73,
			Appearances:  197,
			Country:      "USA",
			NationalTeam: "WNT",
			FirstName:    "Megan",
			LastName:     "Rapinoe",
		},
		{
			PartitionKey: "USA",
			SortKey:      "WNT#Alex#Morgan",
			Goals:        119,
			Assists:      47,
			Appearances:  200,
			Country:      "USA",
			NationalTeam: "WNT",
			FirstName:    "Alex",
			LastName:     "Morgan",
		},
		{
			PartitionKey: "Egypt",
			SortKey:      "MNT#Mohamed#Salah",
			Goals:        47,
			Assists:      26,
			Appearances:  85,
			Country:      "Egypt",
			NationalTeam: "MNT",
			FirstName:    "Mohamed",
			LastName:     "Salah",
		},
	}
	return seedRecords
}
