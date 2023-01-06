package storage

import (
	"context"

	stats "pagination/storage/dynamo"
)

type StatsReader interface {
	ListPlayers(ctx context.Context, country string, nationalTeam string) ([]*stats.StatsRecord, error)
	ListAllPlayers(ctx context.Context, country string, nationalTeam string) ([]*stats.StatsRecord, error)
	ListLimitedPlayers(ctx context.Context, country string, nationalTeam string, c *stats.Cursor) ([]*stats.StatsRecord, error)
	ListPlayersByGoalsThreshold(ctx context.Context, country string, nationalTeam string, goalThreshold int, cursor *stats.Cursor) ([]*stats.StatsRecord, error)
	ListPlayersByGoalsThresholdSorted(ctx context.Context, country string, nationalTeam string, goalThreshold int, cursor *stats.Cursor) ([]*stats.StatsRecord, error)
	GetPlayerStats(context.Context, string, string, string, string) (*stats.StatsRecord, error)
}

type Storage interface {
	StatsReader
}
