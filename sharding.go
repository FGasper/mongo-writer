package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"

	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func clusterIsSharded(ctx context.Context, client *mongo.Client) bool {
	admin := client.Database("admin")
	result := admin.RunCommand(ctx, bson.D{{Key: "getShardMap", Value: 1}})
	fmt.Printf("result: ")
	return result.Err() == nil
}

func getShardNames(ctx context.Context, client *mongo.Client) ([]string, error) {
	shardMap := struct {
		Map map[string]string
	}{}

	admin := client.Database("admin")
	err := admin.RunCommand(ctx, bson.D{{Key: "getShardMap", Value: 1}}).Decode(&shardMap)
	if err != nil {
		return nil, err
	}

	return lo.Without(
		slices.Sorted(maps.Keys(shardMap.Map)),
		"config",
	), nil
}

func setupSharding(ctx context.Context, client *mongo.Client) error {
	if !clusterIsSharded(ctx, client) {
		slog.Info("Cluster is not sharded. Skipping sharding setup.")
		return nil
	}

	slog.Info("Cluster is sharded. Setting up sharding …")

	// Stop the balancer
	admin := client.Database("admin")
	stopRes := admin.RunCommand(ctx, bson.D{{Key: "balancerStop", Value: 1}})
	if err := stopRes.Err(); err != nil {
		return fmt.Errorf("stop balancer: %w", err)
	} else {
		slog.Info("Stopped balancer")
	}

	// Get shard names
	shardNames, err := getShardNames(ctx, client)
	if err != nil {
		return fmt.Errorf("get shard names: %w", err)
	}

	slog.Info("Found shards", "count", len(shardNames), "shards", shardNames)

	collNames, err := client.Database(dbName).ListCollectionNames(
		ctx,
		bson.D{},
	)
	if err != nil {
		return fmt.Errorf("get DB %#q collection names: %w", dbName, err)
	}

	// Setup sharding for each collection
	for _, useCustomID := range customIDModes {
		for _, docSize := range docSizes {
			collName := getCollectionName(useCustomID, docSize)

			if slices.Contains(collNames, collName) {
				slog.Info("Collection already exists.", "name", collName)
				continue
			}

			if err := setupCollectionSharding(ctx, client, collName, useCustomID, shardNames); err != nil {
				return fmt.Errorf("setup sharding for %s: %w", collName, err)
			}
		}
	}

	return nil
}

func setupCollectionSharding(ctx context.Context, client *mongo.Client, collName string, useCustomID bool, shardNames []string) error {
	db := client.Database("test")
	admin := client.Database("admin")

	ns := "test." + collName

	// Enable sharding on database
	enableRes := admin.RunCommand(ctx, bson.D{
		{"enableSharding", db.Name()},
	})
	if err := enableRes.Err(); err != nil {
		return fmt.Errorf("enable sharding on DB %#q: %w", db.Name(), err)
	}

	// Shard the collection
	shardKey := bson.M{"_id": 1}

	if !useCustomID {
		// For sequential ID, create a hashed index
		_, err := db.Collection(collName).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{Key: "_id", Value: "hashed"}},
		})
		if err != nil {
			return fmt.Errorf("create %#q’s hashed index: %w", collName, err)
		}
		shardKey = bson.M{"_id": "hashed"}
	}

	slog.Info("Sharding collection", "collection", collName, "shardKey", shardKey)
	shardRes := admin.RunCommand(ctx, bson.D{
		{"shardCollection", ns},
		{"key", shardKey},
	})

	if err := shardRes.Err(); err != nil {
		return fmt.Errorf("shard collection: %w", err)
	}

	slog.Info("Collection is now sharded.", "collection", collName)

	// For custom ID mode, pre-split the collection
	if useCustomID {
		slog.Info("Pre-splitting collection", "collection", collName)

		maxDocID := 1.0
		if err := splitCollection(ctx, admin, ns, "_id", 0, maxDocID, shardNames); err != nil {
			return fmt.Errorf("split collection: %w", err)
		}
	}

	return nil
}

func splitCollection(ctx context.Context, admin *mongo.Database, ns string, shardKeyField string, min, max float64, shardNames []string) error {
	shardCount := len(shardNames)
	if shardCount == 0 {
		return fmt.Errorf("no shards found")
	}

	slog.Info("Splitting collection", "namespace", ns, "shardCount", shardCount, "range", fmt.Sprintf("[%v, %v]", min, max))

	// Create N-1 splits at evenly spaced boundaries
	for i := 1; i < shardCount; i++ {
		boundaryValue := min + (max-min)*(float64(i)/float64(shardCount))
		boundary := bson.D{{Key: shardKeyField, Value: boundaryValue}}

		slog.Info("Splitting at boundary", "field", shardKeyField, "value", boundaryValue)
		splitRes := admin.RunCommand(ctx, bson.D{
			{Key: "split", Value: ns},
			{Key: "middle", Value: boundary},
		})

		if err := splitRes.Err(); err != nil {
			slog.Warn("Split failed", "value", boundaryValue, "error", err)
		} else {
			slog.Info("Split succeeded", "value", boundaryValue)
		}
	}

	// Move one chunk to each shard
	for i := 0; i < shardCount; i++ {
		var low, high float64

		if i == 0 {
			low = 0.0
			high = 1.0 / float64(shardCount)
		} else if i == shardCount-1 {
			low = float64(i) / float64(shardCount)
			high = 1.0
		} else {
			low = float64(i) / float64(shardCount)
			high = float64(i+1) / float64(shardCount)
		}

		low = min + (max-min)*low
		high = min + (max-min)*high
		mid := (low + high) / 2.0

		findDoc := bson.M{shardKeyField: mid}
		toShard := shardNames[i]

		slog.Info("Moving chunk to shard",
			"field", shardKeyField,
			"mid", mid,
			"range", fmt.Sprintf("[%v, %v]", low, high),
			"shard", toShard,
		)

		moveRes := admin.RunCommand(ctx, bson.D{
			{Key: "moveChunk", Value: ns},
			{Key: "find", Value: findDoc},
			{Key: "to", Value: toShard},
			{Key: "_waitForDelete", Value: true},
			{Key: "_secondaryThrottle", Value: true},
			{Key: "writeConcern", Value: bson.D{
				{Key: "w", Value: "majority"},
				{Key: "j", Value: true},
			}},
		})

		if err := moveRes.Err(); err != nil {
			slog.Warn("moveChunk failed", "shard", toShard, "error", err)
		} else {
			slog.Info("moveChunk succeeded", "shard", toShard)
		}
	}

	slog.Info("Done splitting and distributing collection", "namespace", ns)
	return nil
}
