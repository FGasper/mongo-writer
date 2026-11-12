package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/mongodb-labs/migration-tools/humantools"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func runPruneData(ctx context.Context, cmd any, percentage float64) (retErr error) {
	if percentage < 0 || percentage > 100 {
		return fmt.Errorf("percentage must be between 0 and 100, got %.4f", percentage)
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	database := client.Database("test")

	collections, err := database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("list collections: %w", err)
	}

	if len(collections) == 0 {
		slog.Info("No collections found")
		return nil
	}

	setupLogging()
	slog.Info(fmt.Sprintf("Pruning %s%% of data across all collections …", humantools.FmtPercent(percentage, 100)))

	sampleRate := percentage / 100
	var docsDeleted atomic.Int64
	var collsProcessed atomic.Int64

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	for _, collName := range collections {
		wg.Go(func() {
			coll := database.Collection(collName)
			result, err := coll.DeleteMany(ctx, bson.D{{"$sampleRate", sampleRate}})
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("delete %s: %w", collName, err))
				mu.Unlock()

				slog.Warn("Deletion failed", "collection", collName, "error", err)

				return
			}
			docsDeleted.Add(result.DeletedCount)
			collsProcessed.Add(1)
			slog.Info("Finished collection.", "collection", collName, "deleted", result.DeletedCount)
		})
	}

	wg.Wait()

	slog.Info("Finished all collections",
		"collections", collsProcessed.Load(),
		"documents", docsDeleted.Load(),
	)

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
