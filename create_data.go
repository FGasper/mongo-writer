package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mongodb-labs/migration-tools/humantools"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	seedsCreated       = sync.Map{} // collName → struct{}{}
	useServerSideAgg   = false
)

func runCreateData(ctx context.Context, _ any, docsPerBatch int) error {
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	db = client.Database("test")
	var err2 error
	versionArray, err2 = GetVersionArray(ctx, client)
	if err2 != nil {
		return err2
	}

	setupLogging()

	oldState, restoreTerm, err := setupTerminalRawMode(ctx)
	if err != nil {
		panic(err)
	}
	defer restoreTerm()

	// Setup signal handling for create-data (different from modify-data)
	sigCtx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-sigCtx.Done()
		restoreTerm()
		os.Exit(0)
	}()

	// Initial setup phase - ensure collections exist
	slog.Info("Setting up collections...")
	setupComplete := false
	for _, useCustomID := range customIDModes {
		for _, docSize := range docSizes {
			collName := getCollectionName(useCustomID, docSize)
			coll := db.Collection(collName)
			// Trigger collection creation by getting stats (or could do a single insert)
			_ = coll.Database().RunCommand(sigCtx, bson.D{{Key: "collStats", Value: collName}}).Err()
		}
	}

	// Setup sharding if cluster is sharded
	slog.Info("Setting up sharding (if applicable)...")
	if err := setupSharding(sigCtx, client); err != nil {
		return fmt.Errorf("setup sharding: %w", err)
	}

	slog.Info("Setup complete. Press up/down to add/remove threads, Enter to show status, Ctrl+C to exit.")

	setupComplete = true

	// Thread management
	threadCancel := xsync.NewMap[int, context.CancelCauseFunc]()

	addThread := func() {
		threadNum := threadCancel.Size()

		threadCtx, canceler := context.WithCancelCause(sigCtx)
		threadCancel.Store(threadNum, canceler)

		go func() {
			defer func() {
				canceler, ok := threadCancel.LoadAndDelete(threadNum)
				lo.Assert(ok, "thread %d had no canceler?")

				canceler(fmt.Errorf("thread %d ending", threadNum))

				slog.Info("Thread ended.", "remaining", threadCancel.Size())
			}()

			for {
				if err := performCreateWork(threadCtx, docsPerBatch); err != nil {
					if errors.Is(err, context.Canceled) {
						break
					}
					slog.Warn("Thread failed; will retry.", "error", err, "remaining", threadCancel.Size())
					time.Sleep(2 * time.Second)
				}
			}
		}()
	}

	removeThread := func() bool {
		index := -1

		threadCancel.RangeRelaxed(func(key int, value context.CancelCauseFunc) bool {
			index = max(index, key)
			return true
		})

		if index == -1 {
			return false
		}

		canceler, ok := threadCancel.Load(index)
		if !ok {
			return false
		}

		canceler(fmt.Errorf("manual shutdown"))

		return true
	}

	// Start size logging goroutine
	go logSizes(sigCtx)

	// Start with 1 thread
	printRaw("Starting 1 thread.")
	addThread()

	// Keyboard input loop
	return handleKeyboardInput(restoreTerm, func(isUp bool) error {
		if !setupComplete {
			slog.Warn("Doing nothing cuz sharding stuff still happening")
			return nil
		}
		if isUp {
			addThread()
			slog.Info("Added thread", "newCount", threadCancel.Size())
		} else {
			removeThread()
		}
		return nil
	}, oldState, true)
}

func performCreateInsert(ctx context.Context, coll *mongo.Collection, size int, useCustomID bool, docsPerBatch int) (int, error) {
	collName := coll.Name()

	// Use server-side aggregation if explicitly enabled (requires MongoDB 4.2+)
	if useServerSideAgg && VersionAtLeast(versionArray[:], 4, 2) {
		// Ensure seed document exists once per collection per process
		_, loaded := seedsCreated.LoadOrStore(collName, struct{}{})
		if !loaded {
			// First time seeing this collection in this process
			res := coll.Database().RunCommand(ctx, bson.D{
				{"insert", collName},
				{"documents", []bson.D{
					{
						{"_id", "$$seed$$"},
						{"str", ""},
						{"num", 0.0},
						{"a", 1},
					},
				}},
				{"ordered", false},
			})

			// Ignore duplicate key errors (E11000)
			if err := res.Err(); err != nil {
				if writeEx, ok := err.(mongo.WriteException); ok && len(writeEx.WriteErrors) > 0 && writeEx.WriteErrors[0].Code == 11000 {
					// Duplicate key error, that's fine
				} else {
					return 0, fmt.Errorf("seed insert: %w", err)
				}
			}
		}

		// Build the newRoot document
		newRoot := bson.D{
			{"str", strings.Repeat("x", size)},
			{"num", bson.D{{"$rand", bson.D{}}}},
			{"a", 1},
		}
		if useCustomID {
			newRoot = append(bson.D{{"_id", bson.D{{"$rand", bson.D{}}}}}, newRoot...)
		}

		// Run aggregation to generate and merge documents server-side
		pipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.D{{Key: "_id", Value: "$$seed$$"}}}},
			{{Key: "$addFields", Value: bson.D{{Key: "batchIndex", Value: bson.D{{Key: "$range", Value: bson.A{0, docsPerBatch}}}}}}},
			{{Key: "$unwind", Value: "$batchIndex"}},
			{{Key: "$replaceRoot", Value: bson.D{{Key: "newRoot", Value: newRoot}}}},
			{{Key: "$merge", Value: bson.D{
				{Key: "into", Value: collName},
				{Key: "whenMatched", Value: "keepExisting"},
				{Key: "whenNotMatched", Value: "insert"},
			}}},
		}

		cursor, err := coll.Aggregate(ctx, pipeline)
		if err != nil {
			return 0, fmt.Errorf("aggregation: %w", err)
		}
		_ = cursor.Close(ctx)

		return docsPerBatch, nil
	}

	// Default: client-side document creation and insertion
	docs := make([]any, docsPerBatch)
	baseString := randomString(size / 3)

	for i := 0; i < docsPerBatch; i++ {
		doc := bson.M{
			"str": baseString + randomString(2*size/3),
			"num": rand.Float64(),
			"a":   1,
		}
		if useCustomID {
			doc["_id"] = rand.Float64()
		}
		docs[i] = doc
	}

	res, err := coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil {
		return 0, err
	}
	return len(res.InsertedIDs), nil
}

func performCreateWork(ctx context.Context, docsPerBatch int) error {
	for {
		for _, useCustomID := range customIDModes {
			for _, docSize := range docSizes {
				collName := getCollectionName(useCustomID, docSize)
				coll := db.Collection(collName)

				// Insert
				_, err := performCreateInsert(ctx, coll, docSize, useCustomID, docsPerBatch)
				if err != nil {
					return fmt.Errorf("insert: %w", err)
				}

				// Update
				_, err = performSimpleUpdate(ctx, coll)
				if err != nil {
					return fmt.Errorf("simple update: %w", err)
				}
			}
		}
	}
}

func logSizes(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var totalSize int64
			for _, useCustomID := range customIDModes {
				for _, docSize := range docSizes {
					collName := getCollectionName(useCustomID, docSize)
					coll := db.Collection(collName)

					stats := coll.Database().RunCommand(ctx, bson.D{{Key: "collStats", Value: collName}})
					raw, err := stats.Raw()
					if err != nil {
						slog.Warn("Failed to get collection stats", "collection", collName, "error", err)
						continue
					}

					size := raw.Lookup("size").AsInt64()
					slog.Info("Collection size", "collection", collName, "size", humantools.FmtBytes(size))

					totalSize += size
				}
			}

			slog.Info("Total size", "value", humantools.FmtBytes(totalSize))
		}
	}
}
