package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"strings"
	"syscall"
	"time"

	"github.com/goaux/timer"
	"github.com/mongodb-labs/migration-tools/bsontools"
	"github.com/mongodb-labs/migration-tools/history"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/exp/constraints"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	newDocsCount  = 50000
	docSizes      = []int{500, 1000, 2000}
	customIDModes = []bool{true, false}
	startWorkers  = 5
	uri           = "mongodb://localhost:27017"

	versionArray     [3]int
	db               *mongo.Database
	allOldDocsCounts = make(map[string]int64)

	logLevel = slog.LevelInfo

	writesHistory     = history.New[int](time.Minute)
	brokenPipeHistory = history.New[int](time.Minute)

	localizer = message.NewPrinter(language.English)
)

func runModifyData(ctx context.Context) (retErr error) {
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

	for _, docSize := range docSizes {
		for _, useCustomID := range customIDModes {
			collName := getCollectionName(useCustomID, docSize)
			coll := db.Collection(collName)

			count := lo.Must(coll.EstimatedDocumentCount(ctx))
			allOldDocsCounts[collName] = count
		}
	}

	//----------------------------------------------

	oldState, restoreTerm, err := setupTerminalRawMode()
	if err != nil {
		panic(err)
	}
	defer restoreTerm()

	// Ensure terminal is restored before returning any error
	defer func() {
		if retErr != nil {
			restoreTerm()
		}
	}()

	//----------------------------------------------

	workerCancel := xsync.NewMap[int, context.CancelCauseFunc]()

	addWorker := func() {
		workerNum := workerCancel.Size()

		workerCtx, canceler := context.WithCancelCause(ctx)
		workerCancel.Store(workerNum, canceler)

		go func() {
			defer func() {
				canceler, ok := workerCancel.LoadAndDelete(workerNum)
				lo.Assert(ok, "worker %d had no canceler?")

				canceler(fmt.Errorf("worker %d ending", workerNum))

				slog.Info("Worker ended.", "remaining", workerCancel.Size())
			}()

			for {
				if err := doWork(workerCtx); err != nil {
					if errors.Is(err, context.Canceled) {
						break
					}

					var delay time.Duration

					// Broken-pipe errors are so frequent that there’s little point
					// in surfacing them.
					if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, syscall.EPIPE) || strings.Contains(err.Error(), "broken pipe") {
						delay = 10 * time.Millisecond
						brokenPipeHistory.Add(1)
					} else {
						delay = 2 * time.Second
						slog.Warn("Worker failed; will retry.", "error", err, "remaining", workerCancel.Size())
					}

					// No need to check the error since doWork will fail.
					_ = timer.SleepCause(workerCtx, delay)
				}
			}
		}()
	}

	go func() {
		time.Sleep(5 * time.Second)

		for {
			var writesPerSecond, brokenPipesPerSecond, failurePercentage float64

			logs := writesHistory.Get()
			totalWrites := history.SumLogs(logs)

			if len(logs) > 0 {
				elapsed := time.Since(logs[0].At)
				writesPerSecond = float64(totalWrites) / elapsed.Seconds()
			}

			pipeLogs := brokenPipeHistory.Get()
			totalBrokenPipes := history.SumLogs(pipeLogs)

			if len(pipeLogs) > 0 {
				brokenPipesPerSecond = history.RatePer(pipeLogs, time.Second)
			}

			if totalWrites+totalBrokenPipes > 0 {
				failurePercentage = (float64(totalBrokenPipes) / float64(totalWrites+totalBrokenPipes)) * 100
			}

			attrs := []any{
				"writesPerSecond", localizer.Sprintf("%.02f", writesPerSecond),
				"batches", len(logs),
			}
			if brokenPipesPerSecond > 0 {
				attrs = append(attrs, "brokenPipesPerSecond", localizer.Sprintf("%.02f", brokenPipesPerSecond))
			}
			if failurePercentage > 0 {
				attrs = append(attrs, "failurePercentage", localizer.Sprintf("%.02f", failurePercentage))
			}

			slog.Info("Periodic stats", attrs...)

			time.Sleep(10 * time.Second)
		}
	}()

	printRaw(localizer.Sprintf("Starting %d workers.", startWorkers))
	printRaw("Press up to add a worker or down to remove one.")

	for range startWorkers {
		addWorker()
		time.Sleep(100 * time.Millisecond)
	}

	popWorker := func() bool {
		index := -1

		workerCancel.RangeRelaxed(func(key int, value context.CancelCauseFunc) bool {
			index = max(index, key)
			return true
		})

		if index == -1 {
			return false
		}

		canceler, ok := workerCancel.Load(index)
		if !ok {
			return false
		}

		canceler(fmt.Errorf("manual shutdown"))

		return true
	}

	// 2. The Input Loop
	return handleKeyboardInput(restoreTerm, func(isUp bool) error {
		if isUp {
			addWorker()
			slog.Info("Added worker", "newCount", workerCancel.Size())
		} else {
			popWorker()
		}
		return nil
	}, oldState, true)
}

// A clean way to print in raw mode (since \n doesn't return carriage anymore)
func printRaw(s any) {
	fmt.Print(s, "\r\n")
}

func doWork(ctx context.Context) error {
	srcVersion, err := GetVersionArray(ctx, db.Client())
	if err != nil {
		return fmt.Errorf("getting source version: %w", err)
	}

	for {
		for _, docSize := range docSizes {
			for _, useCustomID := range customIDModes {
				collName := getCollectionName(useCustomID, docSize)
				coll := db.Collection(collName)

				startTime := time.Now()

				baseline := allOldDocsCounts[collName]

				var err error

				// --- 1. INSERT ---
				slog.Debug("Inserting documents.",
					"collection", collName,
					"count", localizer.Sprintf("%d", newDocsCount),
				)

				var attrs []slog.Attr

				inserts, err := performInsert(ctx, coll, docSize, useCustomID)
				if err != nil {
					return fmt.Errorf("insert: %w", err)
				}

				slog.Debug("Inserted documents.",
					"collection", collName,
					"count", localizer.Sprintf("%d", inserts),
				)

				writesHistory.Add(inserts)

				attrs = append(attrs, slog.Int("inserts", inserts))

				// --- 2. SIMPLE UPDATE ---
				slog.Debug("Simple updating documents.",
					"collection", collName,
				)

				simpleUpdates, err := performSimpleUpdate(ctx, coll)
				if err != nil {
					return fmt.Errorf("simple update: %w", err)
				}

				slog.Debug("Simple updated documents.",
					"collection", collName,
					"count", localizer.Sprintf("%d", simpleUpdates),
				)

				writesHistory.Add(int(simpleUpdates))

				attrs = append(attrs, slog.Int("simpleUpdates", int(simpleUpdates)))

				// --- 3. UPDATE ---
				slog.Debug("Updating documents.",
					"collection", collName,
				)

				updates, err := performUpdate(ctx, coll)
				if err != nil {
					return fmt.Errorf("update: %w", err)
				}

				slog.Debug("Updated documents.",
					"collection", collName,
					"count", localizer.Sprintf("%d", updates),
				)

				writesHistory.Add(int(updates))

				attrs = append(attrs, slog.Int("updates", int(updates)))

				// --- 3. DELETE ---
				totalDeleted := 0
				for {
					curr, _ := coll.EstimatedDocumentCount(ctx)

					toDelete := curr - baseline

					if toDelete < 1 {
						break
					}

					fraction := float64(toDelete) / float64(curr)

					slog.Debug("Deleting random documents.",
						"collection", collName,
						"count", localizer.Sprintf("%d", toDelete),
						"fraction", localizer.Sprintf("%f", fraction),
					)

					if VersionAtLeast(srcVersion[:], 4, 4) {
						delRes, err := coll.DeleteMany(ctx, bson.D{{"$sampleRate", fraction}})
						if err == nil {
							slog.Debug("Deleted documents.",
								"collection", collName,
								"count", localizer.Sprintf("%d", delRes.DeletedCount),
							)

							writesHistory.Add(int(delRes.DeletedCount))
							totalDeleted += int(delRes.DeletedCount)
						} else {
							return fmt.Errorf("delete: %w", err)
						}
					} else {
						ids, err := getDocIDs(ctx, coll, toDelete)
						if err != nil {
							return fmt.Errorf("getting doc IDs for delete: %w", err)
						}

						delRes, err := coll.DeleteMany(ctx, bson.D{{"_id", bson.D{{"$in", ids}}}})
						if err == nil {
							slog.Debug("Deleted documents.",
								"collection", collName,
								"count", localizer.Sprintf("%d", delRes.DeletedCount),
							)

							writesHistory.Add(int(delRes.DeletedCount))
							totalDeleted += int(delRes.DeletedCount)
						} else {
							return fmt.Errorf("delete: %w", err)
						}
					}
				}

				attrs = append(
					attrs,
					slog.String("deleted", localizer.Sprintf("%d", totalDeleted)),
					slog.Duration("elapsed", time.Since(startTime)),
					slog.String("collection", collName),
				)

				slog.Debug("Writes sent.", lo.ToAnySlice(attrs)...)
			}
		}
	}
}

func performSimpleUpdate(ctx context.Context, coll *mongo.Collection) (int32, error) {
	var query bson.D

	if VersionAtLeast(versionArray[:], 4, 4) {
		query = bson.D{{"$sampleRate", 0.01}}
	} else {
		ids, err := getDocIDs(ctx, coll, 50_000)
		if err != nil {
			return 0, fmt.Errorf("fetching doc IDs: %w", err)
		}

		query = bson.D{{"_id", bson.D{{"$in", ids}}}}
	}

	res := coll.Database().RunCommand(
		ctx,
		bson.D{
			{"update", coll.Name()},
			{"ordered", false},
			{"updates", []bson.D{
				{
					{"q", query},
					{"u", bson.D{
						{"$inc", bson.D{{"updateCount", 1}}},
						{"$set", bson.D{{"lastUpdated", time.Now()}}},
					}},
					{"multi", true},
				},
			}},
		},
	)
	raw, err := res.Raw()
	if err != nil {
		return 0, err
	}

	return bsontools.RawLookup[int32](raw, "nModified")
}

func performUpdate(ctx context.Context, coll *mongo.Collection) (int32, error) {
	// We use []bson.M for the outer stages (as requested),
	// but strictly use bson.D for the operators to avoid invalid map usage.

	randVal := lo.Ternary[any](
		VersionAtLeast(versionArray[:], 4, 4),
		bson.D{{"$rand", bson.D{}}},
		rand.Float64(),
	)

	pipeline := []bson.M{
		//{"$match": bson.D{{"$sampleRate", 0.01}}},
		//{"$limit": newDocsCount},

		{"$addFields": bson.D{{"randVal", randVal}}},

		{"$addFields": bson.D{
			{"touchedByProcess", bson.D{{"$cond", bson.A{
				bson.D{{"$lt", bson.A{"$randVal", 0.2}}},
				"go-worker",
				"$touchedByProcess",
			}}}},
			{"updatedAt", bson.D{{"$cond", bson.A{
				bson.D{{"$lt", bson.A{"$randVal", 0.2}}},
				"$$NOW",
				"$updatedAt",
			}}}},
			{"flag", bson.D{{"$cond", bson.A{
				bson.D{{"$and", bson.A{
					bson.D{{"$gte", bson.A{"$randVal", 0.2}}},
					bson.D{{"$lt", bson.A{"$randVal", 0.4}}},
				}}},
				bson.D{{"$lt", bson.A{randVal, 0.5}}},
				"$flag",
			}}}},
			{"score", bson.D{{"$cond", bson.A{
				bson.D{{"$and", bson.A{
					bson.D{{"$gte", bson.A{"$randVal", 0.4}}},
					bson.D{{"$lt", bson.A{"$randVal", 0.6}}},
				}}},
				bson.D{{"$floor", bson.D{{"$multiply", bson.A{randVal, 1000}}}}},
				"$score",
			}}}},
			{"visitCount", bson.D{{"$cond", bson.A{
				bson.D{{"$and", bson.A{
					bson.D{{"$gte", bson.A{"$randVal", 0.6}}},
					bson.D{{"$lt", bson.A{"$randVal", 0.8}}},
				}}},
				bson.D{{"$cond", bson.A{
					bson.D{{"$eq", bson.A{bson.D{{"$type", "$visitCount"}}, "missing"}}},
					1,
					bson.D{{"$add", bson.A{"$visitCount", 1}}},
				}}},
				"$visitCount",
			}}}},
			{"archivedField", bson.D{{"$cond", bson.A{
				bson.D{{"$gte", bson.A{"$randVal", 0.8}}},
				"$oldField",
				"$archivedField",
			}}}},
			{"oldField", bson.D{{"$cond", bson.A{
				bson.D{{"$gte", bson.A{"$randVal", 0.8}}},
				"$$REMOVE",
				"$oldField",
			}}}},
		}},

		{"$project": bson.D{{"randVal", 0}}},
	}

	/*
		_, err := coll.Aggregate(ctx, pipeline)
		if err != nil {
			fmt.Printf("Failed to update: %v\n", err)
		}
	*/

	if VersionAtLeast(versionArray[:], 4, 2) {
		var query bson.D

		if VersionAtLeast(versionArray[:], 4, 4) {
			query = bson.D{{"$sampleRate", 0.01}}
		} else {
			ids, err := getDocIDs(ctx, coll, 50_000)
			if err != nil {
				return 0, fmt.Errorf("fetching doc IDs: %w", err)
			}

			query = bson.D{{"_id", bson.D{{"$in", ids}}}}
		}

		res := coll.Database().RunCommand(
			ctx,
			bson.D{
				{"update", coll.Name()},
				{"ordered", false},
				{"updates", []bson.D{
					{
						{"q", query},
						{"u", pipeline},
						{"multi", true},
					},
				}},
			},
		)
		raw, err := res.Raw()
		if err != nil {
			return 0, err
		}

		return bsontools.RawLookup[int32](raw, "nModified")
	}

	ids, err := getDocIDs(ctx, coll, 100_000)
	if err != nil {
		return 0, fmt.Errorf("fetching doc IDs: %w", err)
	}

	cursor, err := coll.Aggregate(
		ctx,
		append(
			[]bson.M{
				{"$match": bson.M{
					"_id": bson.M{"$in": ids},
				}},
			},
			append(
				pipeline,
				bson.M{"$merge": bson.M{
					"into":           coll.Name(),
					"on":             "_id",
					"whenMatched":    "replace",
					"whenNotMatched": "insert",
				}},
			)...,
		),
	)
	if err != nil {
		return 0, fmt.Errorf("merge aggregation: %w", err)
	}
	_ = cursor.Close(ctx)

	return int32(len(ids)), nil
}

func performInsert(ctx context.Context, coll *mongo.Collection, size int, useCustomID bool) (int, error) {
	newDocs := make([]any, newDocsCount)

	// Assume that about 1/3 of each user document is keys, which are
	// mostly identical across documents.
	baseString := randomString(size / 3)

	for i := 0; i < newDocsCount; i++ {
		doc := bson.M{
			"rand":        rand.Float64(),
			"str":         baseString + randomString(2*size/3),
			"num":         rand.Float64(),
			"fromUpdates": true,
		}
		if useCustomID {
			doc["_id"] = rand.Float64()
		}
		newDocs[i] = doc
	}

	res, err := coll.InsertMany(ctx, newDocs, options.InsertMany().SetOrdered(false))
	if err != nil {
		return 0, err
	}
	return len(res.InsertedIDs), nil
}

func getDocIDs[T constraints.Integer](ctx context.Context, coll *mongo.Collection, count T) ([]bson.RawValue, error) {
	cursor, err := coll.Aggregate(
		ctx,
		mongo.Pipeline{
			{{"$sample", bson.D{{"size", count}}}},
			{{"$project", bson.D{{"_id", 1}}}},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("get %d doc IDs: %w", count, err)
	}

	var docs []bson.Raw
	err = cursor.All(ctx, &docs)
	if err != nil {
		return nil, fmt.Errorf("read %d doc IDs: %w", count, err)
	}

	return lo.Map(
		docs,
		func(doc bson.Raw, _ int) bson.RawValue {
			return doc.Lookup("_id")
		},
	), nil
}

func getCollectionName(useCustomID bool, size int) string {
	prefix := "sequentialID"
	if useCustomID {
		prefix = "customID"
	}
	return fmt.Sprintf("%s_%d", prefix, size)
}

func GetVersionArray(ctx context.Context, client *mongo.Client) ([3]int, error) {
	commandResult := client.Database("admin").RunCommand(ctx, bson.D{{"buildinfo", 1}})

	var va [3]int

	rawResp, err := commandResult.Raw()
	if err != nil {
		return va, fmt.Errorf("failed to run %#q: %w", "buildinfo", err)
	}

	bi := struct {
		VersionArray []int
	}{}

	err = bson.Unmarshal(rawResp, &bi)
	if err != nil {
		return va, fmt.Errorf("failed to decode build info version array: %w", err)
	}

	copy(va[:], bi.VersionArray)

	return va, nil
}

func VersionAtLeast(version []int, nums ...int) bool {
	lo.Assertf(
		len(nums) > 0,
		"need at least a major version to check version (%v) against",
		version,
	)

	for i := range nums {
		lo.Assertf(
			len(version) >= i+1,
			"version %v is too short to compare against %v",
			version,
			nums,
		)

		if version[i] < nums[i] {
			return false
		}

		if version[i] > nums[i] {
			break
		}
	}

	return true
}
