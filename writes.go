package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/MatusOllah/slogcolor"
	"github.com/fatih/color"
	"github.com/goaux/timer"
	"github.com/mongodb-labs/migration-tools/bsontools"
	"github.com/mongodb-labs/migration-tools/history"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/samber/lo"
	"github.com/urfave/cli/v3"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/exp/constraints"
	"golang.org/x/term"
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

	writesHistory = history.New[int](time.Minute)

	localizer = message.NewPrinter(language.English)
)

func main() {
	cmd := &cli.Command{
		Name:  "mongo-writer",
		Usage: "A threaded MongoDB load generator",

		// FLags: Map directly to your variables
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "uri",
				Value:   "mongodb://localhost",
				Usage:   "MongoDB connection URI",
				Sources: cli.EnvVars("MONGO_URI"), // Auto-read from ENV
			},
			&cli.IntFlag{
				Name:    "workers",
				Aliases: []string{"w"},
				Value:   startWorkers,
				Usage:   "Number of initial concurrent workers",
			},
			&cli.IntFlag{
				Name:    "docsPerBatch",
				Aliases: []string{"d"},
				Value:   newDocsCount,
				Usage:   "Number of documents to insert per batch",
			},
			&cli.IntSliceFlag{
				Name:    "docSizes",
				Aliases: []string{"s"},
				Value:   docSizes,
				Usage:   "Document sizes (in bytes) to generate",
			},
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "Enable debug level logging",
			},
		},

		// ACTION: This is where your actual main() logic goes
		Action: func(ctx context.Context, cmd *cli.Command) error {
			// 1. Retrieve Flag Values
			uri = cmd.String("uri")
			startWorkers = cmd.Int("workers")
			newDocsCount = cmd.Int("docsPerBatch")

			// 2. Validate or Parse Complex Flags
			// (e.g., converting string slice to int slice)
			docSizes = cmd.IntSlice("docSizes")

			if cmd.Bool("debug") {
				logLevel = slog.LevelDebug
			}

			// 3. Run your application logic
			// return runLoadTest(ctx, uri, workers, docCount)
			return run(ctx)
		},
	}

	// EXECUTE
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Disconnect(ctx)

	db = client.Database("test")
	versionArray = lo.Must(GetVersionArray(ctx, client))

	for _, docSize := range docSizes {
		for _, useCustomID := range customIDModes {
			collName := getCollectionName(useCustomID, docSize)
			coll := db.Collection(collName)

			count := lo.Must(coll.EstimatedDocumentCount(ctx))
			allOldDocsCounts[collName] = count
		}
	}

	//----------------------------------------------

	// 1. Set Terminal to Raw Mode
	// This disables "echo" (seeing what you type) and buffering (waiting for Enter)
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		panic(err)
	}
	// CRITICAL: Restore terminal on exit, or your shell will be broken!
	restoreTerm := func() {
		_ = term.Restore(int(os.Stdin.Fd()), oldState)
	}
	defer restoreTerm()

	logOpts := *slogcolor.DefaultOptions
	logOpts.SrcFileMode = slogcolor.Nop

	// slogcolor’s defaults set the color in the background & leave a weird
	// space afterward. This uses zerolog.ConsoleWriter’s scheme instead.
	logOpts.LevelTags = map[slog.Level]string{
		slog.LevelDebug: color.CyanString("DBG"),
		slog.LevelInfo:  color.GreenString("INF"),
		slog.LevelWarn:  color.YellowString("WRN"),
		slog.LevelError: color.RedString("ERR"),
	}

	/*
		logOpts.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
			// If the key is "msg" and the value is empty, drop it.
			if a.Key == slog.MessageKey && a.Value.String() == "" {
				return slog.Attr{} // Return empty attr to discard
			}
			return a
		}
	*/

	logOpts.Level = logLevel

	crlfWriter := CRLFWriter{os.Stdout}
	slog.SetDefault(
		slog.New(
			slogcolor.NewHandler(crlfWriter, &logOpts),
		),
	)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGALRM)
	go func() {
		<-c
		restoreTerm() // Critical: Fix terminal before dying
		os.Exit(1)
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

					slog.Warn("Worker failed; will retry.", "error", err, "remaining", workerCancel.Size())

					// No need to check the error since doWork will fail.
					_ = timer.SleepCause(workerCtx, 2*time.Second)
				}
			}
		}()
	}

	go func() {
		time.Sleep(5 * time.Second)

		for {
			var perSecond float64

			logs := writesHistory.Get()

			if len(logs) > 0 {
				elapsed := time.Since(logs[0].At)
				perSecond = float64(history.SumLogs(logs)) / elapsed.Seconds()
			}

			slog.Info("Periodic stats",
				"writesPerSecond", localizer.Sprintf("%.02f", perSecond),
				"batches", len(logs),
			)

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
	// We read 3 bytes at a time to catch the full arrow key sequence
	b := make([]byte, 3)

	for {
		n, err := os.Stdin.Read(b)
		if err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}

		if n == 0 {
			continue
		}

		switch b[0] {
		case '\x03': // CTRL-C
			return nil
		case '\x1a':
			restoreTerm()

			// Register for SIGCONT before stopping
			contC := make(chan os.Signal, 1)
			signal.Notify(contC, syscall.SIGCONT)

			// Now stop — terminal is already restored
			p, _ := os.FindProcess(os.Getpid())
			p.Signal(syscall.SIGSTOP) // SIGTSTP, not SIGSTOP — shell can resume it

			// Block until SIGCONT (fg command)
			<-contC
			signal.Stop(contC)

			// Re-enter raw mode
			oldState, err = term.MakeRaw(int(os.Stdin.Fd()))
			if err != nil {
				return fmt.Errorf("re-entering raw mode: %w", err)
			}
		case '\x0d': // Enter
			slog.Info("", "workers", workerCancel.Size())
		case '\x1b':
			if b[1] == '[' {
				switch b[2] {
				case 'A': // up arrow
					addWorker()
					slog.Info("Added worker", "newCount", workerCancel.Size())

				case 'B': // down arrow
					popWorker()
				}
			}
		}
	}
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

				// --- 2. UPDATE ---
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

func checkUpdateCapability(ctx context.Context, client *mongo.Client) (bool, error) {
	var info bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{"buildInfo", 1}}).Decode(&info)
	if err != nil {
		return false, err
	}
	v, _ := info["version"].(string)
	return strings.HasPrefix(v, "4.4") || v >= "5.0", nil
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
