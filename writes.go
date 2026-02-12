package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/mongodb-labs/migration-tools/bsontools"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	uri          = "mongodb://localhost:27017"
	newDocsCount = 50000

	workers = 50
)

var (
	docSizes      = []int{500, 1000, 2000}
	customIDModes = []bool{true, false}
)

func main() {
	ctx := context.Background()
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	db := client.Database("test")
	canUpdate, _ := checkUpdateCapability(client)
	allOldDocsCounts := make(map[string]int64)

	sg := sync.WaitGroup{}

	for range workers {
		sg.Go(
			func() {
				for {
					for _, docSize := range docSizes {
						for _, useCustomID := range customIDModes {
							collName := getCollectionName(useCustomID, docSize)
							coll := db.Collection(collName)

							startTime := time.Now()
							results := bson.M{"plainInserts": 0, "plainDeletes": 0}

							if _, exists := allOldDocsCounts[collName]; !exists {
								count, _ := coll.EstimatedDocumentCount(ctx)
								allOldDocsCounts[collName] = count
							}
							baseline := allOldDocsCounts[collName]

							// --- 1. INSERT ---
							fmt.Printf("%s: Inserting %d documents …\n", collName, newDocsCount)
							results["plainInserts"] = performInsert(ctx, coll, docSize, useCustomID)

							// --- 2. UPDATE ---
							if canUpdate {
								fmt.Printf("%s: Updating documents …\n", collName)
								results["updates"] = performUpdate(ctx, coll)
							}

							// --- 3. DELETE ---
							totalDeleted := 0
							for {
								curr, _ := coll.EstimatedDocumentCount(ctx)

								toDelete := curr - baseline

								if toDelete < 1 {
									break
								}

								fraction := toDelete / curr

								fmt.Printf("%s: Deleting about %d random documents …\n", collName, toDelete)

								delRes, err := coll.DeleteMany(ctx, bson.D{{"$sampleRate", fraction}})
								if err == nil {
									totalDeleted += int(delRes.DeletedCount)
								} else {
									fmt.Printf("Failed to delete: %v\n", err)
									break
								}
							}
							results["plainDeletes"] = totalDeleted

							elapsed := time.Since(startTime).Seconds()
							j, _ := json.Marshal(results)
							fmt.Printf("%s: Writes sent over %.2f secs: %s\n", collName, elapsed, j)
						}
					}
				}
			},
		)
	}

	sg.Wait()
}

func performUpdate(ctx context.Context, coll *mongo.Collection) int32 {
	// We use []bson.M for the outer stages (as requested),
	// but strictly use bson.D for the operators to avoid invalid map usage.
	pipeline := []bson.M{
		//{"$match": bson.D{{"$sampleRate", 0.01}}},
		//{"$limit": newDocsCount},

		{"$addFields": bson.D{{"randVal", bson.D{{"$rand", bson.D{}}}}}},

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
				bson.D{{"$lt", bson.A{bson.D{{"$rand", bson.D{}}}, 0.5}}},
				"$flag",
			}}}},
			{"score", bson.D{{"$cond", bson.A{
				bson.D{{"$and", bson.A{
					bson.D{{"$gte", bson.A{"$randVal", 0.4}}},
					bson.D{{"$lt", bson.A{"$randVal", 0.6}}},
				}}},
				bson.D{{"$floor", bson.D{{"$multiply", bson.A{bson.D{{"$rand", bson.D{}}}, 1000}}}}},
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

		/*
			{"$merge": bson.D{
				{"into", collName},
				{"on", "_id"},
				{"whenMatched", "replace"},
				{"whenNotMatched", "insert"},
			}},
		*/
	}

	/*
		_, err := coll.Aggregate(ctx, pipeline)
		if err != nil {
			fmt.Printf("Failed to update: %v\n", err)
		}
	*/

	res := coll.Database().RunCommand(
		ctx,
		bson.D{
			{"update", coll.Name()},
			{"ordered", false},
			{"updates", []bson.D{
				{
					{"q", bson.D{{"$sampleRate", 0.01}}},
					{"u", pipeline},
					{"multi", true},
				},
			}},
		},
	)
	raw, err := res.Raw()

	if err != nil {
		fmt.Printf("Failed to update: %v\n", err)
		return 0
	}

	return lo.Must(bsontools.RawLookup[int32](raw, "nModified"))
}

func performInsert(ctx context.Context, coll *mongo.Collection, size int, useCustomID bool) int {
	newDocs := make([]any, newDocsCount)
	padding := strings.Repeat("y", size)

	for i := 0; i < newDocsCount; i++ {
		doc := bson.M{
			"rand":        rand.Float64(),
			"str":         padding,
			"fromUpdates": true,
		}
		if useCustomID {
			doc["_id"] = rand.Float64()
		}
		newDocs[i] = doc
	}

	res, err := coll.InsertMany(ctx, newDocs, options.InsertMany().SetOrdered(false))
	if err != nil {
		fmt.Printf("Failed to insert: %v\n", err)
		time.Sleep(3 * time.Second)
		return 0
	}
	return len(res.InsertedIDs)
}

func getCollectionName(useCustomID bool, size int) string {
	prefix := "sequentialID"
	if useCustomID {
		prefix = "customID"
	}
	return fmt.Sprintf("%s_%d", prefix, size)
}

func checkUpdateCapability(client *mongo.Client) (bool, error) {
	var info bson.M
	err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"buildInfo", 1}}).Decode(&info)
	if err != nil {
		return false, err
	}
	v, _ := info["version"].(string)
	return strings.HasPrefix(v, "4.4") || v >= "5.0", nil
}
