package gocosmos_test

import (
	"github.com/microsoft/gocosmos"
	"testing"
)

/*----------------------------------------------------------------------*/

func TestRestClient_CreateCollection(t *testing.T) {
	name := "TestRestClient_CreateCollection"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	collspecList := []gocosmos.CollectionSpec{
		{DbName: dbname, CollName: collname, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}},
		{DbName: dbname, CollName: collname, Ru: 400, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}},
		{DbName: dbname, CollName: collname, MaxRu: 10000, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}},
	}
	for _, collspec := range collspecList {
		_ensureDatabase(client, gocosmos.DatabaseSpec{Id: dbname})

		var collInfo gocosmos.CollInfo
		if result := client.CreateCollection(collspec); result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else if result.Id != collname {
			t.Fatalf("%s failed: <coll-id> expected %#v but received %#v", name+"/CreateDatabase", collname, result.Id)
		} else if result.Rid == "" || result.Self == "" || result.Etag == "" || result.Docs == "" ||
			result.Sprocs == "" || result.Triggers == "" || result.Udfs == "" || result.Conflicts == "" ||
			result.Ts <= 0 || len(result.IndexingPolicy) == 0 || len(result.PartitionKey) == 0 {
			t.Fatalf("%s failed: invalid collinfo returned %#v", name, result.CollInfo)
		} else {
			collInfo = result.CollInfo
		}

		if collspec.Ru > 0 || collspec.MaxRu > 0 {
			if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
				t.Fatalf("%s failed: %s", name, result.Error())
			} else {
				if ru, maxru := result.OfferThroughput(), result.MaxThroughputEverProvisioned(); collspec.Ru > 0 && (collspec.Ru != ru || collspec.Ru != maxru) {
					t.Fatalf("%s failed: <offer-throughput> expected %#v but expected {ru:%#v, maxru:%#v}", name, collspec.Ru, ru, maxru)
				}
				if ru, maxru := result.OfferThroughput(), result.MaxThroughputEverProvisioned(); collspec.MaxRu > 0 && (collspec.MaxRu != ru*10 || collspec.MaxRu != maxru) {
					t.Fatalf("%s failed: <max-throughput> expected %#v but expected {ru:%#v, maxru:%#v}", name, collspec.MaxRu, ru, maxru)
				}
			}
		}

		if result := client.CreateCollection(collspec); result.CallErr != nil {
			t.Fatalf("%s failed: %s", name, result.CallErr)
		} else if result.StatusCode != 409 {
			t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
		}
	}

	_deleteDatabase(client, "db_not_found")
	if result := client.CreateCollection(gocosmos.CollectionSpec{
		DbName:           "db_not_found",
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_CreateCollection_SubPartitions(t *testing.T) {
	name := "TestRestClient_CreateCollection_SubPartitions"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	collspecList := []gocosmos.CollectionSpec{
		// Hierarchical Partition Keys
		{DbName: dbname, CollName: collname, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/TenantId", "/UserId"}, "kind": "MultiHash", "version": 2}},
		{DbName: dbname, CollName: collname, MaxRu: 4000, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/TenantId", "/UserId", "/SessionId"}, "kind": "MultiHash", "version": 2}},
	}
	for _, collspec := range collspecList {
		_ensureDatabase(client, gocosmos.DatabaseSpec{Id: dbname})

		var collInfo gocosmos.CollInfo
		if result := client.CreateCollection(collspec); result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else if result.Id != collname {
			t.Fatalf("%s failed: <coll-id> expected %#v but received %#v", name+"/CreateDatabase", collname, result.Id)
		} else if result.Rid == "" || result.Self == "" || result.Etag == "" || result.Docs == "" ||
			result.Sprocs == "" || result.Triggers == "" || result.Udfs == "" || result.Conflicts == "" ||
			result.Ts <= 0 || len(result.IndexingPolicy) == 0 || len(result.PartitionKey) == 0 {
			t.Fatalf("%s failed: invalid collinfo returned %#v", name, result.CollInfo)
		} else {
			collInfo = result.CollInfo
		}

		if collspec.Ru > 0 || collspec.MaxRu > 0 {
			if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
				t.Fatalf("%s failed: %s", name, result.Error())
			} else {
				if ru, maxru := result.OfferThroughput(), result.MaxThroughputEverProvisioned(); collspec.Ru > 0 && (collspec.Ru != ru || collspec.Ru != maxru) {
					t.Fatalf("%s failed: <offer-throughput> expected %#v but expected {ru:%#v, maxru:%#v}", name, collspec.Ru, ru, maxru)
				}
				if ru, maxru := result.OfferThroughput(), result.MaxThroughputEverProvisioned(); collspec.MaxRu > 0 && (collspec.MaxRu != ru*10 || collspec.MaxRu != maxru) {
					t.Fatalf("%s failed: <max-throughput> expected %#v but expected {ru:%#v, maxru:%#v}", name, collspec.MaxRu, ru, maxru)
				}
			}
		}

		if result := client.CreateCollection(collspec); result.CallErr != nil {
			t.Fatalf("%s failed: %s", name, result.CallErr)
		} else if result.StatusCode != 409 {
			t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 409, result.StatusCode)
		}
	}
}

func TestRestClient_ChangeOfferCollection(t *testing.T) {
	name := "TestRestClient_ChangeOfferCollection"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: dbname})
	collspec := gocosmos.CollectionSpec{DbName: dbname, CollName: collname, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}}

	var collInfo gocosmos.CollInfo
	if result := client.CreateCollection(collspec); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else {
		collInfo = result.CollInfo
	}

	// collection is created with manual ru=400
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if ru, maxru := result.OfferThroughput(), result.MaxThroughputEverProvisioned(); ru != 400 || maxru != 400 {
		t.Fatalf("%s failed: <ru|maxru> expected %#v|%#v but recevied %#v|%#v", name, 400, 400, ru, maxru)
	}

	// change collection's manual ru to 500
	if result := client.ReplaceOfferForResource(collInfo.Rid, 500, 0); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, ru := result.IsAutopilot(), result.OfferThroughput(); ru != 500 || auto {
		t.Fatalf("%s failed: <auto|ru> expected %#v|%#v but recevied %#v|%#v", name, false, 500, auto, ru)
	}
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, ru := result.IsAutopilot(), result.OfferThroughput(); ru != 500 || auto {
		t.Fatalf("%s failed: <auto|ru> expected %#v|%#v but recevied %#v|%#v", name, false, 500, auto, ru)
	}

	// change collection's autopilot ru to 6000
	if result := client.ReplaceOfferForResource(collInfo.Rid, 0, 6000); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, maxru := result.IsAutopilot(), result.MaxThroughputEverProvisioned(); maxru != 6000 || !auto {
		t.Fatalf("%s failed: <auto|maxru> expected %#v|%#v but recevied %#v|%#v", name, true, 6000, auto, maxru)
	}
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, maxru := result.IsAutopilot(), result.MaxThroughputEverProvisioned(); maxru != 6000 || !auto {
		t.Fatalf("%s failed: <auto|maxru> expected %#v|%#v but recevied %#v|%#v", name, true, 6000, auto, maxru)
	}

	// change collection's autopilot ru to 7000
	if result := client.ReplaceOfferForResource(collInfo.Rid, 0, 7000); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, maxru := result.IsAutopilot(), result.MaxThroughputEverProvisioned(); maxru != 7000 || !auto {
		t.Fatalf("%s failed: <auto|maxru> expected %#v|%#v but recevied %#v|%#v", name, true, 7000, auto, maxru)
	}
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, maxru := result.IsAutopilot(), result.MaxThroughputEverProvisioned(); maxru != 7000 || !auto {
		t.Fatalf("%s failed: <auto|maxru> expected %#v|%#v but recevied %#v|%#v", name, true, 7000, auto, maxru)
	}

	// change collection's manual ru to 800
	if result := client.ReplaceOfferForResource(collInfo.Rid, 800, 0); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, ru := result.IsAutopilot(), result.OfferThroughput(); ru != 800 || auto {
		t.Fatalf("%s failed: <auto|ru> expected %#v|%#v but recevied %#v|%#v", name, false, 800, auto, ru)
	}
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto, ru := result.IsAutopilot(), result.OfferThroughput(); ru != 800 || auto {
		t.Fatalf("%s failed: <auto|ru> expected %#v|%#v but recevied %#v|%#v", name, false, 800, auto, ru)
	}

	// change collection's autopilot ru to auto
	if result := client.ReplaceOfferForResource(collInfo.Rid, 0, 0); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto := result.IsAutopilot(); !auto {
		t.Fatalf("%s failed: <auto> expected %#v but recevied %#v", name, true, auto)
	}
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto := result.IsAutopilot(); !auto {
		t.Fatalf("%s failed: <auto> expected %#v but recevied %#v", name, true, auto)
	}

	// change collection's autopilot ru to auto (again)
	if result := client.ReplaceOfferForResource(collInfo.Rid, 0, 0); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto := result.IsAutopilot(); !auto {
		t.Fatalf("%s failed: <auto> expected %#v but recevied %#v", name, true, auto)
	}
	if result := client.GetOfferForResource(collInfo.Rid); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if auto := result.IsAutopilot(); !auto {
		t.Fatalf("%s failed: <auto> expected %#v but recevied %#v", name, true, auto)
	}
}

func TestRestClient_ChangeOfferCollectionInvalid(t *testing.T) {
	name := "TestRestClient_ChangeOfferCollectionInvalid"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: dbname})
	collspec := gocosmos.CollectionSpec{DbName: dbname, CollName: collname, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}}

	var collInfo gocosmos.CollInfo
	if result := client.CreateCollection(collspec); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else {
		collInfo = result.CollInfo
	}

	if result := client.GetOfferForResource("not_found"); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but recevied %#v", name, 404, result.StatusCode)
	}
	if result := client.ReplaceOfferForResource("not_found", 400, 0); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but recevied %#v", name, 404, result.StatusCode)
	}

	if result := client.ReplaceOfferForResource(collInfo.Rid, 400, 4000); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 400 {
		t.Fatalf("%s failed: <status-code> expected %#v but recevied %#v", name, 400, result.StatusCode)
	}
}

func TestRestClient_CreateCollectionIndexingPolicy(t *testing.T) {
	name := "TestRestClient_CreateCollectionIndexingPolicy"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	collSpec := gocosmos.CollectionSpec{
		DbName: dbname, CollName: collname,
		IndexingPolicy:   map[string]interface{}{"indexingMode": "consistent", "automatic": true},
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}}
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: dbname})
	if result := client.CreateCollection(collSpec); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}
}

func TestRestClient_ReplaceCollection(t *testing.T) {
	name := "TestRestClient_ReplaceCollection"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: dbname})
	_ensureCollection(client, gocosmos.CollectionSpec{DbName: dbname, CollName: collname, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"}})

	collspecList := []gocosmos.CollectionSpec{
		{DbName: dbname, CollName: collname, Ru: 800, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
			IndexingPolicy: map[string]interface{}{"indexingMode": "consistent", "automatic": true,
				"includedPaths": []map[string]interface{}{{"path": "/*", "indexes": []map[string]interface{}{{"dataType": "Number", "precision": -1, "kind": "Range"}}}}, "excludedPaths": []map[string]interface{}{},
			}},
		{DbName: dbname, CollName: collname, MaxRu: 8000, PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
			IndexingPolicy: map[string]interface{}{"indexingMode": "consistent", "automatic": true,
				"includedPaths": []map[string]interface{}{{"path": "/*", "indexes": []map[string]interface{}{{"dataType": "String", "precision": 3, "kind": "Hash"}}}}, "excludedPaths": []map[string]interface{}{},
			}},
	}
	for _, colspec := range collspecList {
		if result := client.ReplaceCollection(colspec); result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		} else if result.Id != collname {
			t.Fatalf("%s failed: <coll-id> expected %#v but received %#v", name+"/CreateDatabase", collname, result.Id)
		} else if result.Rid == "" || result.Self == "" || result.Etag == "" || result.Docs == "" ||
			result.Sprocs == "" || result.Triggers == "" || result.Udfs == "" || result.Conflicts == "" ||
			result.Ts <= 0 || len(result.IndexingPolicy) == 0 || len(result.PartitionKey) == 0 {
			t.Fatalf("%s failed: invalid collinfo returned %#v", name, result.CollInfo)
		}
	}

	_deleteCollection(client, dbname, "table_not_found")
	if result := client.ReplaceCollection(gocosmos.CollectionSpec{
		DbName:           dbname,
		CollName:         "table_not_found",
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	_deleteDatabase(client, "db_not_found")
	if result := client.ReplaceCollection(gocosmos.CollectionSpec{
		DbName:           "db_not_found",
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
	}); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_DeleteCollection(t *testing.T) {
	name := "TestRestClient_DeleteCollection"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: dbname})
	_ensureCollection(client, gocosmos.CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
	})
	if result := client.DeleteCollection(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	}
	if result := client.DeleteCollection(dbname, collname); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	_deleteDatabase(client, "db_not_found")
	if result := client.DeleteCollection("db_not_found", collname); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_GetCollection(t *testing.T) {
	name := "TestRestClient_GetCollection"
	client := _newRestClient(t, name)

	dbname := testDb
	collname := testTable
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: dbname})
	_ensureCollection(client, gocosmos.CollectionSpec{
		DbName:           dbname,
		CollName:         collname,
		PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
	})
	if result := client.GetCollection(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if result.Id != collname {
		t.Fatalf("%s failed: <coll-id> expected %#v but received %#v", name, collname, result.Id)
	} else if result.Rid == "" || result.Self == "" || result.Etag == "" || result.Docs == "" ||
		result.Sprocs == "" || result.Triggers == "" || result.Udfs == "" || result.Conflicts == "" ||
		result.Ts <= 0 || len(result.IndexingPolicy) == 0 || len(result.PartitionKey) == 0 ||
		len(result.ConflictResolutionPolicy) == 0 || len(result.GeospatialConfig) == 0 {
		t.Fatalf("%s failed: invalid collinfo returned %#v", name, result.CollInfo)
	}

	_deleteCollection(client, dbname, "table_not_found")
	if result := client.GetCollection(dbname, "table_not_found"); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}

	_deleteDatabase(client, "db_not_found")
	if result := client.GetCollection("db_not_found", "table_not_found"); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}

func TestRestClient_ListCollection(t *testing.T) {
	name := "TestRestClient_ListCollection"
	client := _newRestClient(t, name)

	dbname := testDb
	_ensureDatabase(client, gocosmos.DatabaseSpec{Id: dbname})
	collnames := map[string]int{"table1": 1, "table3": 1, "table5": 1, "table4": 1, "table2": 1}
	for collname := range collnames {
		result := client.CreateCollection(gocosmos.CollectionSpec{
			DbName:           dbname,
			CollName:         collname,
			PartitionKeyInfo: map[string]interface{}{"paths": []string{"/id"}, "kind": "Hash"},
		})
		if result.Error() != nil {
			t.Fatalf("%s failed: %s", name, result.Error())
		}
	}
	if result := client.ListCollections(dbname); result.Error() != nil {
		t.Fatalf("%s failed: %s", name, result.Error())
	} else if int(result.Count) != len(collnames) {
		t.Fatalf("%s failed: number of returned collections %#v", name, result.Count)
	} else {
		for _, coll := range result.Collections {
			delete(collnames, coll.Id)
		}
		if len(collnames) != 0 {
			t.Fatalf("%s failed: collections not returned %#v", name, collnames)
		}
	}

	_deleteDatabase(client, "db_not_found")
	if result := client.ListCollections("db_not_found"); result.CallErr != nil {
		t.Fatalf("%s failed: %s", name, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", name, 404, result.StatusCode)
	}
}
