package gocosmos_test

import (
	"encoding/json"
	"fmt"
	"github.com/microsoft/gocosmos"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/btnguyen2k/consu/reddo"
)

/*----------------------------------------------------------------------*/

type queryTestCase struct {
	name             string
	query            string
	expectedNumItems int
	maxItemCount     int

	distinctQuery int // 0=non-distinct, 1=distinct values, other: distinct docs
	distinctField string

	orderField     string
	orderDirection string
	orderType      reflect.Type

	groupByAggr string

	rewrittenSql bool
}

func TestRestClient_QueryDocuments_DbOrTableNotExists(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_DbOrTableNotExists"
	dbname := testDb
	collname := testTable
	client := _newRestClient(t, testName)
	_initDataSmallRU(t, testName, client, dbname, collname, 0)
	_deleteDatabase(client, "db_not_exists")
	_deleteCollection(client, dbname, "table_not_exists")

	query := gocosmos.QueryReq{DbName: dbname, CollName: collname, Query: "SELECT * FROM c"}

	query.DbName = dbname
	query.CollName = "table_not_exists"
	if result := client.QueryDocuments(query); result.CallErr != nil {
		t.Fatalf("%s failed: %s", testName, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", testName, 404, result.StatusCode)
	}

	query.DbName = "db_not_exists"
	query.CollName = collname
	if result := client.QueryDocuments(query); result.CallErr != nil {
		t.Fatalf("%s failed: %s", testName, result.CallErr)
	} else if result.StatusCode != 404 {
		t.Fatalf("%s failed: <status-code> expected %#v but received %#v", testName, 404, result.StatusCode)
	}
}

type funcTestFatal func(msg string)

func _verifyResult(f funcTestFatal, testName string, testCase queryTestCase, expectedNumItems int, queryResult interface{}) {
	switch queryResult.(type) {
	case *gocosmos.RespQueryDocs:
		_verifyResultRespQueryDocs(f, testName, testCase, expectedNumItems, queryResult.(*gocosmos.RespQueryDocs))
	case []map[string]interface{}:
		_verifyResultDriverSelect(f, testName, testCase, expectedNumItems, queryResult.([]map[string]interface{}))
	}
}

func _verifyResultDriverSelect(f funcTestFatal, testName string, testCase queryTestCase, expectedNumItems int, rows []map[string]interface{}) {
	for i, row := range rows {
		if row == nil {
			f(fmt.Sprintf("%s failed: row at %d-th is nil", testName, i))
		}
	}
	if testCase.groupByAggr == "" {
		if testCase.maxItemCount > 0 && expectedNumItems <= 0 && len(rows) > testCase.maxItemCount {
			f(fmt.Sprintf("%s failed: <num-document> expected not exceeding %#v but received %#v", testName, testCase.maxItemCount, len(rows)))
		}
		if (testCase.maxItemCount <= 0 || expectedNumItems > 0) && len(rows) != expectedNumItems {
			f(fmt.Sprintf("%s failed: <num-document> expected %#v but received %#v", testName, expectedNumItems, len(rows)))
		}
	}
	for i, row := range rows {
		if testCase.groupByAggr == "" && testCase.distinctQuery == 0 {
			var docInfo gocosmos.DocInfo = row
			id, _ := strconv.Atoi(docInfo.Id())
			if !reflect.DeepEqual(docInfo.RemoveSystemAttrs(), dataList[id]) {
				f(fmt.Sprintf("%s failed: %#v-th document expected to be\n%#v\nbut received\n%#v", testName, i, dataList[id], docInfo.RemoveSystemAttrs()))
			}
		}
	}
}

func _verifyResultRespQueryDocs(f funcTestFatal, testName string, testCase queryTestCase, expectedNumItems int, queryResult *gocosmos.RespQueryDocs) {
	if queryResult.Error() != nil {
		f(fmt.Sprintf("%s failed: %s", testName, queryResult.Error()))
	}
	for i, doc := range queryResult.Documents {
		if doc == nil {
			f(fmt.Sprintf("%s failed: document at %d-th is nil", testName, i))
		}
	}
	if testCase.groupByAggr == "" {
		if testCase.maxItemCount > 0 && expectedNumItems <= 0 && (len(queryResult.Documents) > testCase.maxItemCount || queryResult.Count > testCase.maxItemCount) {
			for _, doc := range queryResult.Documents {
				fmt.Printf("[DEBUG] %#v\n", doc)
			}
			f(fmt.Sprintf("%s failed: <num-document> expected not exceeding %#v but received (len: %#v / count: %#v)", testName, testCase.maxItemCount, len(queryResult.Documents), queryResult.Count))
		}
		if (testCase.maxItemCount <= 0 || expectedNumItems > 0) && (len(queryResult.Documents) != expectedNumItems || queryResult.Count != expectedNumItems) {
			for _, doc := range queryResult.Documents {
				fmt.Printf("[DEBUG] %#v\n", doc)
			}
			f(fmt.Sprintf("%s failed: <num-document> expected %#v but received (len: %#v / count: %#v)", testName, expectedNumItems, len(queryResult.Documents), queryResult.Count))
		}
	}
	for i := range queryResult.Documents {
		if testCase.groupByAggr == "" && testCase.distinctQuery == 0 {
			docInfo := queryResult.Documents.AsDocInfoAt(i)
			id, _ := strconv.Atoi(docInfo.Id())
			if !reflect.DeepEqual(docInfo.RemoveSystemAttrs(), dataList[id]) {
				for _, doc := range queryResult.Documents {
					fmt.Printf("[DEBUG] %#v\n", doc)
				}
				f(fmt.Sprintf("%s failed: %#v-th document expected to be\n%#v\nbut received\n%#v", testName, i, dataList[id], docInfo.RemoveSystemAttrs()))
			}
		}
	}
}

func _verifyDistinct(f funcTestFatal, testName string, testCase queryTestCase, queryResult interface{}) {
	if testCase.distinctQuery == 0 {
		return
	}
	switch queryResult.(type) {
	case *gocosmos.RespQueryDocs:
		_verifyDistinctRespQueryDocs(f, testName, testCase, queryResult.(*gocosmos.RespQueryDocs))
	case []map[string]interface{}:
		_verifyDistinctDriverSelect(f, testName, testCase, queryResult.([]map[string]interface{}))
	}
}

func _verifyDistinctDriverSelect(f funcTestFatal, testName string, testCase queryTestCase, rows []map[string]interface{}) {
	for i, row := range rows {
		if row == nil {
			f(fmt.Sprintf("%s failed: row at %d-th is nil", testName, i))
		}
	}
	distinctSet := make(map[string]bool)
	for _, doc := range rows {
		js, _ := json.Marshal(doc)
		distinctSet[string(js)] = true
	}
	expectedNumItems := testCase.expectedNumItems
	if testCase.maxItemCount > 0 && len(distinctSet) > testCase.maxItemCount {
		f(fmt.Sprintf("%s failed: expected max %#v distinct rows, but received %#v", testName, testCase.maxItemCount, rows))
	}
	if testCase.maxItemCount <= 0 && len(distinctSet) != expectedNumItems {
		f(fmt.Sprintf("%s failed: expected %#v distinct rows, but received %#v", testName, expectedNumItems, rows))
	}
}

func _verifyDistinctRespQueryDocs(f funcTestFatal, testName string, testCase queryTestCase, queryResult *gocosmos.RespQueryDocs) {
	for i, doc := range queryResult.Documents {
		if doc == nil {
			f(fmt.Sprintf("%s failed: document at %d-th is nil", testName, i))
		}
	}
	distinctSet := make(map[string]bool)
	for _, doc := range queryResult.Documents {
		js, _ := json.Marshal(doc)
		distinctSet[string(js)] = true
	}
	expectedNumItems := testCase.expectedNumItems
	if testCase.maxItemCount > 0 && len(distinctSet) > testCase.maxItemCount {
		f(fmt.Sprintf("%s failed: expected max %#v distinct rows, but received %#v", testName, testCase.maxItemCount, queryResult.Documents))
	}
	if testCase.maxItemCount <= 0 && len(distinctSet) != expectedNumItems {
		f(fmt.Sprintf("%s failed: expected %#v distinct rows, but received %#v", testName, expectedNumItems, queryResult.Documents))
	}
}

func _verifyOrderBy(f funcTestFatal, testName string, testCase queryTestCase, queryResult interface{}) {
	if testCase.orderField == "" {
		return
	}
	switch queryResult.(type) {
	case *gocosmos.RespQueryDocs:
		_verifyOrderByRespQueryDocs(f, testName, testCase, queryResult.(*gocosmos.RespQueryDocs))
	case []map[string]interface{}:
		_verifyOrderByDriverSelect(f, testName, testCase, queryResult.([]map[string]interface{}))
	}
}

func _verifyOrderByDriverSelect(f funcTestFatal, testName string, testCase queryTestCase, rows []map[string]interface{}) {
	for i, row := range rows {
		if row == nil {
			f(fmt.Sprintf("%s failed: row at %d-th is nil", testName, i))
		}
	}
	odir := strings.ToUpper(testCase.orderDirection)
	var prevDoc interface{}
	for _, doc := range rows {
		if prevDoc != nil {
			var pv, cv interface{}
			var err error
			if pv, err = reddo.Convert(prevDoc.(map[string]interface{})[testCase.orderField], testCase.orderType); err != nil {
				f(fmt.Sprintf("%s failed: error converting %#v - %s", testName, prevDoc.(map[string]interface{})[testCase.orderField], err))
			}
			if cv, err = reddo.Convert(doc[testCase.orderField], testCase.orderType); err != nil {
				f(fmt.Sprintf("%s failed: error converting %#v - %s", testName, doc[testCase.orderField], err))
			}
			switch testCase.orderType {
			case reddo.TypeInt:
				if (odir == "DESC" && pv.(int64) < cv.(int64)) || (odir != "DESC" && pv.(int64) > cv.(int64)) {
					f(fmt.Sprintf("%s failed: out of order {doc: %#v, value: %#v} -> {doc: %#v, value: %#v}", testName, prevDoc, pv, doc, cv))
				}
			case reddo.TypeFloat:
				if (odir == "DESC" && pv.(float64) < cv.(float64)) || (odir != "DESC" && pv.(float64) > cv.(float64)) {
					f(fmt.Sprintf("%s failed: out of order {doc: %#v, value: %#v} -> {doc: %#v, value: %#v}", testName, prevDoc, pv, doc, cv))
				}
			case reddo.TypeString:
				if (odir == "DESC" && pv.(string) < cv.(string)) || (odir != "DESC" && pv.(string) > cv.(string)) {
					f(fmt.Sprintf("%s failed: out of order {doc: %#v, value: %#v} -> {doc: %#v, value: %#v}", testName, prevDoc, pv, doc, cv))
				}
			default:
				f(fmt.Sprintf("%s failed: cannot compare values of type %#v", testName, testCase.orderType))
			}
		}
		prevDoc = doc
	}
}

func _verifyOrderByRespQueryDocs(f funcTestFatal, testName string, testCase queryTestCase, queryResult *gocosmos.RespQueryDocs) {
	for i, doc := range queryResult.Documents {
		if doc == nil {
			f(fmt.Sprintf("%s failed: document at %d-th is nil", testName, i))
		}
	}
	docList := queryResult.Documents
	odir := strings.ToUpper(testCase.orderDirection)
	var prevDoc interface{}
	for _, doc := range docList {
		if prevDoc != nil {
			var pv, cv interface{}
			var err error
			if testCase.distinctQuery > 0 {
				if pv, err = reddo.Convert(prevDoc, testCase.orderType); err != nil {
					f(fmt.Sprintf("%s failed: error converting %#v - %s", testName, prevDoc, err))
				}
				if cv, err = reddo.Convert(doc, testCase.orderType); err != nil {
					f(fmt.Sprintf("%s failed: error converting %#v - %s", testName, doc, err))
				}
			} else {
				if pv, err = reddo.Convert(prevDoc.(map[string]interface{})[testCase.orderField], testCase.orderType); err != nil {
					f(fmt.Sprintf("%s failed: error converting %#v - %s", testName, prevDoc.(map[string]interface{})[testCase.orderField], err))
				}
				if cv, err = reddo.Convert(doc.(map[string]interface{})[testCase.orderField], testCase.orderType); err != nil {
					f(fmt.Sprintf("%s failed: error converting %#v - %s", testName, doc.(map[string]interface{})[testCase.orderField], err))
				}
			}
			switch testCase.orderType {
			case reddo.TypeInt:
				if (odir == "DESC" && pv.(int64) < cv.(int64)) || (odir != "DESC" && pv.(int64) > cv.(int64)) {
					f(fmt.Sprintf("%s failed: out of order {doc: %#v, value: %#v} -> {doc: %#v, value: %#v}", testName, prevDoc, pv, doc, cv))
				}
			case reddo.TypeFloat:
				if (odir == "DESC" && pv.(float64) < cv.(float64)) || (odir != "DESC" && pv.(float64) > cv.(float64)) {
					f(fmt.Sprintf("%s failed: out of order {doc: %#v, value: %#v} -> {doc: %#v, value: %#v}", testName, prevDoc, pv, doc, cv))
				}
			case reddo.TypeString:
				if (odir == "DESC" && pv.(string) < cv.(string)) || (odir != "DESC" && pv.(string) > cv.(string)) {
					f(fmt.Sprintf("%s failed: out of order {doc: %#v, value: %#v} -> {doc: %#v, value: %#v}", testName, prevDoc, pv, doc, cv))
				}
			default:
				f(fmt.Sprintf("%s failed: cannot compare values of type %#v", testName, testCase.orderType))
			}
		}
		prevDoc = doc
	}
}

func _verifyGroupBy(f funcTestFatal, testName string, testCase queryTestCase, partition, lowStr, highStr string, queryResult interface{}) {
	if testCase.groupByAggr == "" {
		return
	}
	switch queryResult.(type) {
	case *gocosmos.RespQueryDocs:
		_verifyGroupByRespQueryDocs(f, testName, testCase, partition, lowStr, highStr, queryResult.(*gocosmos.RespQueryDocs))
	case []map[string]interface{}:
		_verifyGroupByDriverSelect(f, testName, testCase, partition, lowStr, highStr, queryResult.([]map[string]interface{}))
	}
}

func _verifyGroupByDriverSelect(f funcTestFatal, testName string, testCase queryTestCase, partition, lowStr, highStr string, rows []map[string]interface{}) {
	for i, row := range rows {
		if row == nil {
			f(fmt.Sprintf("%s failed: row at %d-th is nil", testName, i))
		}
	}
	countPerCat, sumPerCat := make(map[int]int), make(map[int]int)
	minPerCat, maxPerCat := make(map[int]int), make(map[int]int)
	countPerPartitionPerCat, sumPerPartitionPerCat := make(map[string]map[int]int), make(map[string]map[int]int)
	minPerPartitionPerCat, maxPerPartitionPerCat := make(map[string]map[int]int), make(map[string]map[int]int)
	for i := 0; i < numLogicalPartitions; i++ {
		countPerPartitionPerCat["user"+strconv.Itoa(i)] = make(map[int]int)
		sumPerPartitionPerCat["user"+strconv.Itoa(i)] = make(map[int]int)
		minPerPartitionPerCat["user"+strconv.Itoa(i)] = make(map[int]int)
		maxPerPartitionPerCat["user"+strconv.Itoa(i)] = make(map[int]int)
	}
	for _, docInfo := range dataList {
		if lowStr <= docInfo.Id() && docInfo.Id() < highStr {
			username := docInfo.GetAttrAsTypeUnsafe("username", reddo.TypeString).(string)
			category := docInfo.GetAttrAsTypeUnsafe("category", reddo.TypeInt).(int64)
			grade := docInfo.GetAttrAsTypeUnsafe("grade", reddo.TypeInt).(int64)

			countPerCat[int(category)]++
			sumPerCat[int(category)] += int(grade)
			if minPerCat[int(category)] == 0 || minPerCat[int(category)] > int(grade) {
				minPerCat[int(category)] = int(grade)
			}
			if maxPerCat[int(category)] < int(grade) {
				maxPerCat[int(category)] = int(grade)
			}

			countPerPartitionPerCat[username][int(category)]++
			sumPerPartitionPerCat[username][int(category)] += int(grade)
			if minPerPartitionPerCat[username][int(category)] == 0 || minPerPartitionPerCat[username][int(category)] > int(grade) {
				minPerPartitionPerCat[username][int(category)] = int(grade)
			}
			if maxPerPartitionPerCat[username][int(category)] < int(grade) {
				maxPerPartitionPerCat[username][int(category)] = int(grade)
			}
		}
	}

	for _, row := range rows {
		category, _ := reddo.ToInt(row["Category"])
		value, _ := reddo.ToInt(row["Value"])
		var expected int
		switch strings.ToUpper(testCase.groupByAggr) {
		case "COUNT":
			expected = countPerCat[int(category)]
			if partition != "" {
				expected = countPerPartitionPerCat[partition][int(category)]
			}
		case "SUM":
			expected = sumPerCat[int(category)]
			if partition != "" {
				expected = sumPerPartitionPerCat[partition][int(category)]
			}
		case "MIN":
			expected = minPerCat[int(category)]
			if partition != "" {
				expected = minPerPartitionPerCat[partition][int(category)]
			}
		case "MAX":
			expected = maxPerCat[int(category)]
			if partition != "" {
				expected = maxPerPartitionPerCat[partition][int(category)]
			}
		case "AVG", "AVERAGE":
			expected = sumPerCat[int(category)] / countPerCat[int(category)]
			if partition != "" {
				expected = sumPerPartitionPerCat[partition][int(category)] / countPerPartitionPerCat[partition][int(category)]
			}
		default:
			f(fmt.Sprintf("%s failed: <group-by aggregation %#v> expected %#v but received  %#v", testName, testCase.groupByAggr, expected, value))
		}
		if int(value) != expected {
			f(fmt.Sprintf("%s failed: <group-by aggregation %#v> expected %#v but received  %#v", testName, testCase.groupByAggr, expected, value))
		}
	}
}

func _verifyGroupByRespQueryDocs(f funcTestFatal, testName string, testCase queryTestCase, partition, lowStr, highStr string, queryResult *gocosmos.RespQueryDocs) {
	for i, doc := range queryResult.Documents {
		if doc == nil {
			f(fmt.Sprintf("%s failed: document at %d-th is nil", testName, i))
		}
	}
	countPerCat, sumPerCat := make(map[int]int), make(map[int]int)
	minPerCat, maxPerCat := make(map[int]int), make(map[int]int)
	countPerPartitionPerCat, sumPerPartitionPerCat := make(map[string]map[int]int), make(map[string]map[int]int)
	minPerPartitionPerCat, maxPerPartitionPerCat := make(map[string]map[int]int), make(map[string]map[int]int)
	for i := 0; i < numLogicalPartitions; i++ {
		countPerPartitionPerCat["user"+strconv.Itoa(i)] = make(map[int]int)
		sumPerPartitionPerCat["user"+strconv.Itoa(i)] = make(map[int]int)
		minPerPartitionPerCat["user"+strconv.Itoa(i)] = make(map[int]int)
		maxPerPartitionPerCat["user"+strconv.Itoa(i)] = make(map[int]int)
	}
	for _, docInfo := range dataList {
		if lowStr <= docInfo.Id() && docInfo.Id() < highStr {
			username := docInfo.GetAttrAsTypeUnsafe("username", reddo.TypeString).(string)
			category := docInfo.GetAttrAsTypeUnsafe("category", reddo.TypeInt).(int64)
			grade := docInfo.GetAttrAsTypeUnsafe("grade", reddo.TypeInt).(int64)

			countPerCat[int(category)]++
			sumPerCat[int(category)] += int(grade)
			if minPerCat[int(category)] == 0 || minPerCat[int(category)] > int(grade) {
				minPerCat[int(category)] = int(grade)
			}
			if maxPerCat[int(category)] < int(grade) {
				maxPerCat[int(category)] = int(grade)
			}

			countPerPartitionPerCat[username][int(category)]++
			sumPerPartitionPerCat[username][int(category)] += int(grade)
			if minPerPartitionPerCat[username][int(category)] == 0 || minPerPartitionPerCat[username][int(category)] > int(grade) {
				minPerPartitionPerCat[username][int(category)] = int(grade)
			}
			if maxPerPartitionPerCat[username][int(category)] < int(grade) {
				maxPerPartitionPerCat[username][int(category)] = int(grade)
			}
		}
	}

	docList := queryResult.Documents.AsDocInfoSlice()
	if len(docList) == 0 {
		f(fmt.Sprintf("%s failed: empty/invalid query result", testName))
	}
	for _, doc := range docList {
		category := doc.GetAttrAsTypeUnsafe("Category", reddo.TypeInt).(int64)
		value := doc.GetAttrAsTypeUnsafe("Value", reddo.TypeInt).(int64)
		var expected int
		switch strings.ToUpper(testCase.groupByAggr) {
		case "COUNT":
			expected = countPerCat[int(category)]
			if partition != "" {
				expected = countPerPartitionPerCat[partition][int(category)]
			}
		case "SUM":
			expected = sumPerCat[int(category)]
			if partition != "" {
				expected = sumPerPartitionPerCat[partition][int(category)]
			}
		case "MIN":
			expected = minPerCat[int(category)]
			if partition != "" {
				expected = minPerPartitionPerCat[partition][int(category)]
			}
		case "MAX":
			expected = maxPerCat[int(category)]
			if partition != "" {
				expected = maxPerPartitionPerCat[partition][int(category)]
			}
		case "AVG", "AVERAGE":
			expected = sumPerCat[int(category)] / countPerCat[int(category)]
			if partition != "" {
				expected = sumPerPartitionPerCat[partition][int(category)] / countPerPartitionPerCat[partition][int(category)]
			}
		default:
			f(fmt.Sprintf("%s failed: <group-by aggregation %#v> expected %#v but received  %#v", testName, testCase.groupByAggr, expected, value))
		}
		if int(value) != expected {
			f(fmt.Sprintf("%s failed: <group-by aggregation %#v> expected %#v but received  %#v", testName, testCase.groupByAggr, expected, value))
		}
	}
}

func _countPerPartition(low, high int, dataList []gocosmos.DocInfo) map[string]int {
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	result := make(map[string]int)
	for _, docInfo := range dataList {
		if lowStr <= docInfo.Id() && docInfo.Id() < highStr {
			username := docInfo.GetAttrAsTypeUnsafe("username", reddo.TypeString).(string)
			result[username]++
		}
	}
	return result
}

func _distinctPerPartition(low, high int, dataList []gocosmos.DocInfo, distinctField string) map[string]int {
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	distinctItemsPerPartition := make(map[string]map[string]bool)
	for _, docInfo := range dataList {
		if lowStr <= docInfo.Id() && docInfo.Id() < highStr {
			username := docInfo.GetAttrAsTypeUnsafe("username", reddo.TypeString).(string)
			partitionItems, ok := distinctItemsPerPartition[username]
			if !ok {
				partitionItems = make(map[string]bool)
				distinctItemsPerPartition[username] = partitionItems
			}
			value := docInfo.GetAttrAsTypeUnsafe(distinctField, reddo.TypeString).(string)
			partitionItems[value] = true
		}
	}
	result := make(map[string]int)
	for p := range distinctItemsPerPartition {
		result[p] = len(distinctItemsPerPartition[p])
	}
	return result
}

/*
- Simple queries, with or without ORDER BY, (including No-limit/MaxItemCount/OFFSET...LIMIT) should work.
- SELECT DISTINCT/VALUE, with or without ORDER BY, queries (including No-limit/MaxItemCount/OFFSET...LIMIT) should work.
- (*) GROUP BY combined with ORDER BY is not supported!
- Simple GROUP BY queries (including No-limit/MaxItemCount/OFFSET...LIMIT) should work.
*/
func _testRestClientQueryDocumentsPkValue(t *testing.T, testName string, client *gocosmos.RestClient, dbname, collname string) {
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	countPerPartition := _countPerPartition(low, high, dataList)
	distinctPerPartition := _distinctPerPartition(low, high, dataList, "category")
	var testCases = []queryTestCase{
		{name: "NoLimit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high"},
		{name: "MaxItemCount_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high", maxItemCount: 7},
		{name: "OffsetLimit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high OFFSET 3 LIMIT 5", expectedNumItems: 5},
		{name: "OffsetLimit_MaxItemCount_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high OFFSET 3 LIMIT 10", maxItemCount: 7},

		{name: "NoLimit_OrderAsc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade", orderType: reddo.TypeInt, orderField: "grade", orderDirection: "asc"},
		{name: "MaxItemCount_OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC", maxItemCount: 11, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc"},
		{name: "OffsetLimit_OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category DESC OFFSET 3 LIMIT 5", expectedNumItems: 5, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc"},
		{name: "OffsetLimit_MaxItemCount_OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC OFFSET 1 LIMIT 15", maxItemCount: 7, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc"},

		{name: "NoLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high", distinctQuery: 1},
		{name: "NoLimit_DistinctDoc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high", distinctQuery: -1},
		{name: "MaxItemCount_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high", distinctQuery: 1, maxItemCount: numCategories/2 + 1},
		{name: "MaxItemCount_DistinctDoc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high", distinctQuery: -1, maxItemCount: numCategories/2 + 1},
		{name: "OffsetLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high OFFSET 1 LIMIT 3", distinctQuery: 1, expectedNumItems: 3},
		{name: "OffsetLimit_DistinctDoc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high OFFSET 1 LIMIT 3", distinctQuery: -1, expectedNumItems: 3},
		{name: "OffsetLimit_MaxItemCount_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high OFFSET 1 LIMIT 10", distinctQuery: 1, maxItemCount: 3},
		{name: "OffsetLimit_MaxItemCount_DistinctDoc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high OFFSET 1 LIMIT 10", distinctQuery: -1, maxItemCount: 3},

		{name: "NoLimit_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc"},
		{name: "NoLimit_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category DESC", distinctQuery: -1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc"},
		{name: "MaxItemCount_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", maxItemCount: numCategories/2 + 1},
		{name: "MaxItemCount_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category DESC", distinctQuery: -1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc", maxItemCount: numCategories/2 + 1},
		{name: "OffsetLimit_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category OFFSET 1 LIMIT 3", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", expectedNumItems: 3},
		{name: "OffsetLimit_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category DESC OFFSET 1 LIMIT 3", distinctQuery: -1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc", expectedNumItems: 3},
		{name: "OffsetLimit_MaxItemCount_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category OFFSET 1 LIMIT 10", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", maxItemCount: 5},
		{name: "OffsetLimit_MaxItemCount_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category DESC OFFSET 1 LIMIT 10", distinctQuery: -1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc", maxItemCount: 5},

		/* GROUP BY with ORDER BY is not supported! */

		{name: "NoLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", groupByAggr: "count"},
		{name: "MaxItemCount_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: 5, groupByAggr: "count"},
		{name: "OffsetLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 3", expectedNumItems: 3, groupByAggr: "count"},
		{name: "OffsetLimit_MaxItemCount_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 10", maxItemCount: 7, groupByAggr: "count"},

		{name: "NoLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", groupByAggr: "sum"},
		{name: "MaxItemCount_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: 5, groupByAggr: "sum"},
		{name: "OffsetLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 3", expectedNumItems: 3, groupByAggr: "sum"},
		{name: "OffsetLimit_MaxItemCount_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 10", maxItemCount: 7, groupByAggr: "sum"},

		{name: "NoLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", groupByAggr: "min"},
		{name: "MaxItemCount_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: 5, groupByAggr: "min"},
		{name: "OffsetLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 3", expectedNumItems: 3, groupByAggr: "min"},
		{name: "OffsetLimit_MaxItemCount_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 10", maxItemCount: 7, groupByAggr: "min"},

		{name: "NoLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", groupByAggr: "max"},
		{name: "MaxItemCount_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: 5, groupByAggr: "max"},
		{name: "OffsetLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 3", expectedNumItems: 3, groupByAggr: "max"},
		{name: "OffsetLimit_MaxItemCount_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 10", maxItemCount: 7, groupByAggr: "max"},

		{name: "NoLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", groupByAggr: "average"},
		{name: "MaxItemCount_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: 5, groupByAggr: "average"},
		{name: "OffsetLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 3", expectedNumItems: 3, groupByAggr: "average"},
		{name: "OffsetLimit_MaxItemCount_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 10", maxItemCount: 7, groupByAggr: "average"},
	}
	params := []interface{}{map[string]interface{}{"name": "@low", "value": lowStr}, map[string]interface{}{"name": "@high", "value": highStr}}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query := gocosmos.QueryReq{DbName: dbname, CollName: collname, Query: testCase.query, MaxItemCount: -1, Params: params}
			if testCase.maxItemCount > 0 {
				query.MaxItemCount = testCase.maxItemCount
			}
			savedExpectedNumItems := testCase.expectedNumItems
			for i := 0; i < numLogicalPartitions; i++ {
				testCase.expectedNumItems = savedExpectedNumItems
				expectedNumItems := testCase.expectedNumItems
				username := "user" + strconv.Itoa(i)
				query.PkValue = username
				if expectedNumItems <= 0 && testCase.maxItemCount <= 0 {
					expectedNumItems = countPerPartition[username]
					if testCase.distinctQuery != 0 {
						expectedNumItems = distinctPerPartition[username]
					}
					testCase.expectedNumItems = expectedNumItems
				}

				result := client.QueryDocuments(query)
				_verifyResult(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pk="+username, testCase, expectedNumItems, result)
				_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pk="+username, testCase, result)
				_verifyOrderBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pk="+username, testCase, result)
				_verifyGroupBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pk="+username, testCase, username, lowStr, highStr, result)
			}
		})
	}
}

func TestRestClient_QueryDocuments_PkValue_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_PkValue_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsPkValue(t, testName, client, dbname, collname)
}

func TestRestClient_QueryDocuments_PkValue_LargeRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_PkValue_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsPkValue(t, testName, client, dbname, collname)
}

/*
- Simple queries, with or without ORDER BY, (including No-limit/MaxItemCount/OFFSET...LIMIT) should work.
- (!) We dont test OFFSET...LIMIT combined with MaxItemCount.
- SELECT DISTINCT/VALUE, with or without ORDER BY, queries (including No-limit/MaxItemCount/OFFSET...LIMIT) should work.
- (!) We don't test GROUP BY queries against pkrangeid.
*/
func _testRestClientQueryDocumentsPkrangeid(t *testing.T, testName string, client *gocosmos.RestClient, dbname, collname string) {
	pkranges := client.GetPkranges(dbname, collname)
	if pkranges.Error() != nil {
		t.Fatalf("%s failed: %s", testName, pkranges.Error())
	}
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	var testCases = []queryTestCase{
		{name: "NoLimit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high"},
		{name: "MaxItemCount_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high", maxItemCount: 7},
		{name: "OffsetLimit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high OFFSET 3 LIMIT 5", expectedNumItems: 5},
		{name: "OffsetLimit_MaxItemCount_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high OFFSET 1 LIMIT 10", maxItemCount: 7},

		{name: "NoLimit_OrderAsc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade", orderType: reddo.TypeInt, orderField: "grade", orderDirection: "asc"},
		{name: "MaxItemCount_OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade DESC", maxItemCount: 11, orderType: reddo.TypeInt, orderField: "grade", orderDirection: "desc"},
		{name: "OffsetLimit_OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade DESC OFFSET 1 LIMIT 3", expectedNumItems: 3, orderType: reddo.TypeInt, orderField: "grade", orderDirection: "desc"},
		{name: "OffsetLimit_MaxItemCount_OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade DESC OFFSET 1 LIMIT 7", maxItemCount: 11, orderType: reddo.TypeInt, orderField: "grade", orderDirection: "desc"},

		{name: "NoLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c", distinctQuery: 1, expectedNumItems: numCategories},
		{name: "NoLimit_DistinctDoc", query: "SELECT DISTINCT c.category FROM c", distinctQuery: -1, expectedNumItems: numCategories},
		{name: "MaxItemCount_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c", distinctQuery: 1, maxItemCount: numCategories/2 + 1},
		{name: "MaxItemCount_DistinctDoc", query: "SELECT DISTINCT c.category FROM c", distinctQuery: -1, maxItemCount: numCategories/2 + 1},
		{name: "OffsetLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c OFFSET 1 LIMIT 3", distinctQuery: 1, expectedNumItems: 3},
		{name: "OffsetLimit_DistinctDoc", query: "SELECT DISTINCT c.category FROM c OFFSET 1 LIMIT 3", distinctQuery: -1, expectedNumItems: 3},
		{name: "OffsetLimit_MaxItemCount_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c OFFSET 1 LIMIT 10", distinctQuery: 1, maxItemCount: 3},
		{name: "OffsetLimit_MaxItemCount_DistinctDoc", query: "SELECT DISTINCT c.category FROM c OFFSET 1 LIMIT 10", distinctQuery: -1, maxItemCount: 3},

		{name: "NoLimit_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", expectedNumItems: numCategories},
		{name: "NoLimit_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category DESC", distinctQuery: -1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc", expectedNumItems: numCategories},
		{name: "MaxItemCount_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", maxItemCount: numCategories/2 + 1},
		{name: "MaxItemCount_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category DESC", distinctQuery: -1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc", maxItemCount: numCategories/2 + 1},
		{name: "OffsetLimit_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category OFFSET 1 LIMIT 3", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", expectedNumItems: 3},
		{name: "OffsetLimit_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category DESC OFFSET 1 LIMIT 3", distinctQuery: -1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc", expectedNumItems: 3},
		{name: "OffsetLimit_MaxItemCount_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category OFFSET 1 LIMIT 10", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", maxItemCount: 5},
		{name: "OffsetLimit_MaxItemCount_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category DESC OFFSET 1 LIMIT 10", distinctQuery: -1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc", maxItemCount: 5},

		/* GROUP BY are not tested */
	}
	params := []interface{}{map[string]interface{}{"name": "@low", "value": lowStr}, map[string]interface{}{"name": "@high", "value": highStr}}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query := gocosmos.QueryReq{DbName: dbname, CollName: collname, Query: testCase.query, MaxItemCount: -1, Params: params}
			if testCase.maxItemCount > 0 {
				query.MaxItemCount = testCase.maxItemCount
			}
			totalExpected := high - low
			if testCase.expectedNumItems > 0 {
				totalExpected = testCase.expectedNumItems * pkranges.Count
			}

			totalItems := 0
			for _, pkrange := range pkranges.Pkranges {
				query.PkRangeId = pkrange.Id
				result := client.QueryDocuments(query)
				if result.Error() != nil {
					t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/pkrangeid="+pkrange.Id, result.Error())
				}
				totalItems += result.Count
				// if result.Count == 0 || len(result.Documents) == 0 {
				// 	t.Fatalf("%s failed: <num-document> is zero", testName+"/"+testCase.name+"/pkrangeid="+pkrange.Id)
				// }
				if testCase.groupByAggr == "" {
					if testCase.maxItemCount > 0 && (len(result.Documents) > testCase.maxItemCount || result.Count > testCase.maxItemCount) {
						t.Fatalf("%s failed: <num-document> expected not exceeding %#v but received (len: %#v / count: %#v)", testName+"/"+testCase.name+"/pkrangeid="+pkrange.Id, testCase.maxItemCount, len(result.Documents), result.Count)
					}
					if testCase.maxItemCount <= 0 && testCase.expectedNumItems > 0 && (len(result.Documents) != testCase.expectedNumItems || result.Count != testCase.expectedNumItems) {
						t.Fatalf("%s failed: <num-document> expected %#v but received (len: %#v / count: %#v)", testName+"/"+testCase.name+"/pkrangeid="+pkrange.Id, testCase.expectedNumItems, len(result.Documents), result.Count)
					}
				}
				_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pkrangeid="+pkrange.Id, testCase, result)
				_verifyOrderBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name+"/pkrangeid="+pkrange.Id, testCase, result)
			}
			if testCase.groupByAggr == "" && testCase.maxItemCount <= 0 && totalItems != totalExpected {
				t.Fatalf("%s failed: <total-num-document> expected %#v but received  %#v", testName+"/"+testCase.name, totalExpected, totalItems)
			}
		})
	}
}

func TestRestClient_QueryDocuments_Pkrangeid_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_Pkrangeid_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsPkrangeid(t, testName, client, dbname, collname)
}

func TestRestClient_QueryDocuments_Pkrangeid_LargeRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_Pkrangeid_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsPkrangeid(t, testName, client, dbname, collname)
}

/*
- Simple queries, with or without ORDER BY, (including No-limit/MaxItemCount/OFFSET...LIMIT) should work.
- (*) OFFSET...LIMIT combined with MaxItemCount would not work!
- SELECT DISTINCT/VALUE, with or without ORDER BY, queries (including No-limit/MaxItemCount/OFFSET...LIMIT) should work.
- (*) GROUP BY combined with ORDER BY is not supported!
- SELECT count/sum/min/max/avg...GROUP BY queries (including No-limit/OFFSET...LIMIT) should work.
- (*) GROUP BY combined with MaxItemCount would not work!
*/
func _testRestClientQueryDocumentsCrossPartitions(t *testing.T, testName string, client *gocosmos.RestClient, dbname, collname string) {
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	var testCases = []queryTestCase{
		{name: "NoLimit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high"},
		{name: "MaxItemCount_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high", maxItemCount: 7},
		{name: "OffsetLimit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high OFFSET 3 LIMIT 5", expectedNumItems: 5},
		{name: "OffsetLimit_MaxItemCount_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high OFFSET 1 LIMIT 10", maxItemCount: 5},

		{name: "NoLimit_GradeOrderAsc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade", orderType: reddo.TypeInt, orderField: "grade", orderDirection: "asc"},
		{name: "MaxItemCount_GradeOrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade DESC", maxItemCount: 7, orderType: reddo.TypeInt, orderField: "grade", orderDirection: "desc"},
		{name: "OffsetLimit_GradeOrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade DESC OFFSET 3 LIMIT 5", expectedNumItems: 5, orderType: reddo.TypeInt, orderField: "grade", orderDirection: "desc"},
		{name: "OffsetLimit_MaxItemCount_GradeOrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade DESC OFFSET 1 LIMIT 10", maxItemCount: 5, orderType: reddo.TypeInt, orderField: "grade", orderDirection: "desc"},

		{name: "NoLimit_OrderAscUsername", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username", orderType: reddo.TypeString, orderField: "username", orderDirection: "asc"},
		{name: "MaxItemCount_OrderDescUsername", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC", maxItemCount: 7, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc"},
		{name: "OffsetLimit_OrderDescUsername", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC OFFSET 3 LIMIT 5", expectedNumItems: 5, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc"},
		{name: "OffsetLimit_MaxItemCount_OrderDescUsername", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC OFFSET 1 LIMIT 10", maxItemCount: 5, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc"},

		{name: "NoLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c", distinctQuery: 1, expectedNumItems: numCategories},
		{name: "NoLimit_DistinctDoc", query: "SELECT DISTINCT c.username FROM c", distinctQuery: -1, expectedNumItems: numLogicalPartitions},
		{name: "MaxItemCount_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c", distinctQuery: 1, maxItemCount: numCategories/2 + 1},
		{name: "MaxItemCount_DistinctDoc", query: "SELECT DISTINCT c.username FROM c", distinctQuery: -1, maxItemCount: numLogicalPartitions/2 + 1},
		{name: "OffsetLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c OFFSET 3 LIMIT 7", distinctQuery: 1, expectedNumItems: 7},
		{name: "OffsetLimit_DistinctDoc", query: "SELECT DISTINCT c.username FROM c OFFSET 3 LIMIT 7", distinctQuery: -1, expectedNumItems: 7},
		{name: "OffsetLimit_MaxItemCount_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c OFFSET 1 LIMIT 10", distinctQuery: 1, maxItemCount: numCategories/2 + 1},
		{name: "OffsetLimit_MaxItemCount_DistinctDoc", query: "SELECT DISTINCT c.username FROM c OFFSET 1 LIMIT 10", distinctQuery: -1, maxItemCount: numLogicalPartitions/2 + 1},

		{name: "NoLimit_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.username FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username", distinctQuery: 1, orderType: reddo.TypeString, orderField: "username", orderDirection: "asc", expectedNumItems: numLogicalPartitions},
		{name: "NoLimit_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category DESC", distinctQuery: -1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc", expectedNumItems: numCategories},
		{name: "MaxItemCount_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", maxItemCount: numCategories/2 + 1},
		{name: "MaxItemCount_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.username FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC", distinctQuery: -1, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc", maxItemCount: numLogicalPartitions/2 + 1},
		{name: "OffsetLimit_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category OFFSET 3 LIMIT 7", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", expectedNumItems: 7},
		{name: "OffsetLimit_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.username FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC OFFSET 3 LIMIT 7", distinctQuery: -1, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc", expectedNumItems: 7},
		{name: "OffsetLimit_MaxItemCount_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category OFFSET 1 LIMIT 10", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", maxItemCount: 5},
		{name: "OffsetLimit_MaxItemCount_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.username FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC OFFSET 1 LIMIT 10", distinctQuery: -1, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc", maxItemCount: 5},

		/* GROUP BY with ORDER BY is not supported! */

		{name: "NoLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, groupByAggr: "count"},
		{name: "OffsetLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 3 LIMIT 5", expectedNumItems: 5, groupByAggr: "count"},
		{name: "NoLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, groupByAggr: "sum"},
		{name: "OffsetLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 3 LIMIT 5", expectedNumItems: 5, groupByAggr: "sum"},
		{name: "NoLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, groupByAggr: "min"},
		{name: "OffsetLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 3 LIMIT 5", expectedNumItems: 5, groupByAggr: "min"},
		{name: "NoLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, groupByAggr: "max"},
		{name: "OffsetLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 3 LIMIT 5", expectedNumItems: 5, groupByAggr: "max"},
		{name: "NoLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, groupByAggr: "average"},
		{name: "OffsetLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 3 LIMIT 5", expectedNumItems: 5, groupByAggr: "average"},

		/* GROUP BY combined with MaxItemCount would not work! */
	}
	params := []interface{}{map[string]interface{}{"name": "@low", "value": lowStr}, map[string]interface{}{"name": "@high", "value": highStr}}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query := gocosmos.QueryReq{DbName: dbname, CollName: collname, Query: testCase.query, MaxItemCount: -1, CrossPartitionEnabled: true, Params: params}
			expectedNumItems := high - low
			if testCase.expectedNumItems > 0 {
				expectedNumItems = testCase.expectedNumItems
			}
			if testCase.maxItemCount > 0 {
				query.MaxItemCount = testCase.maxItemCount
				expectedNumItems = 0
			}

			result := client.QueryDocuments(query)
			_verifyResult(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, expectedNumItems, result)
			_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, result)
			_verifyOrderBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, result)
			_verifyGroupBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, "", lowStr, highStr, result)
		})
	}
}

func TestRestClient_QueryDocuments_CrossPartitions_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_CrossPartitions_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsCrossPartitions(t, testName, client, dbname, collname)
}

func TestRestClient_QueryDocuments_CrossPartitions_LargeRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_CrossPartitions_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsCrossPartitions(t, testName, client, dbname, collname)
}

/*
- Simple queries, with or without ORDER BY, (including No-limit/MaxItemCount/OFFSET...LIMIT) should work.
- SELECT DISTINCT/VALUE, with or without ORDER BY, queries (including No-limit/MaxItemCount/OFFSET...LIMIT) should work.
- (*) GROUP BY combined with ORDER BY is not supported!
- Simple SELECT count/sum/min/max/avg with GROUP BY queries (including No-limit/MaxItemCount/OFFSET...LIMIT) should work.
*/
func _testRestClientQueryDocumentsCrossPartition(t *testing.T, testName string, client *gocosmos.RestClient, dbname, collname string) {
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)
	var testCases = []queryTestCase{
		{name: "NoLimit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high"},
		{name: "MaxItemCount_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high", maxItemCount: 7},
		{name: "OffsetLimit_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high OFFSET 3 LIMIT 5", expectedNumItems: 5},
		{name: "OffsetLimit_MaxItemCount_Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high OFFSET 12 LIMIT 345", expectedNumItems: 345, maxItemCount: 7},

		{name: "NoLimit_OrderAsc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade", orderType: reddo.TypeInt, orderField: "grade", orderDirection: "asc"},
		{name: "MaxItemCount_OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC", maxItemCount: 7, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc"},
		{name: "OffsetLimit_OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category DESC OFFSET 3 LIMIT 5", expectedNumItems: 5, orderType: reddo.TypeInt, orderField: "category", orderDirection: "desc"},
		{name: "OffsetLimit_MaxItemCount_OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC OFFSET 12 LIMIT 345", expectedNumItems: 345, maxItemCount: 7, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc"},

		{name: "NoLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c", distinctQuery: 1, expectedNumItems: numCategories},
		{name: "NoLimit_DistinctDoc", query: "SELECT DISTINCT c.username FROM c", distinctQuery: -1, expectedNumItems: numLogicalPartitions},
		{name: "MaxItemCount_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c", distinctQuery: 1, expectedNumItems: numCategories, maxItemCount: numCategories/2 + 1},
		{name: "MaxItemCount_DistinctDoc", query: "SELECT DISTINCT c.username FROM c", distinctQuery: -1, expectedNumItems: numLogicalPartitions, maxItemCount: numLogicalPartitions/2 + 1},
		{name: "OffsetLimit_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c OFFSET 3 LIMIT 7", distinctQuery: 1, expectedNumItems: 7},
		{name: "OffsetLimit_DistinctDoc", query: "SELECT DISTINCT c.username FROM c OFFSET 3 LIMIT 7", distinctQuery: -1, expectedNumItems: 7},
		{name: "OffsetLimit_MaxItemCount_DistinctValue", query: "SELECT DISTINCT VALUE c.category FROM c OFFSET 1 LIMIT 10", distinctQuery: 1, expectedNumItems: 10, maxItemCount: numCategories/2 + 1},
		{name: "OffsetLimit_MaxItemCount_DistinctDoc", query: "SELECT DISTINCT c.username FROM c OFFSET 1 LIMIT 10", distinctQuery: -1, expectedNumItems: 10, maxItemCount: numLogicalPartitions/2 + 1},

		{name: "NoLimit_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", expectedNumItems: numCategories},
		{name: "NoLimit_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.username FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC", distinctQuery: -1, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc", expectedNumItems: numLogicalPartitions},
		{name: "MaxItemCount_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", expectedNumItems: numCategories, maxItemCount: numCategories/2 + 1},
		{name: "MaxItemCount_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.username FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC", distinctQuery: -1, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc", expectedNumItems: numLogicalPartitions, maxItemCount: numLogicalPartitions/2 + 1},
		{name: "OffsetLimit_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category OFFSET 3 LIMIT 7", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", expectedNumItems: 7},
		{name: "OffsetLimit_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.username FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC OFFSET 3 LIMIT 7", distinctQuery: -1, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc", expectedNumItems: 7},
		{name: "OffsetLimit_MaxItemCount_DistinctValue_OrderAsc", query: "SELECT DISTINCT VALUE c.category FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.category OFFSET 1 LIMIT 10", distinctQuery: 1, orderType: reddo.TypeInt, orderField: "category", orderDirection: "asc", expectedNumItems: 10, maxItemCount: numCategories/2 + 1},
		{name: "OffsetLimit_MaxItemCount_DistinctDoc_OrderDesc", query: "SELECT DISTINCT c.username FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.username DESC OFFSET 1 LIMIT 10", distinctQuery: -1, orderType: reddo.TypeString, orderField: "username", orderDirection: "desc", expectedNumItems: 10, maxItemCount: numLogicalPartitions/2 + 1},

		/* GROUP BY with ORDER BY is not supported! */

		{name: "NoLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, groupByAggr: "count"},
		{name: "MaxItemCount_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, maxItemCount: numCategories/2 + 1, groupByAggr: "count"},
		{name: "OffsetLimit_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 3 LIMIT 5", expectedNumItems: 5, groupByAggr: "count"},
		{name: "OffsetLimit_MaxItemCount_GroupByCount", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 10", expectedNumItems: numCategories, maxItemCount: 3, groupByAggr: "count"},

		{name: "NoLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, groupByAggr: "sum"},
		{name: "MaxItemCount_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, maxItemCount: numCategories/2 + 1, groupByAggr: "sum"},
		{name: "OffsetLimit_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 3 LIMIT 5", expectedNumItems: 5, groupByAggr: "sum"},
		{name: "OffsetLimit_MaxItemCount_GroupBySum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 10", expectedNumItems: numCategories, maxItemCount: 3, groupByAggr: "sum"},

		{name: "NoLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, groupByAggr: "min"},
		{name: "MaxItemCount_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, maxItemCount: numCategories/2 + 1, groupByAggr: "min"},
		{name: "OffsetLimit_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 3 LIMIT 5", expectedNumItems: 5, groupByAggr: "min"},
		{name: "OffsetLimit_MaxItemCount_GroupByMin", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 10", expectedNumItems: numCategories, maxItemCount: 3, groupByAggr: "min"},

		{name: "NoLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, groupByAggr: "max"},
		{name: "MaxItemCount_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, maxItemCount: numCategories/2 + 1, groupByAggr: "max"},
		{name: "OffsetLimit_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 3 LIMIT 5", expectedNumItems: 5, groupByAggr: "max"},
		{name: "OffsetLimit_MaxItemCount_GroupByMax", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 10", expectedNumItems: numCategories, maxItemCount: 3, groupByAggr: "max"},

		{name: "NoLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, groupByAggr: "average"},
		{name: "MaxItemCount_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", expectedNumItems: numCategories, maxItemCount: numCategories/2 + 1, groupByAggr: "average"},
		{name: "OffsetLimit_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 3 LIMIT 5", expectedNumItems: 5, groupByAggr: "average"},
		{name: "OffsetLimit_MaxItemCount_GroupByAvg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category OFFSET 1 LIMIT 10", expectedNumItems: numCategories, maxItemCount: 3, groupByAggr: "avg"},
	}
	params := []interface{}{map[string]interface{}{"name": "@low", "value": lowStr}, map[string]interface{}{"name": "@high", "value": highStr}}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query := gocosmos.QueryReq{DbName: dbname, CollName: collname, Query: testCase.query, MaxItemCount: -1, CrossPartitionEnabled: true, Params: params}
			if testCase.maxItemCount > 0 {
				query.MaxItemCount = testCase.maxItemCount
			}
			expectedNumItems := high - low
			if testCase.expectedNumItems > 0 {
				expectedNumItems = testCase.expectedNumItems
			}

			result := client.QueryDocumentsCrossPartition(query)
			testCase.maxItemCount = -1
			_verifyResult(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, expectedNumItems, result)
			_verifyDistinct(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, result)
			_verifyOrderBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, result)
			_verifyGroupBy(func(msg string) { t.Fatal(msg) }, testName+"/"+testCase.name, testCase, "", lowStr, highStr, result)
		})
	}
}

func TestRestClient_QueryDocumentsCrossPartition_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryDocumentsCrossPartition_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsCrossPartition(t, testName, client, dbname, collname)
}

func TestRestClient_QueryDocumentsCrossPartition_LargeRU(t *testing.T) {
	testName := "TestRestClient_QueryDocumentsCrossPartition_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsCrossPartition(t, testName, client, dbname, collname)
}

/*----------------------------------------------------------------------*/

/*
- (*) OFFSET...LIMIT combined with MaxItemCount would not work!
- (*) GROUP BY combined with ORDER BY is not supported!
- Simple queries (without ORDER BY), should work. (*) Cross-partitions queries that combine `ORDER BY` with QueryReq.MaxItemCount would not work!
- (-) SELECT DISTINCT/VALUE <non-pk-field> (without ORDER BY) combined with MaxItemCount would not work!
- (-) GROUP BY <non-pk-field> combined with MaxItemCount would not work! (*) only SELECT ...count(1) obeys MaxItemCount, other aggregate functions do not!
*/
func _testRestClientQueryDocumentsContinuation(t *testing.T, testName string, client *gocosmos.RestClient, dbname, collname string) {
	pkranges := client.GetPkranges(dbname, collname)
	if pkranges.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", pkranges.Error())
	}
	low, high := 123, 987
	lowStr, highStr := fmt.Sprintf("%05d", low), fmt.Sprintf("%05d", high)

	// only "bare" query is working with cross-partition continuation!
	// amongst GROUP BY queries, only count(x) obeys maxItemCount; count(c.field)/sum/min/max/avg do NOT!
	var testCases = []queryTestCase{
		/* OFFSET...LIMIT combined with MaxItemCount would not work! */

		/* GROUP BY combined with ORDER BY is not supported! */

		{name: "Bare", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high", maxItemCount: 7},

		/* ORDER BY combined with MaxItemCount would not work */
		{name: "*OrderAsc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade", maxItemCount: 7, orderType: reddo.TypeInt, orderField: "grade", orderDirection: "asc"},
		{name: "*OrderDesc", query: "SELECT * FROM c WHERE @low<=c.id AND c.id<@high ORDER BY c.grade DESC", maxItemCount: 7, orderType: reddo.TypeInt, orderField: "grade", orderDirection: "desc"},

		/* SELECT DISTINCT/VALUE <non-pk-field> combined with MaxItemCount would not work! */
		{name: "DistinctValue", query: "SELECT DISTINCT VALUE c.username FROM c", maxItemCount: 3, distinctQuery: 1, expectedNumItems: numLogicalPartitions},
		{name: "*DistinctDoc", query: "SELECT DISTINCT c.category FROM c", maxItemCount: 3, distinctQuery: -1, expectedNumItems: numCategories},
		{name: "*DistinctValue_OrderDesc", query: "SELECT DISTINCT VALUE c.username FROM c ORDER BY c.username DESC", maxItemCount: 3, distinctQuery: 1, expectedNumItems: numLogicalPartitions, orderField: "username", orderDirection: "desc"},
		{name: "*DistinctDoc_OrderAsc", query: "SELECT DISTINCT c.category FROM c ORDER BY c.category", maxItemCount: 3, distinctQuery: -1, expectedNumItems: numCategories},

		/* GROUP BY combined with MaxItemCount would not work */
		{name: "*GroupByCategory_Count", query: "SELECT c.category AS 'Category', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: numCategories/3 + 1, groupByAggr: "count"},
		{name: "GroupByUser_Count", query: "SELECT c.username AS 'Username', count(1) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.username", maxItemCount: numLogicalPartitions/3 + 1, groupByAggr: "count"},
		{name: "*GroupByCategory_Sum", query: "SELECT c.category AS 'Category', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: numCategories/3 + 1, groupByAggr: "sum"},
		{name: "GroupByUser_Sum", query: "SELECT c.username AS 'Username', sum(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.username", maxItemCount: numLogicalPartitions/3 + 1, groupByAggr: "sum"},
		{name: "*GroupByCategory_Min", query: "SELECT c.category AS 'Category', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: numCategories/3 + 1, groupByAggr: "min"},
		{name: "GroupByUser_Min", query: "SELECT c.username AS 'Username', min(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.username", maxItemCount: numLogicalPartitions/3 + 1, groupByAggr: "min"},
		{name: "*GroupByCategory_Max", query: "SELECT c.category AS 'Category', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: numCategories/3 + 1, groupByAggr: "max"},
		{name: "GroupByUser_Max", query: "SELECT c.username AS 'Username', max(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.username", maxItemCount: numLogicalPartitions/3 + 1, groupByAggr: "max"},
		{name: "*GroupByCategory_Avg", query: "SELECT c.category AS 'Category', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.category", maxItemCount: numCategories/3 + 1, groupByAggr: "average"},
		{name: "GroupByUser_Avg", query: "SELECT c.username AS 'Username', avg(c.grade) AS 'Value' FROM c WHERE @low<=c.id AND c.id<@high GROUP BY c.username", maxItemCount: numLogicalPartitions/3 + 1, groupByAggr: "average"},
	}
	params := []interface{}{map[string]interface{}{"name": "@low", "value": lowStr}, map[string]interface{}{"name": "@high", "value": highStr}}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			verifiedQuery := gocosmos.QueryReq{DbName: dbname, CollName: collname, Query: testCase.query, MaxItemCount: -1, CrossPartitionEnabled: true, Params: params}
			expectedResult := client.QueryDocuments(verifiedQuery)
			if expectedResult.Error() != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/Query", expectedResult.Error())
			}
			if expectedResult.Count < 1 {
				t.Fatalf("%s failed: empty result", testName+"/"+testCase.name+"/Query")
			}

			query := gocosmos.QueryReq{
				DbName: dbname, CollName: collname, Query: testCase.query, MaxItemCount: testCase.maxItemCount, CrossPartitionEnabled: true, Params: params,
			}
			var result *gocosmos.RespQueryDocs
			for {
				tempResult := client.QueryDocuments(query)
				if tempResult.Error() != nil {
					t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/Query", tempResult.Error())
				}
				ignoreMaxItemCountCheck := strings.HasSuffix(testCase.name, "_Count") ||
					strings.HasSuffix(testCase.name, "_Sum") ||
					strings.HasSuffix(testCase.name, "_Min") || strings.HasSuffix(testCase.name, "_Max") ||
					strings.HasSuffix(testCase.name, "_Avg")
				if !ignoreMaxItemCountCheck && (tempResult.Count > testCase.maxItemCount || len(tempResult.Documents) > testCase.maxItemCount) {
					t.Fatalf("%s failed: <num-document> expected not exceeding %#v but received (len: %#v / count: %#v)", testName+"/"+testCase.name, testCase.maxItemCount, len(tempResult.Documents), tempResult.Count)
				}
				if result == nil {
					result = tempResult
				} else {
					if strings.HasPrefix(testCase.name, "*") {
						result.RestResponse = tempResult.RestResponse
						if result.RewrittenDocuments == nil {
							result.Documents = result.Documents.Merge(tempResult.QueryPlan, tempResult.Documents)
						} else {
							result.RewrittenDocuments = result.RewrittenDocuments.Merge(tempResult.QueryPlan, tempResult.RewrittenDocuments)
							// if tempResult.QueryPlan.IsDistinctQuery() {
							// 	result.RewrittenDocuments = result.RewrittenDocuments.ReduceDistinct(tempResult.QueryPlan)
							// }
							result.Documents = result.RewrittenDocuments.Flatten(tempResult.QueryPlan)
						}
					} else {
						result.Documents = append(result.Documents, tempResult.Documents...)
					}
					result.ContinuationToken = tempResult.ContinuationToken
					result.Count = len(result.Documents)
				}
				query.ContinuationToken = result.ContinuationToken
				if result.ContinuationToken == "" {
					break
				}
			}
			if expectedResult.Count != result.Count || len(expectedResult.Documents) != len(result.Documents) {
				t.Fatalf("%s failed: <num-document> expected (%#v-%#v) but received (%#v-%#v)", testName+"/"+testCase.name+"/Query", expectedResult.Count, len(expectedResult.Documents), result.Count, len(result.Documents))
			}
			if testCase.orderField == "" {
				sort.Slice(expectedResult.Documents, func(i, j int) bool {
					jsi, _ := json.Marshal(expectedResult.Documents[i])
					jsj, _ := json.Marshal(expectedResult.Documents[j])
					return string(jsi) < string(jsj)
				})
				sort.Slice(result.Documents, func(i, j int) bool {
					jsi, _ := json.Marshal(result.Documents[i])
					jsj, _ := json.Marshal(result.Documents[j])
					return string(jsi) < string(jsj)
				})
			}
			for i := range expectedResult.Documents {
				if !reflect.DeepEqual(expectedResult.Documents[i], result.Documents[i]) {
					t.Fatalf("%s failed: %#v-th position expected\n%#v\nbut received\n%#v", testName+"/"+testCase.name+"/Query", i, expectedResult.Documents[i], result.Documents[i])
				}
			}
		})
	}
}

func TestRestClient_QueryDocuments_Continuation_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_Continuation_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataSmallRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsContinuation(t, testName, client, dbname, collname)
}

func TestRestClient_QueryDocuments_Continuation_LargeRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_Continuation_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataLargeRU(t, testName, client, dbname, collname, 1000)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsContinuation(t, testName, client, dbname, collname)
}

/*----------------------------------------------------------------------*/

type customQueryTestCase struct {
	name, query        string
	expectedResultJson string
	ordering           bool
	nonDocResult       bool
	compareField       string
}

func _testRestClientQueryDocumentsCustomDataset(t *testing.T, testName string, testCases []customQueryTestCase, client *gocosmos.RestClient, dbname, collname string) {
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query := gocosmos.QueryReq{DbName: dbname, CollName: collname, Query: testCase.query, CrossPartitionEnabled: true}
			result := client.QueryDocuments(query)
			if result.Error() != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, result.Error())
			}
			var expectedResult []interface{}
			_ = json.Unmarshal([]byte(testCase.expectedResultJson), &expectedResult)
			if result.Count != len(expectedResult) || len(result.Documents) != len(expectedResult) {
				t.Fatalf("%s failed: <num-documents> expected to be %#v but received (count: %#v / len: %#v)", testName+"/"+testCase.name, len(expectedResult), result.Count, len(result.Documents))
			}
			resultDocs := result.Documents
			if !testCase.ordering {
				sort.Slice(resultDocs, func(i, j int) bool {
					if !testCase.nonDocResult {
						var doci, docj = resultDocs.AsDocInfoAt(i), resultDocs.AsDocInfoAt(j)
						stri := doci.GetAttrAsTypeUnsafe(testCase.compareField, reddo.TypeString).(string)
						strj := docj.GetAttrAsTypeUnsafe(testCase.compareField, reddo.TypeString).(string)
						return stri < strj
					}
					stri, _ := json.Marshal(resultDocs[i])
					strj, _ := json.Marshal(resultDocs[j])
					return string(stri) < string(strj)
				})
				sort.Slice(expectedResult, func(i, j int) bool {
					if !testCase.nonDocResult {
						var doci gocosmos.DocInfo = expectedResult[i].(map[string]interface{})
						var docj gocosmos.DocInfo = expectedResult[j].(map[string]interface{})
						stri := doci.GetAttrAsTypeUnsafe(testCase.compareField, reddo.TypeString).(string)
						strj := docj.GetAttrAsTypeUnsafe(testCase.compareField, reddo.TypeString).(string)
						return stri < strj
					}
					stri, _ := json.Marshal(expectedResult[i])
					strj, _ := json.Marshal(expectedResult[j])
					return string(stri) < string(strj)
				})
			}
			for i, doc := range resultDocs {
				myDoc := doc.(interface{})
				if !testCase.nonDocResult {
					docInfo := resultDocs.AsDocInfoAt(i)
					myDoc = docInfo.RemoveSystemAttrs().AsMap()
				}
				expected := expectedResult[i]
				if !reflect.DeepEqual(myDoc, expected) {
					t.Fatalf("%s failed: result\n%#v\ndoes not match expected one\n%#v", testName+"/"+testCase.name, myDoc, expected)
				}
			}
		})
	}
}

func _testRestClientQueryDocumentsDatasetFamilies(t *testing.T, testName string, client *gocosmos.RestClient, dbname, collname string) {
	var testCases = []customQueryTestCase{
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/getting-started
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/select
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/from
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/order-by
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/group-by
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/offset-limit
		{name: "QuerySingleDoc", compareField: "id", query: `SELECT * FROM Families f WHERE f.id = "AndersenFamily"`, expectedResultJson: _toJson([]gocosmos.DocInfo{dataMapFamilies["AndersenFamily"]})},
		{name: "QuerySingleAttr", compareField: "id", query: `SELECT f.address FROM Families f WHERE f.id = "AndersenFamily"`, expectedResultJson: `[{"address":{"state":"WA","county":"King","city":"Seattle"}}]`},
		{name: "QuerySubAttrs", compareField: "id", query: `SELECT {"Name":f.id, "City":f.address.city} AS Family FROM Families f WHERE f.address.city = f.address.state`, expectedResultJson: `[{"Family":{"Name":"WakefieldFamily","City":"NY"}}]`},
		{name: "QuerySubItems1", nonDocResult: true, query: `SELECT * FROM Families.children`, expectedResultJson: `[[{"firstName":"Henriette Thaulow","gender":"female","grade":5,"pets":[{"givenName":"Fluffy"}]}],[{"familyName":"Merriam","gender":"female","givenName":"Jesse","grade":1,"pets":[{"givenName":"Goofy"},{"givenName":"Shadow"}]},{"familyName":"Miller","gender":"female","givenName":"Lisa","grade":8}]]`},
		{name: "QuerySubItems2", nonDocResult: true, query: `SELECT * FROM Families.address.state`, expectedResultJson: `["WA","NY"]`},
		{name: "QuerySingleAttrWithOrderBy", ordering: true, query: `SELECT c.givenName FROM Families f JOIN c IN f.children WHERE f.id = 'WakefieldFamily' ORDER BY f.address.city ASC`, expectedResultJson: `[{"givenName":"Jesse"},{"givenName":"Lisa"}]`},
		{name: "QuerySubAttrsWithOrderByAsc", ordering: true, query: `SELECT f.id, f.address.city FROM Families f ORDER BY f.address.city`, expectedResultJson: `[{"id":"WakefieldFamily","city":"NY"},{"id":"AndersenFamily","city":"Seattle"}]`},
		{name: "QuerySubAttrsWithOrderByDesc", ordering: true, query: `SELECT f.id, f.creationDate FROM Families f ORDER BY f.creationDate DESC`, expectedResultJson: `[{"id":"AndersenFamily","creationDate":1431620472},{"id":"WakefieldFamily","creationDate":1431620462}]`},
		{name: "QuerySubAttrsWithOrderByMissingField", ordering: true, query: `SELECT f.id, f.lastName FROM Families f ORDER BY f.lastName`, expectedResultJson: `[{"id":"WakefieldFamily"},{"id":"AndersenFamily","lastName":"Andersen"}]`},
		{name: "QueryGroupBy", compareField: "id", query: `SELECT COUNT(UniqueLastNames) FROM (SELECT AVG(f.age) FROM f GROUP BY f.lastName) AS UniqueLastNames`, expectedResultJson: `[{"$1":2}]`},
		{name: "QueryOffsetLimitWithOrderBy", compareField: "id", query: `SELECT f.id, f.address.city FROM Families f ORDER BY f.address.city OFFSET 1 LIMIT 1`, expectedResultJson: `[{"id":"AndersenFamily","city":"Seattle"}]`},
		// without ORDER BY, the returned result is un-deterministic
		// {name: "QueryOffsetLimitWithoutOrderBy", query: `SELECT f.id, f.address.city FROM Families f OFFSET 1 LIMIT 1`, expectedResultJson: `[{"id":"AndersenFamily","city":"Seattle"}]`},
	}
	_testRestClientQueryDocumentsCustomDataset(t, testName, testCases, client, dbname, collname)
}

func TestRestClient_QueryDocuments_DatasetFamilies_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_DatasetFamilies_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataFamiliesSmallRU(t, testName, client, dbname, collname)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsDatasetFamilies(t, testName, client, dbname, collname)
}

func TestRestClient_QueryDocuments_DatasetFamilies_LargeRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_DatasetFamilies_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataFamiliesLargeRU(t, testName, client, dbname, collname)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsDatasetFamilies(t, testName, client, dbname, collname)
}

func _testRestClientQueryDocumentsDatasetNutrition(t *testing.T, testName string, client *gocosmos.RestClient, dbname, collname string) {
	var testCases = []customQueryTestCase{
		// ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/group-by
		{name: "Count", query: `SELECT COUNT(1) AS foodGroupCount FROM Food f`, expectedResultJson: `[{"foodGroupCount": 8618}]`},
		{name: "QueryGroupBy1", compareField: "foodGroupCount",
			query:              "SELECT COUNT(1) AS foodGroupCount, UPPER(f.foodGroup) AS upperFoodGroup FROM Food f GROUP BY UPPER(f.foodGroup)",
			expectedResultJson: `[{"foodGroupCount":64,"upperFoodGroup":"SPICES AND HERBS"},{"foodGroupCount":108,"upperFoodGroup":"RESTAURANT FOODS"},{"foodGroupCount":113,"upperFoodGroup":"MEALS, ENTREES, AND SIDE DISHES"},{"foodGroupCount":133,"upperFoodGroup":"NUT AND SEED PRODUCTS"},{"foodGroupCount":165,"upperFoodGroup":"AMERICAN INDIAN/ALASKA NATIVE FOODS"},{"foodGroupCount":171,"upperFoodGroup":"SNACKS"},{"foodGroupCount":183,"upperFoodGroup":"CEREAL GRAINS AND PASTA"},{"foodGroupCount":219,"upperFoodGroup":"FATS AND OILS"},{"foodGroupCount":244,"upperFoodGroup":"SAUSAGES AND LUNCHEON MEATS"},{"foodGroupCount":264,"upperFoodGroup":"DAIRY AND EGG PRODUCTS"},{"foodGroupCount":267,"upperFoodGroup":"FINFISH AND SHELLFISH PRODUCTS"},{"foodGroupCount":315,"upperFoodGroup":"BEVERAGES"},{"foodGroupCount":343,"upperFoodGroup":"PORK PRODUCTS"},{"foodGroupCount":346,"upperFoodGroup":"FRUITS AND FRUIT JUICES"},{"foodGroupCount":347,"upperFoodGroup":"SWEETS"},{"foodGroupCount":362,"upperFoodGroup":"BABY FOODS"},{"foodGroupCount":363,"upperFoodGroup":"BREAKFAST CEREALS"},{"foodGroupCount":371,"upperFoodGroup":"FAST FOODS"},{"foodGroupCount":389,"upperFoodGroup":"LEGUMES AND LEGUME PRODUCTS"},{"foodGroupCount":390,"upperFoodGroup":"POULTRY PRODUCTS"},{"foodGroupCount":438,"upperFoodGroup":"LAMB, VEAL, AND GAME PRODUCTS"},{"foodGroupCount":452,"upperFoodGroup":"SOUPS, SAUCES, AND GRAVIES"},{"foodGroupCount":797,"upperFoodGroup":"BAKED PRODUCTS"},{"foodGroupCount":828,"upperFoodGroup":"VEGETABLES AND VEGETABLE PRODUCTS"},{"foodGroupCount":946,"upperFoodGroup":"BEEF PRODUCTS"}]`},
		{name: "QueryGroupBy2", compareField: "foodGroupCount",
			query:              `SELECT COUNT(1) AS foodGroupCount, ARRAY_CONTAINS(f.tags, {name: 'orange'}) AS containsOrangeTag, f.version BETWEEN 0 AND 2 AS correctVersion FROM Food f GROUP BY ARRAY_CONTAINS(f.tags, {name: 'orange'}), f.version BETWEEN 0 AND 2`,
			expectedResultJson: `[{"foodGroupCount":10,"containsOrangeTag":true,"correctVersion":true},{"foodGroupCount":8608,"containsOrangeTag":false,"correctVersion":true}]`},
	}
	_testRestClientQueryDocumentsCustomDataset(t, testName, testCases, client, dbname, collname)
}

func TestRestClient_QueryDocuments_DatasetNutrition_SmallRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_DatasetNutrition_SmallRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataNutritionSmallRU(t, testName, client, dbname, collname)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count != 1 {
		t.Fatalf("%s failed: <num-partition> expected to be %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsDatasetNutrition(t, testName, client, dbname, collname)
}

func TestRestClient_QueryDocuments_DatasetNutrition_LargeRU(t *testing.T) {
	testName := "TestRestClient_QueryDocuments_DatasetNutrition_LargeRU"
	client := _newRestClient(t, testName)
	dbname := testDb
	collname := testTable
	_initDataNutritionLargeRU(t, testName, client, dbname, collname)
	if result := client.GetPkranges(dbname, collname); result.Error() != nil {
		t.Fatalf("%s failed: %s", testName+"/GetPkranges", result.Error())
	} else if result.Count < 2 {
		t.Fatalf("%s failed: <num-partition> expected to be larger than %#v but received %#v", testName+"/GetPkranges", 1, result.Count)
	}
	_testRestClientQueryDocumentsDatasetNutrition(t, testName, client, dbname, collname)
}
