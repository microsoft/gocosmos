package gocosmos

import (
	"reflect"
	"testing"
)

func TestStmtCreateCollection_parse(t *testing.T) {
	testName := "TestStmtCreateCollection_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtCreateCollection
		mustError bool
	}{
		{name: "error_no_pk", sql: "CREATE collection db.coll", mustError: true},
		{name: "error_pk_and_large_pk", sql: "CREATE collection db.coll WITH Pk=/a WITH largepk=/b", mustError: true},
		{name: "error_invalid_pk", sql: "CREATE collection db.coll WITH Pk=", mustError: true},
		{name: "error_invalid_large_pk", sql: "CREATE collection db.coll WITH largepk=", mustError: true},
		{name: "error_ru_and_maxru", sql: "CREATE collection db.coll WITH Pk=/id WITH ru=400 WITH maxru=1000", mustError: true},
		{name: "error_invalid_ru", sql: "create TABLE db.coll WITH Pk=/id WITH ru=-1 WITH maxru=1000", mustError: true},
		{name: "error_invalid_maxru", sql: "CREATE COLLECTION db.coll WITH Pk=/id WITH ru=400 WITH maxru=-1", mustError: true},
		{name: "error_invalid_ru2", sql: "CREATE TABLE db.table WITH Pk=/id WITH ru=-1", mustError: true},
		{name: "error_invalid_maxru2", sql: "CREATE COLLECTION db.table WITH Pk=/id WITH maxru=-1", mustError: true},
		{name: "error_no_collection", sql: "CREATE TABLE db WITH Pk=/id", mustError: true},
		{name: "error_if_not_exist", sql: "CREATE TABLE IF NOT EXIST db.table WITH Pk=/id", mustError: true},
		{name: "error_invalid_with", sql: "CREATE TABLE db.table WITH Pk=/id, WITH a=1", mustError: true},

		{name: "basic", sql: "CREATE COLLECTION db1.table1 WITH pk=/id", expected: &StmtCreateCollection{dbName: "db1", collName: "table1", pk: "/id"}},
		{name: "table_with_ru", sql: "create\ntable\rdb-2.table_2 WITH\tPK=/email WITH\r\nru=100", expected: &StmtCreateCollection{dbName: "db-2", collName: "table_2", pk: "/email", ru: 100}},
		{name: "if_not_exists_large_pk_with_maxru", sql: "CREATE collection\nIF\rNOT\t\nEXISTS\n\tdb_3.table-3 with largePK=/id WITH\t\rmaxru=100", expected: &StmtCreateCollection{dbName: "db_3", collName: "table-3", ifNotExists: true, pk: "/id", maxru: 100}},
		{name: "table_if_not_exists_large_pk_with_uk", sql: "create TABLE if not exists db-0_1.table_0-1 WITH LARGEpk=/a/b/c with uk=/a:/b,/c/d;/e/f/g", expected: &StmtCreateCollection{dbName: "db-0_1", collName: "table_0-1", ifNotExists: true, pk: "/a/b/c", uk: [][]string{{"/a"}, {"/b", "/c/d"}, {"/e/f/g"}}}},
		{name: "subpartitions", sql: "CREATE COLLECTION db1.table1 WITH pK=/TenantId,/UserId,/SessionId", expected: &StmtCreateCollection{dbName: "db1", collName: "table1", pk: "/TenantId,/UserId,/SessionId"}},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := ParseQueryWithDefaultDb(nil, "", testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s\n%s", testName+"/"+testCase.name, err, testCase.sql)
			}
			stmt, ok := s.(*StmtCreateCollection)
			if !ok {
				t.Fatalf("%s failed: expected StmtCreateCollection but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtCreateCollection_parse_defaultDb(t *testing.T) {
	testName := "TestStmtCreateCollection_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtCreateCollection
		mustError bool
	}{
		{name: "error_invalid_query", db: "mydb", sql: "CREATE TABLE .mytable WITH Pk=/id", mustError: true},
		{name: "error_invalid_with", db: "mydb", sql: "CREATE TABLE mytable WITH Pk=/id WITH a", mustError: true},

		{name: "basic", db: "mydb", sql: "CREATE COLLECTION table1 WITH PK=/id", expected: &StmtCreateCollection{dbName: "mydb", collName: "table1", pk: "/id"}},
		{name: "db_in_query", db: "mydb", sql: "create\ntable\r\ndb2.table_2 WITH\r\t\nPK=/email WITH\nru=100", expected: &StmtCreateCollection{dbName: "db2", collName: "table_2", pk: "/email", ru: 100}},
		{name: "if_not_exists", db: "mydb", sql: "CREATE collection\nIF\nNOT\t\nEXISTS\n\ttable-3 with largePK=/id WITH\tmaxru=100", expected: &StmtCreateCollection{dbName: "mydb", collName: "table-3", ifNotExists: true, pk: "/id", maxru: 100}},
		{name: "db_in_query_if_not_exists", db: "mydb", sql: "create TABLE if not exists db3.table_0-1 WITH LARGEpk=/a/b/c with uk=/a:/b,/c/d;/e/f/g", expected: &StmtCreateCollection{dbName: "db3", collName: "table_0-1", ifNotExists: true, pk: "/a/b/c", uk: [][]string{{"/a"}, {"/b", "/c/d"}, {"/e/f/g"}}}},
		{name: "subpartitions", db: "mydb", sql: "CREATE COLLECTION table1 WITH pk=/TenantId,/UserId", expected: &StmtCreateCollection{dbName: "mydb", collName: "table1", pk: "/TenantId,/UserId"}},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := ParseQueryWithDefaultDb(nil, testCase.db, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtCreateCollection)
			if !ok {
				t.Fatalf("%s failed: expected StmtCreateCollection but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtAlterCollection_parse(t *testing.T) {
	testName := "TestStmtAlterCollection_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtAlterCollection
		mustError bool
	}{
		{name: "error_no_ru_maxru", sql: "ALTER collection db.coll", mustError: true},
		{name: "error_no_db", sql: "ALTER collection coll WITH ru=400", mustError: true},
		{name: "error_invalid_query", sql: "ALTER collection .coll WITH maxru=4000", mustError: true},
		{"error_ru_and_maxru", "alter TABLE db.coll WITH ru=400 WITH maxru=4000", nil, true},
		{name: "error_invalid_ru", sql: "alter TABLE db.coll WITH ru=-1", mustError: true},
		{name: "error_invalid_maxru", sql: "alter TABLE db.coll WITH maxru=-1", mustError: true},
		{name: "error_invalid_with", sql: "alter TABLE db.coll WITH ru=400, WITH a=1", mustError: true},

		{name: "basic", sql: "ALTER collection db1.table1 WITH ru=400", expected: &StmtAlterCollection{dbName: "db1", collName: "table1", ru: 400}},
		{name: "table", sql: "alter\nTABLE\rdb-2.table_2 WITH\tmaxru=40000", expected: &StmtAlterCollection{dbName: "db-2", collName: "table_2", maxru: 40000}},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := ParseQueryWithDefaultDb(nil, "", testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtAlterCollection)
			if !ok {
				t.Fatalf("%s failed: expected StmtAlterCollection but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtAlterCollection_parse_defaultDb(t *testing.T) {
	testName := "TestStmtAlterCollection_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtAlterCollection
		mustError bool
	}{
		{name: "error_invalid_query", db: "mydb", sql: "ALTER COLLECTION .mytable WITH ru=400", mustError: true},
		{name: "error_notable", db: "mydb", sql: "ALTER COLLECTION mydb. WITH ru=400", mustError: true},
		{name: "error_no_db_table", db: "mydb", sql: "ALTER COLLECTION     WITH ru=400", mustError: true},
		{name: "error_invalid_with", db: "mydb", sql: "ALTER COLLECTION mytable WITH ru=400 WITH a", mustError: true},

		{name: "basic", db: "mydb", sql: "ALTER collection table1 WITH ru=400", expected: &StmtAlterCollection{dbName: "mydb", collName: "table1", ru: 400}},
		{name: "db_in_query", db: "mydb", sql: "alter\nTABLE\rdb-2.table_2 WITH\tmaxru=40000", expected: &StmtAlterCollection{dbName: "db-2", collName: "table_2", maxru: 40000}},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := ParseQueryWithDefaultDb(nil, testCase.db, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtAlterCollection)
			if !ok {
				t.Fatalf("%s failed: expected StmtAlterCollection but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtDropCollection_parse(t *testing.T) {
	testName := "TestStmtDropCollection_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtDropCollection
		mustError bool
	}{
		{name: "error_no_collection", sql: "DROP collection db", mustError: true},
		{name: "error_no_collection2", sql: "Drop Table db.", mustError: true},
		{name: "error_invalid_query", sql: "DROP COLLECTION .mytable", mustError: true},
		{name: "error_if_not_exists", sql: "DROP COLLECTION IF NOT EXISTS mydb.mytable", mustError: true},
		{name: "error_if_exist", sql: "DROP COLLECTION IF EXIST mydb.mytable", mustError: true},

		{name: "basic", sql: "DROP \rCOLLECTION\n db1.table1", expected: &StmtDropCollection{dbName: "db1", collName: "table1"}},
		{name: "table", sql: "DROP\t\rtable\n\tdb-2.table_2", expected: &StmtDropCollection{dbName: "db-2", collName: "table_2"}},
		{name: "if_exists", sql: "drop \rcollection\n IF EXISTS \t db_3.table-3", expected: &StmtDropCollection{dbName: "db_3", ifExists: true, collName: "table-3"}},
		{name: "table_if_exists", sql: "Drop Table If Exists db-4_0.table_4-0", expected: &StmtDropCollection{dbName: "db-4_0", ifExists: true, collName: "table_4-0"}},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := ParseQueryWithDefaultDb(nil, "", testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtDropCollection)
			if !ok {
				t.Fatalf("%s failed: expected StmtDropCollection but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtDropCollection_parse_defaultDb(t *testing.T) {
	testName := "TestStmtDropCollection_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtDropCollection
		mustError bool
	}{
		{name: "error_invalid_query", db: "mydb", sql: "DROP collection .mytable", mustError: true},
		{name: "error_if_not_exists", db: "mydb", sql: "DROP COLLECTION IF NOT EXISTS mydb.mytable", mustError: true},
		{name: "error_if_exists", db: "mydb", sql: "DROP COLLECTION IF EXIST mydb.mytable", mustError: true},

		{name: "basic", db: "mydb", sql: "DROP COLLECTION table1", expected: &StmtDropCollection{dbName: "mydb", collName: "table1"}},
		{name: "db_in_query", db: "mydb", sql: "DROP\t\rtable\n\tdb-2.table_2", expected: &StmtDropCollection{dbName: "db-2", collName: "table_2"}},
		{name: "if_exists", db: "mydb", sql: "drop \tcollection\r IF   EXISTS \n table-3", expected: &StmtDropCollection{dbName: "mydb", ifExists: true, collName: "table-3"}},
		{name: "table_if_exists", db: "mydb", sql: "Drop Table If Exists db-4_0.table_4-0", expected: &StmtDropCollection{dbName: "db-4_0", ifExists: true, collName: "table_4-0"}},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := ParseQueryWithDefaultDb(nil, testCase.db, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtDropCollection)
			if !ok {
				t.Fatalf("%s failed: expected StmtDropCollection but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtListCollections_parse(t *testing.T) {
	testName := "TestStmtListCollections_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtListCollections
		mustError bool
	}{
		{name: "error_invalid_query1", sql: "LIST COLLECTIONS", mustError: true},
		{name: "error_invalid_query2", sql: "LIST TABLES", mustError: true},
		{name: "error_invalid_query3", sql: "LIST COLLECTION", mustError: true},
		{name: "error_invalid_query4", sql: "LIST TABLE", mustError: true},
		{name: "error_invalid_query5", sql: "LIST COLLECTIONS FROM", mustError: true},
		{name: "error_invalid_query6", sql: "LIST TABLES FROM", mustError: true},
		{name: "error_invalid_query7", sql: "LIST COLLECTION FROM", mustError: true},
		{name: "error_invalid_query8", sql: "LIST TABLE FROM", mustError: true},

		{name: "basic", sql: "LIST COLLECTIONS from db1", expected: &StmtListCollections{dbName: "db1"}},
		{name: "collections", sql: "list \n\tcollection \t FROM \t\rdb-2", expected: &StmtListCollections{dbName: "db-2"}},
		{name: "tables", sql: "LIST tables\n\tFROM\t\rdb_3", expected: &StmtListCollections{dbName: "db_3"}},
		{name: "table", sql: "list TABLE from db-4_0", expected: &StmtListCollections{dbName: "db-4_0"}},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := ParseQueryWithDefaultDb(nil, "", testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtListCollections)
			if !ok {
				t.Fatalf("%s failed: expected StmtListCollections but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtListCollections_parse_defaultDb(t *testing.T) {
	testName := "TestStmtListCollections_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtListCollections
		mustError bool
	}{
		{name: "basic", db: "mydb", sql: "LIST COLLECTIONS", expected: &StmtListCollections{dbName: "mydb"}},
		{name: "db_in_query", db: "mydb", sql: "list\r\tcollection FROM\n db-2", expected: &StmtListCollections{dbName: "db-2"}},
		{name: "tables", db: "mydb", sql: "LIST tables", expected: &StmtListCollections{dbName: "mydb"}},
		{name: "table_db_in_query", db: "mydb", sql: "list TABLE from db-4_0", expected: &StmtListCollections{dbName: "db-4_0"}},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := ParseQueryWithDefaultDb(nil, testCase.db, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtListCollections)
			if !ok {
				t.Fatalf("%s failed: expected StmtListCollections but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}
