package gocosmos

import (
	"reflect"
	"testing"
)

func TestStmtInsert_parse(t *testing.T) {
	testName := "TestStmtInsert_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtInsert
		mustError bool
	}{
		{name: "error_no_collection", sql: `INSERT INTO db (a,b,c) VALUES (1,2,3)`, mustError: true},
		{name: "error_values", sql: `INSERT INTO db.table (a,b,c)`, mustError: true},
		{name: "error_columns", sql: `INSERT INTO db.table VALUES (1,2,3)`, mustError: true},
		{name: "error_invalid_string", sql: `INSERT INTO db.table (a) VALUES ('a string')`, mustError: true},
		{name: "error_invalid_string2", sql: `INSERT INTO db.table (a) VALUES ("a string")`, mustError: true},
		{name: "error_invalid_string3", sql: `INSERT INTO db.table (a) VALUES ("{key:value}")`, mustError: true},
		{name: "error_num_values_not_matched", sql: `INSERT INTO db.table (a,b) VALUES (1,2,3)`, mustError: true},
		{name: "error_invalid_number", sql: `INSERT INTO db.table (a,b) VALUES (0x1qa,2)`, mustError: true},
		{name: "error_invalid_string", sql: `INSERT INTO db.table (a,b) VALUES ("cannot \\"unquote",2)`, mustError: true},
		{name: "error_invalid_with", sql: `INSERT INTO db.table (a,b) VALUES (1,2) WITH a`, mustError: true},

		{
			name: "basic",
			sql: `INSERT INTO
db1.table1 (a, b, c, d, e, 
f) VALUES
	(null, 1.0, 
true, "\"a string 'with' \\\"quote\\\"\"", "{\"key\":\"value\"}", "[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]")`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 0}, dbName: "db1", collName: "table1"}, fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`}}},
		},
		{
			name: "with_placeholders",
			sql: `INSERT 
INTO db-2.table_2 (
a,b,c) VALUES (
$1, :3, @2)`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 3}, dbName: "db-2", collName: "table_2"}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "singlepk",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (1,2,3) WITH singlePK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 0}, dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{1.0, 2.0, 3.0}},
		},
		{
			name:     "single_pk",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (:1,$2,3) WITH SINGLE_PK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 2}, dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{2}, 3.0}},
		},
		{
			name:     "singlepk2",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (1,2,3) WITH singlePK=true`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 0}, dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{1.0, 2.0, 3.0}},
		},
		{
			name:     "single_pk2",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (:1,$2,3) WITH SINGLE_PK=true`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 2}, dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{2}, 3.0}},
		},
		{
			name:      "error_singlepk_single_pk",
			sql:       `INSERT INTO db.table (a,b,c) VALUES (1,2,@1) WITH singlePK, with SINGLE_PK`,
			mustError: true,
		},
		{
			name:      "error_invalid_singlepk",
			sql:       `INSERT INTO db.table (a,b,c) VALUES (1,2,3) WITH singlePK=false`,
			mustError: true,
		},
		{
			name:      "error_invalid_single_pk",
			sql:       `INSERT INTO db.table (a,b,c) VALUES (:1,$2,3) WITH SINGLE_PK=error`,
			mustError: true,
		},
		{
			name:     "repeated_placeholders",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (:1,$1,@1)`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 1}, dbName: "db", collName: "table"}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{1}, placeholder{1}}},
		},
		{
			name:     "high_placeholders",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (1,2,$5)`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 5}, dbName: "db", collName: "table"}, fields: []string{"a", "b", "c"}, values: []interface{}{1.0, 2.0, placeholder{5}}},
		},
		{
			name:     "with_pk",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (:1,$2,3) WITH withPk=/mypk`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 2}, dbName: "db", collName: "table", numPkPaths: 1, withPk: "/mypk", pkPaths: []string{"/mypk"}}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{2}, 3.0}},
		},
		{
			name:     "with_pk_subpartition",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (:1,$2,3) WITH PK=/TenantId,/UserId,/SessionId`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 2}, dbName: "db", collName: "table", numPkPaths: 3, withPk: "/TenantId,/UserId,/SessionId", pkPaths: []string{"/TenantId", "/UserId", "/SessionId"}}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{2}, 3.0}},
		},
		{
			name:      "with_pk_singlepk",
			sql:       `INSERT INTO db.table (a,b,c) VALUES (:1,$2,3) WITH withPk=/mypk WITH SINGLE_PK`,
			mustError: true,
		},
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
			stmt, ok := s.(*StmtInsert)
			if !ok {
				t.Fatalf("%s failed: expected StmtInsert but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = &Stmt{numInputs: stmt.numInputs}
			stmt.fieldsStr = ""
			stmt.valuesStr = ""
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %s\nreceived %s", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtInsert_parse_defaultDb(t *testing.T) {
	testName := "TestStmtInsert_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtInsert
		mustError bool
	}{
		{name: "error_invalid_query", sql: `INSERT INTO .table (a,b) VALUES (1,2)`, mustError: true},
		{name: "error_invalid_query2", sql: `INSERT INTO db. (a,b) VALUES (1,2)`, mustError: true},
		{name: "error_invalid_with", db: "mydb", sql: `INSERT INTO table (a,b) VALUES (1,2) WITH a=1`, mustError: true},

		{
			name: "basic",
			db:   "mydb",
			sql: `INSERT INTO
table1 (a, b, c, d, e,
f) VALUES
	(null, 1.0,
true, "\"a string 'with' \\\"quote\\\"\"", "{\"key\":\"value\"}", "[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]")`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table1"}, fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`}}},
		},
		{
			name: "with_placeholders_table_in_query",
			db:   "mydb",
			sql: `INSERT
INTO db-2.table_2 (
a,b,c) VALUES (
$1, :3, @2)`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db-2", collName: "table_2"}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "singlepk",
			db:       "mydb",
			sql:      `INSERT INTO table (a,b,c) VALUES (1,2,3) WITH singlePK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{1.0, 2.0, 3.0}},
		},
		{
			name:     "singlepk2",
			db:       "mydb",
			sql:      `INSERT INTO table (a,b,c) VALUES (1,2,3) WITH singlePK=true`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{1.0, 2.0, 3.0}},
		},
		{
			name:     "single_pk",
			db:       "mydb",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (:1,$2,3) WITH SINGLE_PK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{2}, 3.0}},
		},
		{
			name:     "single_pk2",
			db:       "mydb",
			sql:      `INSERT INTO db.table (a,b,c) VALUES (:1,$2,3) WITH SINGLE_PK=true`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{2}, 3.0}},
		},
		{
			name:      "error_singlepk_single_pk",
			db:        "mydb",
			sql:       `INSERT INTO table (a,b,c) VALUES (1,2,@1) WITH singlePK, with SINGLE_PK`,
			mustError: true,
		},
		{
			name:      "error_invalid_singlepk",
			db:        "mydb",
			sql:       `INSERT INTO table (a,b,c) VALUES (1,2,3) WITH singlePK=false`,
			mustError: true,
		},
		{
			name:      "error_invalid_single_pk",
			db:        "mydb",
			sql:       `INSERT INTO db.table (a,b,c) VALUES (:1,$2,3) WITH SINGLE_PK=error`,
			mustError: true,
		},
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
			stmt, ok := s.(*StmtInsert)
			if !ok {
				t.Fatalf("%s failed: expected StmtInsert but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			stmt.fieldsStr = ""
			stmt.valuesStr = ""
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtUpsert_parse(t *testing.T) {
	testName := "TestStmtUpsert_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtInsert
		mustError bool
	}{
		{name: "error_no_collection", sql: `UPSERT INTO db (a,b,c) VALUES (1,2,3)`, mustError: true},
		{name: "error_values", sql: `UPSERT INTO db.table (a,b,c)`, mustError: true},
		{name: "error_columns", sql: `UPSERT INTO db.table VALUES (1,2,3)`, mustError: true},
		{name: "error_invalid_string", sql: `UPSERT INTO db.table (a) VALUES ('a string')`, mustError: true},
		{name: "error_invalid_string2", sql: `UPSERT INTO db.table (a) VALUES ("a string")`, mustError: true},
		{name: "error_invalid_string3", sql: `UPSERT INTO db.table (a) VALUES ("{key:value}")`, mustError: true},
		{name: "error_num_values_not_matched", sql: `UPSERT INTO db.table (a,b) VALUES (1,2,3)`, mustError: true},
		{name: "error_invalid_number", sql: `UPSERT INTO db.table (a,b) VALUES (0x1qa,2)`, mustError: true},
		{name: "error_invalid_string", sql: `UPSERT INTO db.table (a,b) VALUES ("cannot \\"unquote",2)`, mustError: true},
		{name: "error_invalid_with", sql: `UPSERT INTO db.table (a,b) VALUES (1,2) WITH a`, mustError: true},

		{
			name: "basic",
			sql: `UPSERT INTO
db1.table1 (a,
b, c, d, e,
f) VALUES
	(null, 1.0, true,
  "\"a string 'with' \\\"quote\\\"\"", "{\"key\":\"value\"}", "[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]")`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db1", collName: "table1"}, isUpsert: true, fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`}}},
		},
		{
			name: "with_placeholders",
			sql: `UPSERT
INTO db-2.table_2 (
a,b,c) VALUES ($1,
	:3, @2)`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db-2", collName: "table_2"}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "singlepk",
			sql:      `UPSERT INTO db.table (a,b,c) VALUES (:1, :3, :2) WITH singlePK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "single_pk",
			sql:      `UPSERT INTO db.table (a,b,c) VALUES (:1, :3, :2) WITH SINGLE_PK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "singlepk2",
			sql:      `UPSERT INTO db.table (a,b,c) VALUES (:1, :3, :2) WITH singlePK=true`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "single_pk2",
			sql:      `UPSERT INTO db.table (a,b,c) VALUES (:1, :3, :2) WITH SINGLE_PK=true`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:      "error_singlepk_single_pk",
			sql:       `UPSERT INTO db.table (a,b,c) VALUES (:1, :3, :2) WITH SINGLE_PK, with singlePK`,
			mustError: true,
		},
		{
			name:      "error_invalid_single_pk",
			sql:       `UPSERT INTO db.table (a,b,c) VALUES (:1, :3, :2) WITH SINGLE_PK=error`,
			mustError: true,
		},
		{
			name:      "error_invalid_singlepk",
			sql:       `UPSERT INTO db.table (a,b,c) VALUES (:1, :3, :2) WITH singlePK=false`,
			mustError: true,
		},
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
			stmt, ok := s.(*StmtInsert)
			if !ok {
				t.Fatalf("%s failed: expected StmtInsert but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			stmt.fieldsStr = ""
			stmt.valuesStr = ""
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtUpsert_parse_defaultDb(t *testing.T) {
	testName := "TestStmtUpsert_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtInsert
		mustError bool
	}{
		{name: "error_invalid_query", sql: `UPSERT INTO .table (a,b) VALUES (1,2)`, mustError: true},
		{name: "error_invalid_query2", sql: `UPSERT INTO db. (a,b) VALUES (1,2)`, mustError: true},
		{name: "error_invalid_with", db: "mydb", sql: `UPSERT INTO table (a,b) VALUES (1,2) WITH a=1`, mustError: true},

		{
			name: "basic",
			db:   "mydb",
			sql: `UPSERT INTO
table1 (a,
b, c, d, e,
f) VALUES
	(null, 1.0, true,
  "\"a string 'with' \\\"quote\\\"\"", "{\"key\":\"value\"}", "[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]")`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table1"}, isUpsert: true, fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`}}},
		},
		{
			name: "with_placeholders_table_in_query",
			db:   "mydb",
			sql: `UPSERT
INTO db-2.table_2 (
a,b,c) VALUES ($1,
	:3, @2)`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db-2", collName: "table_2"}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "singlepk",
			db:       "mydb",
			sql:      `UPSERT INTO db.table (a,b,c) VALUES ($1, :3, @2) WITH SINGLEPK`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "single_pk",
			db:       "mydb",
			sql:      `UPSERT INTO table (a,b,c) VALUES ($1, :3, @2) WITH single_pk`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "singlepk2",
			db:       "mydb",
			sql:      `UPSERT INTO db.table (a,b,c) VALUES ($1, :3, @2) WITH SINGLEPK=true`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:     "single_pk2",
			db:       "mydb",
			sql:      `UPSERT INTO table (a,b,c) VALUES ($1, :3, @2) WITH single_pk=true`,
			expected: &StmtInsert{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, isUpsert: true, fields: []string{"a", "b", "c"}, values: []interface{}{placeholder{1}, placeholder{3}, placeholder{2}}},
		},
		{
			name:      "error_singlepk_single_pk",
			db:        "mydb",
			sql:       `UPSERT INTO db.table (a,b,c) VALUES ($1, :3, @2) WITH single_pk WITH singlePK`,
			mustError: true,
		},
		{
			name:      "error_invalid_singlepk",
			db:        "mydb",
			sql:       `UPSERT INTO db.table (a,b,c) VALUES ($1, :3, @2) WITH SINGLEPK=false`,
			mustError: true,
		},
		{
			name:      "error_invalid_single_pk",
			db:        "mydb",
			sql:       `UPSERT INTO table (a,b,c) VALUES ($1, :3, @2) WITH single_pk=error`,
			mustError: true,
		},
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
			stmt, ok := s.(*StmtInsert)
			if !ok {
				t.Fatalf("%s failed: expected StmtInsert but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			stmt.fieldsStr = ""
			stmt.valuesStr = ""
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v/%#v\nreceived %#v/%#v", testName+"/"+testCase.name, testCase.expected.StmtCRUD, testCase.expected, stmt.StmtCRUD, stmt)
			}
		})
	}
}

func TestDummy(t *testing.T) {
	t.Logf("[DEBUG] %s", reDelete.String())
}

func TestStmtDelete_parse(t *testing.T) {
	testName := "TestStmtDelete_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtDelete
		mustError bool
	}{
		{name: "error_no_collection", sql: `DELETE FROM db WHERE id=1`, mustError: true},
		{name: "error_where", sql: `DELETE FROM db.table`, mustError: true},
		{name: "error_empty_id", sql: `DELETE FROM db.table WHERE id=`, mustError: true},
		{name: "error_invalid_value", sql: `DELETE FROM db.table WHERE id="1`, mustError: true},
		{name: "error_invalid_value2", sql: `DELETE FROM db.table WHERE id=2"`, mustError: true},
		{name: "error_invalid_where", sql: `DELETE FROM db.table WHERE id=@1 a`, mustError: true},
		{name: "error_invalid_where2", sql: `DELETE FROM db.table WHERE id=b $2`, mustError: true},
		{name: "error_invalid_where3", sql: `DELETE FROM db.table WHERE id=c :3 d`, mustError: true},
		{name: "error_invalid_with", sql: `DELETE FROM db.table WHERE id=1 WITH a`, mustError: true},

		{
			name: "basic",
			sql: `DELETE FROM 
db1.table1 WHERE 
	id=abc`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{}, dbName: "db1", collName: "table1", pkPaths: []string{}}, id: "abc", pkValues: []interface{}{}},
		},
		{
			name: "basic2",
			sql: `
			DELETE
		FROM db-2.table_2
			WHERE     id="\"def\""`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{}, dbName: "db-2", collName: "table_2", pkPaths: []string{}}, id: "def", pkValues: []interface{}{}},
		},
		{
			name: "basic3",
			sql: `DELETE FROM
		db_3-0.table-3_0 WHERE
			id=@2`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 2}, dbName: "db_3-0", collName: "table-3_0", pkPaths: []string{}}, id: placeholder{2}, pkValues: []interface{}{}},
		},
		{
			name:     "singlepk",
			sql:      `DELETE FROM db.table WHERE id=$1 WITH singlePK`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 1}, dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1, pkPaths: []string{}}, id: placeholder{1}, pkValues: []interface{}{}},
		},
		{
			name:     "single_pk",
			sql:      `DELETE FROM db.table WHERE id=:3 with Single_PK`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 3}, dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1, pkPaths: []string{}}, id: placeholder{3}, pkValues: []interface{}{}},
		},
		{
			name:     "singlepk-2",
			sql:      `DELETE FROM db.table WHERE id=:1 WITH singlePK=true`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 1}, dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1, pkPaths: []string{}}, id: placeholder{1}, pkValues: []interface{}{}},
		},
		{
			name:     "single_pk-2",
			sql:      `DELETE FROM db.table WHERE id=@2 with Single_PK=True`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 2}, dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1, pkPaths: []string{}}, id: placeholder{2}, pkValues: []interface{}{}},
		},
		{
			name:      "error_singlepk_single_pk",
			sql:       `DELETE FROM db.table WHERE id=@2 with SinglePK WITH SINGLE_PK`,
			mustError: true,
		},
		{
			name:      "error_invalid_singlepk",
			sql:       `DELETE FROM db.table WHERE id=@2 WITH singlePK=false`,
			mustError: true,
		},
		{
			name:      "error_invalid_single_pk",
			sql:       `DELETE FROM db.table WHERE id=@2 with Single_PK=error`,
			mustError: true,
		},

		{
			name:     "where_pk",
			sql:      `DELETE FROM db.table WHERE id=1 and withPk=abc`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{}, dbName: "db", collName: "table", numPkPaths: 1, pkPaths: []string{"/withPk"}}, id: 1.0, pkValues: []interface{}{"abc"}},
		},
		{
			name:     "where_pk_subpartitions",
			sql:      `DELETE FROM db.table WHERE id=:3 AND app=$2 and Username=1`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 3}, dbName: "db", collName: "table", numPkPaths: 2, pkPaths: []string{"/app", "/Username"}}, id: placeholder{3}, pkValues: []interface{}{placeholder{2}, 1.0}},
		},
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
			stmt, ok := s.(*StmtDelete)
			if !ok {
				t.Fatalf("%s failed: expected StmtDelete but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = &Stmt{numInputs: stmt.numInputs}
			stmt.whereStr = "" // ignore
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %s\nreceived %s", testName+"/"+testCase.name, testCase.expected.StmtCRUD, stmt.StmtCRUD)
			}
		})
	}
}

func TestStmtDelete_parse_defaultDb(t *testing.T) {
	testName := "TestStmtDelete_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtDelete
		mustError bool
	}{
		{name: "error_invalid_query", sql: `DELETE FROM .table WHERE id=1`, mustError: true},
		{name: "error_invalid_query2", sql: `DELETE FROM db. WHERE id=1`, mustError: true},
		{name: "error_invalid_with", db: "mydb", sql: `DELETE FROM table WHERE id=1 WITH a=1`, mustError: true},

		{
			name: "basic",
			db:   "mydb",
			sql: `DELETE FROM
table1 WHERE
	id=abc`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{}, dbName: "mydb", collName: "table1", pkPaths: []string{}}, id: "abc", pkValues: []interface{}{}},
		},
		{
			name: "db_in_query",
			db:   "mydb",
			sql: `
	DELETE
FROM db-2.table_2
	WHERE     id="\"def\""`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{}, dbName: "db-2", collName: "table_2", pkPaths: []string{}}, id: "def", pkValues: []interface{}{}},
		},
		{
			name: "placeholder",
			db:   "mydb",
			sql: `DELETE FROM
		table-3_0 WHERE
			id=@2`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 2}, dbName: "mydb", collName: "table-3_0", pkPaths: []string{}}, id: placeholder{2}, pkValues: []interface{}{}},
		},
		{
			name:     "singlepk",
			db:       "mydb",
			sql:      `DELETE FROM table WHERE id=:1 With singlePk`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 1}, dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1, pkPaths: []string{}}, id: placeholder{1}, pkValues: []interface{}{}},
		},
		{
			name:     "single_pk",
			db:       "mydb",
			sql:      `DELETE FROM db.table WHERE id=$3 With single_Pk`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 3}, dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1, pkPaths: []string{}}, id: placeholder{3}, pkValues: []interface{}{}},
		},
		{
			name:     "singlepk-2",
			db:       "mydb",
			sql:      `DELETE FROM table WHERE id=@2 With singlePk=true`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 2}, dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1, pkPaths: []string{}}, id: placeholder{2}, pkValues: []interface{}{}},
		},
		{
			name:     "single_pk2",
			db:       "mydb",
			sql:      `DELETE FROM db.table WHERE id=@2 With single_Pk=true`,
			expected: &StmtDelete{StmtCRUD: &StmtCRUD{Stmt: &Stmt{numInputs: 2}, dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1, pkPaths: []string{}}, id: placeholder{2}, pkValues: []interface{}{}},
		},
		{
			name:      "error_singlepk_single_pk",
			db:        "mydb",
			sql:       `DELETE FROM table WHERE id=@2 With single_Pk, With SinglePK`,
			mustError: true,
		},
		{
			name:      "error_invalid_singlepk",
			db:        "mydb",
			sql:       `DELETE FROM table WHERE id=@2 With singlePk=false`,
			mustError: true,
		},
		{
			name:      "error_invalid_single_pk",
			db:        "mydb",
			sql:       `DELETE FROM db.table WHERE id=@2 With single_Pk=error`,
			mustError: true,
		},
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
			stmt, ok := s.(*StmtDelete)
			if !ok {
				t.Fatalf("%s failed: expected StmtDelete but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = &Stmt{numInputs: stmt.numInputs}
			stmt.whereStr = "" // ignore
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %s\nreceived %s", testName+"/"+testCase.name, testCase.expected.StmtCRUD, stmt.StmtCRUD)
			}
		})
	}
}

func TestStmtSelect_parse(t *testing.T) {
	testName := "TestStmtSelect_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtSelect
		mustError bool
	}{
		{name: "error_db_and_collection", sql: `SELECT * FROM db.table`, mustError: true},
		{name: "error_no_collection", sql: `SELECT * WITH db=dbname`, mustError: true},
		{name: "error_no_db", sql: `SELECT * FROM c WITH collection=collname`, mustError: true},
		{name: "error_cross_partition_must_be_true", sql: `SELECT * FROM c WITH db=dbname WITH collection=collname WITH cross_partition=false`, mustError: true},
		{name: "error_cross_partition_must_be_true2", sql: `SELECT * FROM c WITH db=dbname WITH collection=collname WITH cross_partition=error`, mustError: true},
		{name: "error_cross_partition_more_than_once", sql: `SELECT * FROM c WITH db=dbname WITH collection=collname WITH cross_partition WITH CrossPartition=true`, mustError: true},
		{name: "error_cross_partition_more_than_once2", sql: `SELECT CROSS PARTITION * FROM c WITH db=dbname WITH collection=collname WITH CrossPartition`, mustError: true},
		{name: "error_invalid_with", sql: `SELECT * FROM c WITH db=dbname WITH collection=collname WITH a`, mustError: true},
		{name: "error_invalid_with2", sql: `SELECT * FROM c WITH db=dbname WITH collection=collname WITH a=1`, mustError: true},

		{
			name:     "basic",
			sql:      `SELECT * FROM c WITH database=db WITH collection=tbl`,
			expected: &StmtSelect{dbName: "db", collName: "tbl", selectQuery: `SELECT * FROM c`, placeholders: map[int]string{}},
		},
		{
			name:     "cross_partition",
			sql:      `SELECT CROSS PARTITION * FROM c WHERE id="1" WITH db=db-1 WITH table=tbl_1`,
			expected: &StmtSelect{dbName: "db-1", collName: "tbl_1", isCrossPartition: true, selectQuery: `SELECT * FROM c WHERE id="1"`, placeholders: map[int]string{}},
		},
		{
			name:     "placeholders",
			sql:      `SELECT id,username,email FROM c WHERE username!=@1 AND (id>:2 OR email=$3) WITH CROSS_PARTITION=true WITH database=db_3-0 WITH table=table-3_0`,
			expected: &StmtSelect{dbName: "db_3-0", collName: "table-3_0", isCrossPartition: true, selectQuery: `SELECT id,username,email FROM c WHERE username!=@_1 AND (id>@_2 OR email=@_3)`, placeholders: map[int]string{1: "@_1", 2: "@_2", 3: "@_3"}},
		},
		{
			name:     "collection_in_query",
			sql:      `SELECT a,b,c FROM user u WHERE u.id="1" WITH db=dbtemp WITH CrossPartition`,
			expected: &StmtSelect{dbName: "dbtemp", collName: "user", isCrossPartition: true, selectQuery: `SELECT a,b,c FROM user u WHERE u.id="1"`, placeholders: map[int]string{}},
		},
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
			stmt, ok := s.(*StmtSelect)
			if !ok {
				t.Fatalf("%s failed: expected StmtSelect but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtSelect_parse_defaultDb(t *testing.T) {
	testName := "TestStmtSelect_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtSelect
		mustError bool
	}{
		{
			name:     "basic",
			db:       "mydb",
			sql:      `SELECT * FROM c WITH collection=tbl`,
			expected: &StmtSelect{dbName: "mydb", collName: "tbl", selectQuery: `SELECT * FROM c`, placeholders: map[int]string{}},
		},
		{
			name:     "db_table_in_query",
			db:       "mydb",
			sql:      `SELECT CROSS PARTITION * FROM c WHERE id="1" WITH db=db-1 WITH table=tbl_1`,
			expected: &StmtSelect{dbName: "db-1", collName: "tbl_1", isCrossPartition: true, selectQuery: `SELECT * FROM c WHERE id="1"`, placeholders: map[int]string{}},
		},
		{
			name:     "placeholders",
			db:       "mydb",
			sql:      `SELECT id,username,email FROM c WHERE username!=@1 AND (id>:2 OR email=$3) WITH CROSS_PARTITION=true WITH table=tbl_2-0`,
			expected: &StmtSelect{dbName: "mydb", collName: "tbl_2-0", isCrossPartition: true, selectQuery: `SELECT id,username,email FROM c WHERE username!=@_1 AND (id>@_2 OR email=@_3)`, placeholders: map[int]string{1: "@_1", 2: "@_2", 3: "@_3"}},
		},
		{
			name:     "collection_in_query",
			db:       "mydb",
			sql:      `SELECT a,b,c FROM user u WHERE u.id="1" with CrossPartition`,
			expected: &StmtSelect{dbName: "mydb", collName: "user", isCrossPartition: true, selectQuery: `SELECT a,b,c FROM user u WHERE u.id="1"`, placeholders: map[int]string{}},
		},
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
			stmt, ok := s.(*StmtSelect)
			if !ok {
				t.Fatalf("%s failed: expected StmtSelect but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtUpdate_parse(t *testing.T) {
	testName := "TestStmtUpdate_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtUpdate
		mustError bool
	}{
		{name: "error_no_collection", sql: `UPDATE db SET a=1,b=2,c=3 WHERE id=4`, mustError: true},
		{name: "error_where", sql: `UPDATE db.table SET a=1,b=2,c=3 WHERE username=4`, mustError: true},
		{name: "error_no_where", sql: `UPDATE db.table SET a=1,b=2,c=3`, mustError: true},
		{name: "error_no_set", sql: `UPDATE db.table WHERE id=1`, mustError: true},
		{name: "error_empty_set", sql: `UPDATE db.table SET      WHERE id=1`, mustError: true},
		{name: "error_invalid_value", sql: `UPDATE db.table SET a="{key:value}" WHERE id=1`, mustError: true},
		{name: "error_invalid_query", sql: `UPDATE db.table SET =1 WHERE id=2`, mustError: true},
		{name: "error_invalid_query2", sql: `UPDATE db.table SET a=1 WHERE id=   `, mustError: true},
		{name: "error_invalid_query3", sql: `UPDATE db.table SET a=1,b=2,c=3 WHERE id="4`, mustError: true},
		{name: "error_invalid_with", sql: `UPDATE db.table SET a=1,b=2,c=3 WHERE id=4 WITH a`, mustError: true},

		{
			name: "basic",
			sql: `UPDATE db1.table1 
SET a=null, b=
	1.0, c=true, 
  d="\"a string 'with' \\\"quote\\\"\"", e="{\"key\":\"value\"}"
,f="[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]" WHERE
	id="abc"`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db1", collName: "table1"}, updateStr: `a=null, b=
	1.0, c=true, 
  d="\"a string 'with' \\\"quote\\\"\"", e="{\"key\":\"value\"}"
,f="[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]"`, fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`}}, idStr: "abc"},
		},
		{
			name: "basic2",
			sql: `UPDATE db-1.table_1 
SET a=$1, b=
	$2, c=:3, d=0 WHERE
	id=@4`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db-1", collName: "table_1"}, updateStr: `a=$1, b=
	$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "singlepk",
			sql:      `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 with SinglePk`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "single_pk",
			sql:      `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 WITH SINGLE_PK`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "singlepk2",
			sql:      `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 with SinglePk=true`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "single_pk2",
			sql:      `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 WITH SINGLE_PK=true`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:      "error_singlepk_single_pk",
			sql:       `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 with SINGLE_PK, With SinglePk`,
			mustError: true,
		},
		{
			name:      "error_invalid_singlepk",
			sql:       `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 with SinglePk=false`,
			mustError: true,
		},
		{
			name:      "error_invalid_single_pk",
			sql:       `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 WITH SINGLE_PK=error`,
			mustError: true,
		},
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
			stmt, ok := s.(*StmtUpdate)
			if !ok {
				t.Fatalf("%s failed: expected StmtUpdate but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtUpdate_parse_defaultDb(t *testing.T) {
	testName := "TestStmtUpdate_parse_defaultDb"
	testData := []struct {
		name      string
		db        string
		sql       string
		expected  *StmtUpdate
		mustError bool
	}{
		{name: "error_invalid_query", sql: `UPDATE .table SET a=1,b=2,c=3 WHERE id=4`, mustError: true},
		{name: "error_invalid_query2", sql: `UPDATE db. SET a=1,b=2,c=3 WHERE id=4`, mustError: true},
		{name: "error_invalid_with", db: "mydb", sql: `UPDATE table SET a=1,b=2,c=3 WHERE id=4 WITH a=1`, mustError: true},

		{
			name: "basic",
			db:   "mydb",
			sql: `UPDATE table1 
SET a=null, b=
	1.0, c=true, 
  d="\"a string 'with' \\\"quote\\\"\"", e="{\"key\":\"value\"}"
,f="[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]" WHERE
	id="abc"`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table1"}, updateStr: `a=null, b=
	1.0, c=true, 
  d="\"a string 'with' \\\"quote\\\"\"", e="{\"key\":\"value\"}"
,f="[2.0,null,false,\"a string 'with' \\\"quote\\\"\"]"`, fields: []string{"a", "b", "c", "d", "e", "f"}, values: []interface{}{nil, 1.0, true, `a string 'with' "quote"`, map[string]interface{}{"key": "value"}, []interface{}{2.0, nil, false, `a string 'with' "quote"`}}, idStr: "abc"}},
		{
			name: "db_in_query",
			db:   "mydb",
			sql: `UPDATE db-1.table_1 
SET a=$1, b=
	$2, c=:3, d=0 WHERE
	id=@4`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db-1", collName: "table_1"}, updateStr: `a=$1, b=
	$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "singlepk",
			db:       "mydb",
			sql:      `UPDATE table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 with SinglePk`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "single_pk",
			db:       "mydb",
			sql:      `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 WITH SINGLE_PK`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "singlepk2",
			db:       "mydb",
			sql:      `UPDATE table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 with SinglePk=true`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "mydb", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:     "single_pk2",
			db:       "mydb",
			sql:      `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 WITH SINGLE_PK=true`,
			expected: &StmtUpdate{StmtCRUD: &StmtCRUD{dbName: "db", collName: "table", isSinglePathPk: true, numPkPaths: 1}, updateStr: `a=$1, b=$2, c=:3, d=0`, fields: []string{"a", "b", "c", "d"}, values: []interface{}{placeholder{1}, placeholder{2}, placeholder{3}, 0.0}, idStr: "@4", id: placeholder{4}},
		},
		{
			name:      "error_singlepk_single_pk",
			db:        "mydb",
			sql:       `UPDATE table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 with SINGLE_PK, With SinglePk`,
			mustError: true,
		},
		{
			name:      "error_invalid_singlepk",
			db:        "mydb",
			sql:       `UPDATE table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 with SinglePk=false`,
			mustError: true,
		},
		{
			name:      "error_invalid_single_pk",
			db:        "mydb",
			sql:       `UPDATE db.table SET a=$1, b=$2, c=:3, d=0 WHERE id=@4 WITH SINGLE_PK=error`,
			mustError: true,
		},
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
			stmt, ok := s.(*StmtUpdate)
			if !ok {
				t.Fatalf("%s failed: expected StmtUpdate but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}
