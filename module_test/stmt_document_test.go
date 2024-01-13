package gocosmos_test

import (
	"errors"
	"fmt"
	"github.com/microsoft/gocosmos"
	"strings"
	"testing"
)

func TestStmtUpdate_Query(t *testing.T) {
	testName := "TestStmtUpdate_Query"
	db := _openDb(t, testName)
	_, err := db.Query("UPDATE db.table SET a=1 WHERE id=2", nil)
	if !errors.Is(err, gocosmos.ErrQueryNotSupported) {
		t.Fatalf("%s failed: expected ErrQueryNotSupported, but received %#v", testName, err)
	}
}

func TestStmtUpdate_Exec(t *testing.T) {
	testName := "TestStmtUpdate_Exec"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
	testData := []struct {
		name         string
		initSqls     []string
		initParams   [][]interface{}
		sql          string
		args         []interface{}
		mustConflict bool
		mustNotFound bool
		mustError    string
		affectedRows int64
	}{
		{
			name: "update_1",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email,grade,active) VALUES (@1,$2,:3,$4,@5)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email,grade,active) VALUES (@1,$2,:3,$4,@5)`, dbname),
			},
			initParams:   [][]interface{}{nil, nil, nil, nil, {"1", "user", "user@domain.com", 1, true, "user"}, {"2", "user", "user2@domain.com", 1, true, "user"}},
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET grade=2.0,active=false,data="\"a string 'with' \\\"quote\\\"\"" WHERE id=1`, dbname),
			args:         []interface{}{"user"},
			affectedRows: 1,
		},
		{
			name:         "update_pk",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET username="\"user1\"" WHERE id=1 with SinglePk`, dbname),
			args:         []interface{}{"user1"},
			affectedRows: 0,
		},
		{
			name:         "error_uk",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET email="\"user2@domain.com\"" WHERE id=1`, dbname),
			args:         []interface{}{"user"},
			mustConflict: true,
		},
		{
			name:         "row_not_exists",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET grade=3.4 WHERE id=3 with Single_Pk`, dbname),
			args:         []interface{}{"user"},
			affectedRows: 0,
		},
		{
			name:         "row_not_exists_in_partition",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET grade=5.6 WHERE id=2`, dbname),
			args:         []interface{}{"user2"},
			affectedRows: 0,
		},
		{
			name:         "table_not_exists",
			sql:          fmt.Sprintf(`UPDATE %s.tbl_not_found SET email="\"user2@domain.com\"" WHERE id=1`, dbname),
			args:         []interface{}{"user"},
			mustNotFound: true,
		},
		{
			name:         "db_not_exists",
			sql:          `UPDATE db_not_exists.tbltemp SET email="\"user2@domain.com\"" WHERE id=1`,
			args:         []interface{}{"user"},
			mustNotFound: true,
		},
		{
			name: "update_1_placeholders",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email,grade,active) VALUES (@1,$2,:3,$4,@5)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email,grade,active) VALUES (@1,$2,:3,$4,@5)`, dbname),
			},
			initParams:   [][]interface{}{nil, nil, nil, nil, {"1", "user", "user@domain.com", 1, true, "user"}, {"2", "user", "user2@domain.com", 1, true, "user"}},
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET grade=:1,active=@2,data=$3 WHERE id=:4  with SinglePk`, dbname),
			args:         []interface{}{2.0, false, "a string 'with' \"quote\"", "1", "user"},
			affectedRows: 1,
		},
		{
			name:         "update_pk_placeholders",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET username=$1 WHERE id=:2`, dbname),
			args:         []interface{}{"user1", "1", "user1"},
			affectedRows: 0,
		},
		{
			name:         "error_uk_placeholders",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET email=@1 WHERE id=:2 with Single_Pk`, dbname),
			args:         []interface{}{"user2@domain.com", "1", "user"},
			mustConflict: true,
		},
		{
			name:         "row_not_exists_placeholders",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET grade=$1 WHERE id=:2`, dbname),
			args:         []interface{}{3.4, "3", "user"},
			affectedRows: 0,
		},
		{
			name:         "row_not_exists_in_partition_placeholders",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET grade=@1 WHERE id=:2`, dbname),
			args:         []interface{}{5.6, "2", "user2"},
			affectedRows: 0,
		},
		{
			name:         "table_not_exists_placeholders",
			sql:          fmt.Sprintf(`UPDATE %s.tbl_not_found SET email=:1 WHERE id=:2 with SinglePk`, dbname),
			args:         []interface{}{"user2@domain.com", "1", "user"},
			mustNotFound: true,
		},
		{
			name:         "db_not_exists_placeholders",
			sql:          `UPDATE db_not_exists.tbltemp SET email=:1 WHERE id=:2`,
			args:         []interface{}{"user2@domain.com", "1", "user"},
			mustNotFound: true,
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			for i, initSql := range testCase.initSqls {
				var params []interface{}
				if len(testCase.initParams) > i {
					params = testCase.initParams[i]
				}
				_, err := db.Exec(initSql, params...)
				if err != nil {
					t.Fatalf("%s failed: {error: %s / sql: %s}", testName+"/"+testCase.name+"/init", err, initSql)
				}
			}
			execResult, err := db.Exec(testCase.sql, testCase.args...)
			if testCase.mustConflict && !errors.Is(err, gocosmos.ErrConflict) {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && !errors.Is(err, gocosmos.ErrNotFound) {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' bur received %#v", testCase.name, testCase.mustError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}
		})
	}
}

func TestStmtUpdate_Exec_DefaultDb(t *testing.T) {
	testName := "TestStmtUpdate_Exec_DefaultDb"
	dbname := "dbdefault"
	db := _openDefaultDb(t, testName, dbname)
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
	testData := []struct {
		name         string
		initSqls     []string
		initParams   [][]interface{}
		sql          string
		args         []interface{}
		mustConflict bool
		mustNotFound bool
		mustError    string
		affectedRows int64
	}{
		{
			name: "update_1",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email,grade,active) VALUES (@1,$2,:3,$4,@5)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email,grade,active) VALUES (@1,$2,:3,$4,@5)`, dbname),
			},
			initParams:   [][]interface{}{nil, nil, nil, nil, {"1", "user", "user@domain.com", 1, true, "user"}, {"2", "user", "user2@domain.com", 1, true, "user"}},
			sql:          `UPDATE tbltemp SET grade=2.0,active=false,data="\"a string 'with' \\\"quote\\\"\"" WHERE id=1 with SinglePk`,
			args:         []interface{}{"user"},
			affectedRows: 1,
		},
		{
			name:         "update_pk",
			sql:          `UPDATE tbltemp SET username="\"user1\"" WHERE id=1`,
			args:         []interface{}{"user1"},
			affectedRows: 0,
		},
		{
			name:         "error_uk",
			sql:          `UPDATE tbltemp SET email="\"user2@domain.com\"" WHERE id=1 with Single_Pk`,
			args:         []interface{}{"user"},
			mustConflict: true,
		},
		{
			name:         "row_not_exists",
			sql:          `UPDATE tbltemp SET grade=3.4 WHERE id=3`,
			args:         []interface{}{"user"},
			affectedRows: 0,
		},
		{
			name:         "row_not_exists_in_partition",
			sql:          `UPDATE tbltemp SET grade=5.6 WHERE id=2 with SinglePk`,
			args:         []interface{}{"user2"},
			affectedRows: 0,
		},
		{
			name:         "table_not_exists",
			sql:          `UPDATE tbl_not_found SET email="\"user2@domain.com\"" WHERE id=1`,
			args:         []interface{}{"user"},
			mustNotFound: true,
		},
		{
			name: "update_1_placeholders",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/username WITH uk=/email", dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email,grade,active) VALUES (@1,$2,:3,$4,@5)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,username,email,grade,active) VALUES (@1,$2,:3,$4,@5)`, dbname),
			},
			initParams:   [][]interface{}{nil, nil, nil, nil, {"1", "user", "user@domain.com", 1, true, "user"}, {"2", "user", "user2@domain.com", 1, true, "user"}},
			sql:          `UPDATE tbltemp SET grade=:1,active=@2,data=$3 WHERE id=:4 with SinglePk`,
			args:         []interface{}{2.0, false, "a string 'with' \"quote\"", "1", "user"},
			affectedRows: 1,
		},
		{
			name:         "update_pk_placeholders",
			sql:          `UPDATE tbltemp SET username=$1 WHERE id=:2`,
			args:         []interface{}{"user1", "1", "user1"},
			affectedRows: 0,
		},
		{
			name:         "error_uk_placeholders",
			sql:          `UPDATE tbltemp SET email=@1 WHERE id=:2`,
			args:         []interface{}{"user2@domain.com", "1", "user"},
			mustConflict: true,
		},
		{
			name:         "row_not_exists_placeholders",
			sql:          `UPDATE tbltemp SET grade=$1 WHERE id=:2 with Single_Pk`,
			args:         []interface{}{3.4, "3", "user"},
			affectedRows: 0,
		},
		{
			name:         "row_not_exists_in_partition_placeholders",
			sql:          `UPDATE tbltemp SET grade=@1 WHERE id=:2`,
			args:         []interface{}{5.6, "2", "user2"},
			affectedRows: 0,
		},
		{
			name:         "table_not_exists_placeholders",
			sql:          `UPDATE tbl_not_found SET email=:1 WHERE id=:2`,
			args:         []interface{}{"user2@domain.com", "1", "user"},
			mustNotFound: true,
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			for i, initSql := range testCase.initSqls {
				var params []interface{}
				if len(testCase.initParams) > i {
					params = testCase.initParams[i]
				}
				_, err := db.Exec(initSql, params...)
				if err != nil {
					t.Fatalf("%s failed: {error: %s / sql: %s}", testName+"/"+testCase.name+"/init", err, initSql)
				}
			}
			execResult, err := db.Exec(testCase.sql, testCase.args...)
			if testCase.mustConflict && !errors.Is(err, gocosmos.ErrConflict) {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && !errors.Is(err, gocosmos.ErrNotFound) {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' bur received %#v", testCase.name, testCase.mustError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}
		})
	}
}

func TestStmtUpdate_SubPartitions(t *testing.T) {
	testName := "TestStmtUpdate_SubPartitions"
	db := _openDb(t, testName)
	dbname := "dbtemp"
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	}()
	testData := []struct {
		name         string
		initSqls     []string
		initParams   [][]interface{}
		sql          string
		args         []interface{}
		mustConflict bool
		mustNotFound bool
		mustError    string
		affectedRows int64
	}{
		{
			name: "update_1",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/app,/username WITH uk=/email", dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email,grade,active) VALUES (@1,$2,:3,$4,@5,:6)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email,grade,active) VALUES (@1,$2,:3,$4,@5,:6)`, dbname),
			},
			initParams: [][]interface{}{nil, nil, nil, nil, {"1", "app", "user", "user@domain.com", 1, true, "app", "user"},
				{"2", "app", "user", "user2@domain.com", 1, true, "app", "user"}},
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET grade=2.0,active=false,data="\"a string 'with' \\\"quote\\\"\"" WHERE id=1`, dbname),
			args:         []interface{}{"app", "user"},
			affectedRows: 1,
		},
		{
			name:         "update_pk",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET username="\"user1\"" WHERE id=1`, dbname),
			args:         []interface{}{"app", "user1"},
			affectedRows: 0,
		},
		{
			name:         "error_uk",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET email="\"user2@domain.com\"" WHERE id=1`, dbname),
			args:         []interface{}{"app", "user"},
			mustConflict: true,
		},
		{
			name:         "row_not_exists",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET grade=3.4 WHERE id=3`, dbname),
			args:         []interface{}{"app", "user"},
			affectedRows: 0,
		},
		{
			name:         "row_not_exists_in_partition",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET grade=5.6 WHERE id=2`, dbname),
			args:         []interface{}{"app", "user2"},
			affectedRows: 0,
		},
		{
			name: "update_1_placeholders",
			initSqls: []string{
				"DROP DATABASE IF EXISTS db_not_exists",
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname),
				fmt.Sprintf("CREATE DATABASE %s", dbname),
				fmt.Sprintf("CREATE COLLECTION %s.tbltemp WITH pk=/app,/username WITH uk=/email", dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email,grade,active) VALUES (@1,$2,:3,$4,@5,:6)`, dbname),
				fmt.Sprintf(`INSERT INTO %s.tbltemp (id,app,username,email,grade,active) VALUES (@1,$2,:3,$4,@5,:6)`, dbname),
			},
			initParams: [][]interface{}{nil, nil, nil, nil, {"1", "app", "user", "user@domain.com", 1, true, "app", "user"},
				{"2", "app", "user", "user2@domain.com", 1, true, "app", "user"}},
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET grade=:1,active=@2,data=$3 WHERE id=:4`, dbname),
			args:         []interface{}{2.0, false, "a string 'with' \"quote\"", "1", "app", "user"},
			affectedRows: 1,
		},
		{
			name:         "update_pk_placeholders",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET username=$1 WHERE id=:2`, dbname),
			args:         []interface{}{"user1", "1", "app", "user1"},
			affectedRows: 0,
		},
		{
			name:         "error_uk_placeholders",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET email=@1 WHERE id=:2`, dbname),
			args:         []interface{}{"user2@domain.com", "1", "app", "user"},
			mustConflict: true,
		},
		{
			name:         "row_not_exists_placeholders",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET grade=$1 WHERE id=:2`, dbname),
			args:         []interface{}{3.4, "3", "app", "user"},
			affectedRows: 0,
		},
		{
			name:         "row_not_exists_in_partition_placeholders",
			sql:          fmt.Sprintf(`UPDATE %s.tbltemp SET grade=@1 WHERE id=:2`, dbname),
			args:         []interface{}{5.6, "2", "app", "user2"},
			affectedRows: 0,
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			for i, initSql := range testCase.initSqls {
				var params []interface{}
				if len(testCase.initParams) > i {
					params = testCase.initParams[i]
				}
				_, err := db.Exec(initSql, params...)
				if err != nil {
					t.Fatalf("%s failed: {error: %s / sql: %s}", testName+"/"+testCase.name+"/init", err, initSql)
				}
			}
			execResult, err := db.Exec(testCase.sql, testCase.args...)
			if testCase.mustConflict && !errors.Is(err, gocosmos.ErrConflict) {
				t.Fatalf("%s failed: expect ErrConflict but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustNotFound && !errors.Is(err, gocosmos.ErrNotFound) {
				t.Fatalf("%s failed: expect ErrNotFound but received %#v", testName+"/"+testCase.name+"/exec", err)
			}
			if testCase.mustConflict || testCase.mustNotFound {
				return
			}
			if testCase.mustError != "" {
				if err == nil || strings.Index(err.Error(), testCase.mustError) < 0 {
					t.Fatalf("%s failed: expected '%s' bur received %#v", testCase.name, testCase.mustError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}
		})
	}
}
