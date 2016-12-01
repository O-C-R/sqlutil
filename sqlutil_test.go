package sqlutil

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/O-C-R/auth/id"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const testDBUp = `
DROP TABLE IF EXISTS test;
CREATE TABLE test (
	id bytea PRIMARY KEY,
	unique_key varchar(256) NOT NULL UNIQUE,
	value varchar(256)
);


DROP TABLE IF EXISTS test_serial;
CREATE TABLE test_serial (
	id serial PRIMARY KEY,
	unique_key varchar(256) NOT NULL UNIQUE,
	value varchar(256)
);
`

const testDBDown = `
DROP TABLE test, test_serial;
`

type TestValue struct {
	ID        id.ID   `db:"id"`
	UniqueKey string  `db:"unique_key"`
	Value     *string `db:"value"`
}

type TestSerialValue struct {
	ID        *int    `db:"id"`
	UniqueKey string  `db:"unique_key"`
	Value     *string `db:"value"`
}

func init() {
	Register(TestValue{}, "test")
	Register(TestSerialValue{}, "test_serial")
}

func testDB(t *testing.T, test func(*sql.DB) error) {
	url := "postgres://localhost/test?sslmode=disable"
	if envURL := os.Getenv("POSTGRES_URL"); envURL != "" {
		url = envURL
	}

	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	if _, err := db.Exec(testDBUp); err != nil {
		t.Error(err)
		return
	}

	if err := test(db); err != nil {
		t.Error(err)
	}

	if _, err := db.Exec(testDBDown); err != nil {
		t.Error(err)
	}
}

func TestDBUp(t *testing.T) {
	testDB(t, func(db *sql.DB) error {
		return nil
	})
}

func TestInsertFunc(t *testing.T) {
	testDB(t, func(db *sql.DB) error {

		valueFunc, err := InsertFunc(db, TestValue{}, "test")
		if err != nil {
			return err
		}

		testValue := TestValue{}

		id1, err := id.New()
		if err != nil {
			return err
		}

		testValue.ID = id1
		testValue.UniqueKey = "id1"
		returnedID1Interface, err := valueFunc(testValue)
		if err != nil {
			return err
		}

		returnedID1, ok := returnedID1Interface.(id.ID)
		if !ok {
			return fmt.Errorf("unexpected type %T", returnedID1Interface)
		}

		if id1 != returnedID1 {
			return fmt.Errorf("%s != %s", id1, returnedID1)
		}

		id2, err := id.New()
		if err != nil {
			return err
		}

		testValue.ID = id2
		testValue.UniqueKey = "id2"
		returnedID2Interface, err := valueFunc(testValue)
		if err != nil {
			return err
		}

		returnedID2, ok := returnedID2Interface.(id.ID)
		if !ok {
			return fmt.Errorf("unexpected type %T", returnedID2Interface)
		}

		if id2 != returnedID2 {
			return fmt.Errorf("%s != %s", id2, returnedID2)
		}

		return nil
	})
}

func TestInsertSerialFunc(t *testing.T) {
	testDB(t, func(db *sql.DB) error {

		valueFunc, err := InsertSerialFunc(db, TestSerialValue{}, "test_serial")
		if err != nil {
			return err
		}

		testValue := TestSerialValue{}

		testValue.UniqueKey = "id1"
		returnedID1Interface, err := valueFunc(testValue)
		if err != nil {
			return err
		}

		returnedID1, ok := returnedID1Interface.(*int)
		if !ok {
			return fmt.Errorf("unexpected type %T", returnedID1Interface)
		}

		if *returnedID1 != 1 {
			return fmt.Errorf("%d != %d", *returnedID1, 1)
		}

		testValue.UniqueKey = "id2"
		returnedID2Interface, err := valueFunc(testValue)
		if err != nil {
			return err
		}

		returnedID2, ok := returnedID2Interface.(*int)
		if !ok {
			return fmt.Errorf("unexpected type %T", returnedID1Interface)
		}

		if *returnedID2 != 2 {
			return fmt.Errorf("%d != %d", *returnedID2, 2)
		}

		return nil
	})
}

func TestUpsertFunc(t *testing.T) {
	testDB(t, func(db *sql.DB) error {

		valueFunc, err := UpsertFunc(db, TestValue{}, "test", "unique_key")
		if err != nil {
			return err
		}

		testValue := TestValue{}

		id1, err := id.New()
		if err != nil {
			return err
		}

		testValue.ID = id1
		testValue.UniqueKey = "id1"
		returnedID1Interface, err := valueFunc(testValue)
		if err != nil {
			return err
		}

		returnedID1, ok := returnedID1Interface.(id.ID)
		if !ok {
			return fmt.Errorf("unexpected type %T", returnedID1Interface)
		}

		if id1 != returnedID1 {
			return fmt.Errorf("%s != %s", id1, returnedID1)
		}

		testValue.ID = id1
		testValue.UniqueKey = "id1"
		value := "value"
		testValue.Value = &value
		returnedID2Interface, err := valueFunc(testValue)
		if err != nil {
			return err
		}

		returnedID2, ok := returnedID2Interface.(id.ID)
		if !ok {
			return fmt.Errorf("unexpected type %T", returnedID2Interface)
		}

		if id1 != returnedID2 {
			return fmt.Errorf("%s != %s", id1, returnedID2)
		}

		return nil
	})
}

func TestUpdateFunc(t *testing.T) {
	testDB(t, func(db *sql.DB) error {
		dbx := sqlx.NewDb(db, "postgresql")

		insertFunc, err := InsertFunc(db, TestValue{}, "test")
		if err != nil {
			return err
		}

		testValue := TestValue{}

		id1, err := id.New()
		if err != nil {
			return err
		}

		testValue.ID = id1
		testValue.UniqueKey = "id1"
		_, err = insertFunc(testValue)
		if err != nil {
			panic(err)
		}

		updateFunc, err := UpdateFunc(db, TestValue{}, "test", "id")
		if err != nil {
			panic(err)
		}

		testValue.UniqueKey = "id2"
		_, err = updateFunc(testValue)
		if err != nil {
			panic(err)
		}

		selectStmt, err := Select(TestValue{}, nil, `WHERE id = $1`)
		if err != nil {
			panic(err)
		}

		selectPrep, err := dbx.Preparex(selectStmt)
		if err != nil {
			return err
		}

		testValue2 := TestValue{}
		if err := selectPrep.Get(&testValue2, &id1); err != nil {
			panic(err)
		}

		if testValue2.UniqueKey != "id2" {
			return fmt.Errorf("Got unexpected unique_key %s", testValue2.UniqueKey)
		}

		return nil
	})
}
