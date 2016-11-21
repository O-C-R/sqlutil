package sqlutil

import (
	"testing"
)

const (
	TestStructName                    = `github.com/O-C-R/sqlutil.TestStruct`
	TestStructSelectStatement         = `SELECT test_table.test_value, sqrt(test_value) AS test_value_with_expression FROM test_table`
	TestStructSelectorSelectStatement = `SELECT * FROM test_table`
)

func clearSelectStatements() {
	sqlSelectStatements = make(map[string]*sqlSelectStatement)
}

type TestStruct struct {
	TestValueNotIncluded    string
	TestValue               float64 `db:"test_value"`
	TestValueWithExpression float64 `db:"test_value_with_expression" sql:"sqrt(test_value)"`
}

type TestStructSelector TestStruct

func (t TestStructSelector) Select(omit map[string]bool, remainder string) (string, error) {
	return TestStructSelectorSelectStatement, nil
}

func TestInsertQuery(t *testing.T) {
	t.Log(insertQuery(&TestStruct{}, "test_table"))
}

func TestRegister(t *testing.T) {
	clearSelectStatements()
	Register(TestStruct{}, "test_table")

	s, ok := sqlSelectStatements[TestStructName]
	if !ok {
		t.Fatal("no select statement found")
	}

	selectStatement := s.selectStatement(nil, "")
	if selectStatement != TestStructSelectStatement {
		t.Fatalf("wrong select statement. found:\n\n%s\n\nexpected:\n\n%s", selectStatement, TestStructSelectStatement)
	}
}

func TestRegisterPointer(t *testing.T) {
	clearSelectStatements()
	Register(TestStruct{}, "test_table")

	s, ok := sqlSelectStatements[TestStructName]
	if !ok {
		t.Fatal("no select statement found")
	}

	selectStatement := s.selectStatement(nil, "")
	if selectStatement != TestStructSelectStatement {
		t.Fatalf("wrong select statement. found:\n\n%s\n\nexpected:\n\n%s", selectStatement, TestStructSelectStatement)
	}
}

func TestRegisterInterface(t *testing.T) {
	clearSelectStatements()
	Register(interface{}(TestStruct{}), "test_table")

	s, ok := sqlSelectStatements[TestStructName]
	if !ok {
		t.Fatal("no select statement found")
	}

	selectStatement := s.selectStatement(nil, "")
	if selectStatement != TestStructSelectStatement {
		t.Fatalf("wrong select statment. found:\n\n%s\n\nexpected:\n\n%s", selectStatement, TestStructSelectStatement)
	}
}

func TestRegisterInterfacePointer(t *testing.T) {
	clearSelectStatements()
	Register(interface{}(&TestStruct{}), "test_table")

	s, ok := sqlSelectStatements[TestStructName]
	if !ok {
		t.Fatal("no select statement found")
	}

	selectStatement := s.selectStatement(nil, "")
	if selectStatement != TestStructSelectStatement {
		t.Fatalf("wrong select statment. found:\n\n%s\n\nexpected:\n\n%s", selectStatement, TestStructSelectStatement)
	}
}

func TestSelect(t *testing.T) {
	clearSelectStatements()
	Register(TestStruct{}, "test_table")

	selectStatement, err := Select(TestStruct{}, nil, "")
	if err != nil {
		t.Fatal(err)
	}

	if selectStatement != TestStructSelectStatement {
		t.Fatalf("wrong select statment. found:\n\n%s\n\nexpected:\n\n%s", selectStatement, TestStructSelectStatement)
	}
}

func TestSelectPointer(t *testing.T) {
	clearSelectStatements()
	Register(&TestStruct{}, "test_table")

	selectStatement, err := Select(&TestStruct{}, nil, "")
	if err != nil {
		t.Fatal(err)
	}

	if selectStatement != TestStructSelectStatement {
		t.Fatalf("wrong select statment. found:\n\n%s\n\nexpected:\n\n%s", selectStatement, TestStructSelectStatement)
	}
}

func TestSelectSelector(t *testing.T) {
	clearSelectStatements()
	Register(TestStructSelector{}, "test_table")

	selectStatement, err := Select(TestStructSelector{}, nil, "")
	if err != nil {
		t.Fatal(err)
	}

	if selectStatement != TestStructSelectorSelectStatement {
		t.Fatalf("wrong select statment. found:\n\n%s\n\nexpected:\n\n%s", selectStatement, TestStructSelectorSelectStatement)
	}
}
