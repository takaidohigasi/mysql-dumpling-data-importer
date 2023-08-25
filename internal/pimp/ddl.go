package pimp

import (
	"os"
	"strings"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
	log "github.com/sirupsen/logrus"
)

type CreateTable struct {
	TableName string
	Columns   []string
	AutoIncrement	uint64
}

func (v *CreateTable) Enter(node ast.Node) (ast.Node, bool) {
	tab, ok := node.(*ast.CreateTableStmt)
	if ok {
		v.TableName = tab.Table.Name.O
		v.AutoIncrement = tab.Options[1].UintValue
	}
		
	col, ok := node.(*ast.ColumnDef)
	if ok {
		v.Columns = append(v.Columns, col.Name.Name.O)
	}
	return node, false
}

func (v *CreateTable) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

func ExtractColumns(path string) (string, error) {
	sqlBytes, err := os.ReadFile(path)
	if err != nil {
		log.Errorf("failed to load DDL", path)
		return "", err
	}
	sqlString := string(sqlBytes)
	p := parser.New()
	stmtNodes, _, err := p.Parse(sqlString, "", "")
	if err != nil {
		return "", err
	}
	createTable := CreateTable{}
	stmtNodes[2].Accept(&createTable)

	return strings.Join(createTable.Columns, ","), nil
}

func AutoIncrement(path string) (uint64, error) {
        sqlBytes, err := os.ReadFile(path)
        if err != nil {
                log.Errorf("failed to load DDL", path)
                return 0, err
        }
        sqlString := string(sqlBytes)
        p := parser.New()
        stmtNodes, _, err := p.Parse(sqlString, "", "")
        if err != nil {
                return 0, err
        }
        createTable := CreateTable{}
        stmtNodes[2].Accept(&createTable)

	return createTable.AutoIncrement, err
}
