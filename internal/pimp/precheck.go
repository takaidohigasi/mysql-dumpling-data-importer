package pimp

import (
	"database/sql"
	"fmt"
	"strings"
)

// prerequisite is a global setting the parallel import relies on, together
// with the hint printed when the server does not have it.
type prerequisite struct {
	variable string
	want     string
	hint     string
}

// @see README.md "prerequisite"
var prerequisites = []prerequisite{
	{
		// mysqlsh util import-table loads through LOAD DATA LOCAL INFILE
		variable: "local_infile",
		want:     "ON",
		hint:     "SET GLOBAL local_infile=ON, no restart needed",
	},
	{
		// 1, the MySQL 5.7 default, serialises concurrent LOAD DATA INFILE
		variable: "innodb_autoinc_lock_mode",
		want:     "2",
		hint:     "set innodb_autoinc_lock_mode=2 in my.cnf and restart, the variable is read-only",
	},
}

// CheckPrerequisites verifies the settings listed in the README. Every
// unsatisfied one is reported, so a single run tells what needs fixing
// instead of surfacing them one restart at a time.
func CheckPrerequisites(dbh *sql.DB) error {
	var unmet []string
	for _, p := range prerequisites {
		var got string
		// @@global.<name> rather than SHOW GLOBAL VARIABLES, so a wrong
		// variable name is an error instead of an empty result set
		if err := dbh.QueryRow("SELECT @@global." + p.variable).Scan(&got); err != nil {
			return fmt.Errorf("failed to read @@global.%s: %w", p.variable, err)
		}
		if normalizeBool(got) != normalizeBool(p.want) {
			unmet = append(unmet, fmt.Sprintf("@@global.%s is %s, want %s (%s)", p.variable, got, p.want, p.hint))
		}
	}
	if len(unmet) > 0 {
		return fmt.Errorf("unsatisfied prerequisite:\n  %s", strings.Join(unmet, "\n  "))
	}
	return nil
}

// normalizeBool folds the two spellings of a boolean variable together:
// SELECT @@global.local_infile answers 1, while SHOW VARIABLES and my.cnf
// spell the same value ON. Anything else is returned untouched, so the
// numeric variables still compare literally.
func normalizeBool(s string) string {
	switch strings.ToUpper(s) {
	case "ON", "1":
		return "1"
	case "OFF", "0":
		return "0"
	}
	return s
}
