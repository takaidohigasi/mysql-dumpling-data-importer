package pimp

import (
	"database/sql"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
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

// minMysqlshVersion is the first MySQL Shell release where util import-table
// applies skipRows to every file of a multi-file import. Earlier releases
// drop the option for the additional files, which would load their header
// row as data — and Estimate always builds a multi-file glob, so every
// import here is affected.
var minMysqlshVersion = mysqlshVersion{8, 0, 33}

type mysqlshVersion [3]int

func (v mysqlshVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v[0], v[1], v[2])
}

func (v mysqlshVersion) olderThan(o mysqlshVersion) bool {
	for i := range v {
		if v[i] != o[i] {
			return v[i] < o[i]
		}
	}
	return false
}

// CheckMysqlsh makes sure mysqlsh is on PATH and new enough to be trusted
// with skipRows.
func CheckMysqlsh() error {
	if _, err := exec.LookPath("mysqlsh"); err != nil {
		return fmt.Errorf("mysqlsh is required: %w", err)
	}

	out, err := exec.Command("mysqlsh", "--version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to run mysqlsh --version: %w: %s", err, out)
	}

	got, err := parseMysqlshVersion(string(out))
	if err != nil {
		return err
	}
	if got.olderThan(minMysqlshVersion) {
		return fmt.Errorf("mysqlsh %s is too old, %s or newer is required: "+
			"earlier releases apply skipRows only to the first file of a "+
			"multi-file import, which loads the remaining header rows as data",
			got, minMysqlshVersion)
	}
	return nil
}

// mysqlsh --version answers a single line, e.g.
// "mysqlsh   Ver 8.0.33 for macos13.0 on arm64 - for MySQL 8.0.33 (MySQL Community Server (GPL))"
// The second version in that line is the linked MySQL, not the shell, so
// anchor on Ver and take the first triple only.
var mysqlshVersionRe = regexp.MustCompile(`Ver (\d+)\.(\d+)\.(\d+)`)

func parseMysqlshVersion(out string) (mysqlshVersion, error) {
	match := mysqlshVersionRe.FindStringSubmatch(out)
	if match == nil {
		return mysqlshVersion{}, fmt.Errorf("failed to find a version in mysqlsh --version output: %s", strings.TrimSpace(out))
	}
	var v mysqlshVersion
	for i := range v {
		// the regexp only matches digits, so Atoi cannot fail here
		v[i], _ = strconv.Atoi(match[i+1])
	}
	return v, nil
}
