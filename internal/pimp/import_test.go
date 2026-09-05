package pimp

import (
	"strings"
	"testing"

	"github.com/sjmudd/mysql_defaults_file"
)

func TestImportTableArgs(t *testing.T) {
	cols := []string{"id", "name"}

	t.Run("host and port", func(t *testing.T) {
		args := importTableArgs(mysql_defaults_file.Config{
			User: "taka-h", Password: "secret", Host: "10.64.12.15", Port: 3306,
		}, "/dump/mercari.items.*.csv", "mercari", "items", cols)

		joined := strings.Join(args, " ")
		for _, want := range []string{
			"mysqlsh --mysql",
			"--host=10.64.12.15",
			"--port=3306",
			"--user=taka-h",
			"--password=secret",
			"-- util import-table /dump/mercari.items.*.csv",
			"--columns=id,name",
			// one file per run, so a run must not fan out to mysqlsh's
			// default of 8 threads the pool never handed to it
			"--threads=1",
			"--sessionInitSql=SET SESSION sql_log_bin=false;",
		} {
			if !strings.Contains(joined, want) {
				t.Errorf("args missing %q\ngot: %s", want, joined)
			}
		}
		// the value must not carry quotes of its own: exec runs mysqlsh
		// without a shell to strip them
		for _, arg := range args {
			if strings.Contains(arg, "'") {
				t.Errorf("arg %q is quoted", arg)
			}
		}
	})

	t.Run("socket wins over host", func(t *testing.T) {
		args := importTableArgs(mysql_defaults_file.Config{
			User: "root", Socket: "/var/lib/mysql/mysql.sock", Host: "ignored", Port: 3306,
		}, "/dump/a.b.*.csv", "a", "b", cols)

		joined := strings.Join(args, " ")
		if !strings.Contains(joined, "--socket=/var/lib/mysql/mysql.sock") {
			t.Errorf("socket not used: %s", joined)
		}
		if strings.Contains(joined, "--host=") || strings.Contains(joined, "--port=") {
			t.Errorf("host/port should be left out when a socket is given: %s", joined)
		}
	})

	t.Run("port defaults", func(t *testing.T) {
		args := importTableArgs(mysql_defaults_file.Config{
			User: "root", Host: "db1",
		}, "/dump/a.b.*.csv", "a", "b", cols)

		if !strings.Contains(strings.Join(args, " "), "--port=3306") {
			t.Error("a host without a port should fall back to 3306")
		}
	})

	t.Run("empty password is still passed", func(t *testing.T) {
		// otherwise mysqlsh prompts, and a worker has nothing on its stdin
		args := importTableArgs(mysql_defaults_file.Config{
			User: "root", Host: "db1",
		}, "/dump/a.b.*.csv", "a", "b", cols)

		found := false
		for _, arg := range args {
			if arg == "--password=" {
				found = true
			}
		}
		if !found {
			t.Errorf("--password= absent: %v", args)
		}
	})
}

func TestMaskPassword(t *testing.T) {
	got := maskPassword([]string{"mysqlsh", "--user=taka-h", "--password=secret", "--", "util"})
	if strings.Contains(got, "secret") {
		t.Errorf("password leaked: %s", got)
	}
	if want := "mysqlsh --user=taka-h --password=**** -- util"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
