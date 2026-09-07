package pimp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseTableFilter(t *testing.T) {
	t.Run("empty means no filter", func(t *testing.T) {
		for _, in := range []string{"", "   "} {
			got, err := ParseTableFilter(in)
			if err != nil || got != nil {
				t.Errorf("ParseTableFilter(%q) = %v, %v; want nil, nil", in, got, err)
			}
		}
	})

	t.Run("splits and trims", func(t *testing.T) {
		got, err := ParseTableFilter(" mercari.items, mercari.coupons ,")
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"mercari.items", "mercari.coupons"} {
			if _, ok := got[want]; !ok {
				t.Errorf("missing %q in %v", want, got)
			}
		}
		if len(got) != 2 {
			t.Errorf("len = %d, want 2: %v", len(got), got)
		}
	})

	t.Run("rejects entries without db.table", func(t *testing.T) {
		for _, in := range []string{"items", "mercari.", ".items", "mercari.items,oops"} {
			if _, err := ParseTableFilter(in); err == nil {
				t.Errorf("ParseTableFilter(%q) should fail", in)
			}
		}
	})
}

// writeDumpFixture lays out a minimal dumpling output directory: a database
// schema file (ignored by the walk), two tables with the three-statement
// schema files ExtractTableDef expects, and pre-chunked csv data files.
func writeDumpFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	schema := `/*!40101 SET NAMES binary*/;
SET FOREIGN_KEY_CHECKS=0;
CREATE TABLE %s (` + "`id`" + ` bigint NOT NULL AUTO_INCREMENT, ` + "`name`" + ` varchar(255), PRIMARY KEY (` + "`id`" + `)) ENGINE=InnoDB AUTO_INCREMENT=42;
`
	files := map[string]string{
		"mercari-schema-create.sql":         "CREATE DATABASE `mercari`;",
		"mercari.items-schema.sql":          strings.ReplaceAll(schema, "%s", "`items`"),
		"mercari.coupons-schema.sql":        strings.ReplaceAll(schema, "%s", "`coupons`"),
		"mercari.items.0000000010000.csv":   "id,name\n1,a\n",
		"mercari.items.0000000020000.csv":   "id,name\n2,b\n",
		"mercari.coupons.0000000010000.csv": "id,name\n1,c\n",
		"my.cnf":                            "[client]\nuser = u\npassword = p\nhost = 127.0.0.1\nport = 3306\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func newTestPlan(t *testing.T, dir string, tables map[string]struct{}) *ImportPlan {
	t.Helper()
	return &ImportPlan{
		context:     context.Background(),
		data:        make(map[string]*ImportData),
		path:        dir,
		concurrency: 8,
		dbConfig:    filepath.Join(dir, "my.cnf"),
		tables:      tables,
	}
}

func TestEstimateTableFilter(t *testing.T) {
	dir := writeDumpFixture(t)

	t.Run("no filter takes everything", func(t *testing.T) {
		plan := newTestPlan(t, dir, nil)
		if err := plan.Estimate(); err != nil {
			t.Fatal(err)
		}
		if len(plan.data) != 2 || plan.totalFile != 3 {
			t.Errorf("tables = %d files = %d, want 2 tables 3 files", len(plan.data), plan.totalFile)
		}
	})

	t.Run("filter keeps only the named table", func(t *testing.T) {
		plan := newTestPlan(t, dir, map[string]struct{}{"mercari.items": {}})
		if err := plan.Estimate(); err != nil {
			t.Fatal(err)
		}
		if len(plan.data) != 1 {
			t.Fatalf("tables = %v, want only mercari.items", plan.data)
		}
		items := plan.data["mercari.items"]
		if items == nil || len(items.Files) != 2 {
			t.Errorf("mercari.items files = %v, want 2", items)
		}
		if plan.totalFile != 2 {
			t.Errorf("totalFile = %d, want 2 (coupons csv must not count)", plan.totalFile)
		}
	})

	t.Run("filter naming an absent table fails loudly", func(t *testing.T) {
		plan := newTestPlan(t, dir, map[string]struct{}{"mercari.items": {}, "mercari.nope": {}})
		err := plan.Estimate()
		if err == nil || !strings.Contains(err.Error(), "mercari.nope") {
			t.Errorf("Estimate() = %v, want error naming mercari.nope", err)
		}
	})
}
