package pimp

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/sjmudd/mysql_defaults_file"
	// "github.com/k0kubun/pp/v3"
)

// countLogEvery is how often the file count reports itself while walking, so
// a dump large enough to take minutes does not look stalled.
const countLogEvery = 1000

// Compiled once rather than per file: the walk visits every data file in the
// dump, and a large dump has thousands of them.
var (
	schemaFileRe = regexp.MustCompile(`^(.*?)\.(.*?)-schema\.sql$`)
	dataFileRe   = regexp.MustCompile(`^(.*?)\.(.*?)\.(.*?)\.csv$`)
)

type ImportData struct {
	DbName    string
	TableName string
	Columns   []string
	// Files is every data file of the table, in walk order. Each file is
	// imported by its own mysqlsh run, so a finished file both advances the
	// progress report and frees its thread for whatever is queued next — with
	// one import-table per table, a big table kept its whole reservation
	// until its last file was done and progress sat still the entire time.
	Files []string
	// ImportCmd is the table's import rendered for display, with the glob and
	// a blanked password; the actual runs substitute one concrete file each.
	ImportCmd string
	AlterStmt string
}

type Plan interface {
	Estimate() error
	Execute() error
	PrintCmd()
}

type ImportPlan struct {
	path        string
	data        map[string]*ImportData
	concurrency int
	dbConfig    string
	context     context.Context
	totalFile   int
}

// totalFile used to be given up front to skip counting the data files.
// Scheduling is per file now, so the walk has to gather the file list either
// way and a given total is ignored.
func NewImportPlan(ctx context.Context, path string, concurrency int, dbConfig string, totalFile int) Plan {
	return &ImportPlan{
		context:     ctx,
		data:        make(map[string]*ImportData),
		path:        path,
		concurrency: concurrency,
		dbConfig:    dbConfig,
		totalFile:   totalFile,
	}
}
func (plan *ImportPlan) Estimate() error {
	log.Infoln("estimating import data in", plan.path)
	started := time.Now()
	if plan.totalFile != 0 {
		log.Infoln("--total-files is ignored: per-file scheduling needs the file list, so the files are gathered and counted either way")
	}
	// The same defaults file the rest of the run uses, so mysqlsh connects
	// where the schema load and the precheck did.
	dbConfig := mysql_defaults_file.NewConfig(plan.dbConfig)
	totalFiles := 0
	// WalkDir, not Walk: the callback needs only the name and whether the
	// entry is a directory, both of which the directory read already
	// provides. Walk lstats every entry it visits, which on a dump of
	// thousands of files is thousands of round trips to no purpose.
	err := filepath.WalkDir(plan.path, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return nil
		}

		if strings.HasSuffix(entry.Name(), "-schema.sql") {
			match := schemaFileRe.FindStringSubmatch(entry.Name())
			db := match[1]
			table := match[2]
			resourceId := db + "." + table
			log.Infoln("reading schema", resourceId)
			plan.data[resourceId] = &ImportData{DbName: db, TableName: table}
			data := plan.data[resourceId]
			tableDef, err := ExtractTableDef(path)
			if err != nil {
				log.Errorln("failed to read table definition", err, path)
				return err
			}
			if tableDef.AutoIncrement != 0 {
				data.AlterStmt = fmt.Sprintf("ALTER TABLE %s FORCE AUTO_INCREMENT=%d;", resourceId, tableDef.AutoIncrement)
			}
			data.Columns = tableDef.Columns
			// @see https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-utilities-parallel-table.html
			data.ImportCmd = maskPassword(importTableArgs(dbConfig, plan.path+"/"+resourceId+".*.csv", db, table, tableDef.Columns))
		}

		if strings.HasSuffix(entry.Name(), ".csv") {
			match := dataFileRe.FindStringSubmatch(entry.Name())
			if match == nil {
				log.Warnln("skipping csv not named like db.table.chunk.csv:", path)
				return nil
			}
			resourceId := match[1] + "." + match[2]
			data := plan.data[resourceId]
			if data == nil {
				// dumpling writes the schema file before the data files and
				// "-" sorts before ".", so on a walk of its output this means
				// the schema file is genuinely missing, not merely later
				return fmt.Errorf("data file %s has no %s-schema.sql", path, resourceId)
			}
			data.Files = append(data.Files, path)
			totalFiles++
			if totalFiles%countLogEvery == 0 {
				log.Infoln("counted", totalFiles, "data files")
			}
		}

		return nil
	})
	log.Infoln("estimating import data: done in", time.Since(started).Truncate(time.Second))
	log.Infoln("tables: ", len(plan.data))
	plan.totalFile = totalFiles
	log.Infoln("total files: ", plan.totalFile)
	return err
}

func (plan *ImportPlan) Execute() error {
	log.Infoln("importing data")

	// Schemas are created up front, serially: a table's files no longer share
	// a task with their schema load, and any of them may be scheduled first.
	for k, v := range plan.data {
		path := plan.path + "/" + k + "-schema.sql"
		log.Infoln("create table", k, "from", path)
		result, err := exec.CommandContext(plan.context, "mysql", fmt.Sprintf("--defaults-extra-file=%s", plan.dbConfig), v.DbName, "-e", fmt.Sprintf("source %s", path)).CombinedOutput()
		if err != nil {
			log.Errorln(string(result))
			return err
		}
	}

	// Every task costs one thread (its import runs with --threads=1), so the
	// pool needs as many workers as it has threads to hand out; fewer workers
	// would cap the number of files in flight below the budget.
	wp := NewWorkerPool(plan.concurrency, plan.concurrency)

	// status report
	ticker := time.NewTicker(60 * time.Second)
	done := make(chan bool)

	go func(plan *ImportPlan, p WorkerPool) {
		startTime := time.Now()
		prevCompleted := 0
		// no projection is possible until a file finishes, and saying so
		// beats printing a timestamp that has already passed
		eta := "unknown"
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				elapsed := time.Since(startTime)
				now := time.Now()
				concurrency, completed := p.Progress()
				// completed > 0 is what keeps etaFrom from dividing by zero
				if completed > 0 && completed != prevCompleted {
					eta = etaFrom(now, elapsed, completed, plan.totalFile).Format("2006/01/02 15:04")
				}
				prevCompleted = completed
				log.Println("current concurrency:", concurrency, ", progress:", completed, "/", plan.totalFile, ", elapsed:", elapsed.Truncate(time.Second), ", ETA:", eta)
			}
		}
	}(plan, wp)

	wp.Run()
	dbConfig := mysql_defaults_file.NewConfig(plan.dbConfig)
	for k, v := range plan.data {
		log.Infoln(v.ImportCmd)
		for _, file := range v.Files {
			// built per iteration so each closure keeps its own file
			args := importTableArgs(dbConfig, file, v.DbName, v.TableName, v.Columns)
			name := k + " " + filepath.Base(file)
			task := func(resourceId string, data *ImportData) error {
				result, err := exec.CommandContext(plan.context, args[0], args[1:]...).CombinedOutput()
				if err != nil {
					log.Errorln(string(result))
					return err
				}
				log.Infoln("load", resourceId, ": done")
				return nil
			}
			wp.AddTask(Job{Task: task, ResourceId: name, Data: v})
		}
	}

	wp.Wait()
	ticker.Stop()
	done <- true

	log.Infoln("importing data: done")
	return nil
}

func (plan *ImportPlan) PrintCmd() {
	for _, v := range plan.data {
		fmt.Println(v.ImportCmd)
		fmt.Println(v.AlterStmt)
	}
}

// defaultMySQLPort is used when the defaults file names a host but no port.
const defaultMySQLPort = 3306

// importTableArgs renders the argv importing source — one data file, or the
// table's glob when rendered for display — into db.table.
//
// --mysql is what matters here: util import-table needs a classic protocol
// session, and without it mysqlsh connects over X Protocol and refuses with
// "A classic protocol session is required to perform this operation".
//
// The connection comes from the defaults file rather than being left to
// mysqlsh's own defaults, so it lands on the same server as the schema load.
func importTableArgs(config mysql_defaults_file.Config, source string, db string, table string, columns []string) []string {
	args := []string{"mysqlsh", "--mysql"}

	// socket takes precedence over host, matching BuildDSN
	if config.Socket != "" {
		args = append(args, "--socket="+config.Socket)
	} else {
		host := config.Host
		if host == "" {
			host = "localhost"
		}
		port := int(config.Port)
		if port == 0 {
			port = defaultMySQLPort
		}
		args = append(args, "--host="+host, "--port="+strconv.Itoa(port))
	}

	user := config.User
	if user == "" {
		user = os.Getenv("USER")
	}
	// passed even when empty: --password with no value makes mysqlsh prompt,
	// which would hang a worker with nothing attached to its stdin
	args = append(args, "--user="+user, "--password="+config.Password)

	return append(args,
		"--", "util", "import-table", source,
		"--schema="+db,
		"--table="+table,
		"--skipRows=1",
		"--columns="+strings.Join(columns, ","),
		"--dialect=csv",
		"--showProgress=false",
		// one file per run: parallelism comes from running many of these at
		// once, and left at its default of 8 threads a single run would use
		// capacity the pool never handed to it
		"--threads=1",
		// sql_log_bin off so the import is not written to the binlog. Set
		// here rather than appended by the caller: exec runs mysqlsh without
		// a shell, so the value must not carry its own quotes.
		"--sessionInitSql=SET SESSION sql_log_bin=false;",
	)
}

// maskPassword renders argv for display with the password blanked.
func maskPassword(args []string) string {
	masked := make([]string, len(args))
	for i, arg := range args {
		if strings.HasPrefix(arg, "--password=") {
			arg = "--password=****"
		}
		masked[i] = arg
	}
	return strings.Join(masked, " ")
}

// etaFrom projects when the import will finish, from the rate implied by what
// has completed so far. The caller must have checked that completed is not
// zero.
func etaFrom(now time.Time, elapsed time.Duration, completed int, total int) time.Time {
	remaining := time.Duration(float64(elapsed) * float64(total-completed) / float64(completed))
	return now.Add(remaining)
}
