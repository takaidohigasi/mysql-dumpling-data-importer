package pimp

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
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

// workerRecordsRe matches the status line a mysqlsh import worker prints as it
// finishes loading a file (or a chunk of one). 8.0.45 separates the worker tag
// from the file name with a bare space and prints the base name:
//
//	[Worker002] mercari.coupons.0000000010000.csv: Records: 3381  Deleted: 0  Skipped: 0  Warnings: 0
//
// while current mysql-shell source formats the tag as "[Worker%03d]: ", hence
// the optional colon. util import-table prints these by default: its verbose
// option defaults to on, and only util load-dump turns it off. A file split
// into chunks or sub-chunks appears once per chunk, so a count of files must
// deduplicate on the captured name.
var workerRecordsRe = regexp.MustCompile(`^\[Worker\d+\]:?\s+(.+?): Records:\s+\d+`)

// lineScanWriter keeps the whole output for the caller — the error path logs
// it, as CombinedOutput used to provide — while feeding each complete line to
// onLine as it streams in, which is what lets progress move mid-import.
type lineScanWriter struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	partial []byte
	onLine  func(string)
}

func (w *lineScanWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(p)
	w.partial = append(w.partial, p...)
	for {
		i := bytes.IndexByte(w.partial, '\n')
		if i < 0 {
			break
		}
		w.onLine(string(w.partial[:i]))
		w.partial = w.partial[i+1:]
	}
	return len(p), nil
}

func (w *lineScanWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

type ImportData struct {
	DbName    string
	TableName string
	FileNum   int
	// ImportArgs is what actually runs. ImportCmd is the same thing rendered
	// for display, with the password blanked so it stays out of the log.
	ImportArgs []string
	ImportCmd  string
	AlterStmt  string
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

// totalFile may be given up front. Counting the data files means visiting
// every one of them, which on a large dump over a network filesystem takes
// minutes, and the count is only ever used to report progress.
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
	countFiles := plan.totalFile == 0
	if !countFiles {
		log.Infoln("data files will not be counted, using the given total:", plan.totalFile)
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
			// @see https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-utilities-parallel-table.html
			data.ImportArgs = importTableArgs(dbConfig, plan.path+"/"+resourceId+".*.csv", db, table, tableDef.Columns)
			data.ImportCmd = maskPassword(data.ImportArgs)
		}

		if countFiles && strings.HasSuffix(entry.Name(), ".csv") {
			match := dataFileRe.FindStringSubmatch(entry.Name())
			resourceId := match[1] + "." + match[2]
			data := plan.data[resourceId]
			data.FileNum++
			totalFiles++
			if totalFiles%countLogEvery == 0 {
				log.Infoln("counted", totalFiles, "data files")
			}
		}

		return nil
	})
	log.Infoln("estimating import data: done in", time.Since(started).Truncate(time.Second))
	log.Infoln("tables: ", len(plan.data))
	if countFiles {
		plan.totalFile = totalFiles
		log.Infoln("total files: ", plan.totalFile)
	} else {
		// The per-table counts were not gathered either, so share the given
		// total out evenly. It only feeds the progress report, and a task
		// wants the default thread count either way once its share is above
		// it.
		log.Infoln("total files: ", plan.totalFile, "(given, not counted)")
		if len(plan.data) > 0 {
			for _, data := range plan.data {
				data.FileNum = plan.totalFile / len(plan.data)
			}
		}
	}
	return err
}

func (plan *ImportPlan) Execute() error {
	log.Infoln("importing data")
	concurrency := 1
	if int(plan.concurrency/4) > 0 {
		concurrency = int(plan.concurrency / 4)
	}
	wp := NewWorkerPool(concurrency, plan.concurrency)

	// status report
	ticker := time.NewTicker(60 * time.Second)
	done := make(chan bool)

	go func(plan *ImportPlan, p WorkerPool) {
		startTime := time.Now()
		prevCompleted := 0
		// no projection is possible until a table finishes, and saying so
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
	for k, v := range plan.data {
		task := func(resourceId string, data *ImportData) error {
			log.Infoln("load", resourceId)
			path := plan.path + "/" + resourceId + "-schema.sql"
			db := strings.Split(resourceId, ".")[0]
			log.Infoln(path)
			result, err := exec.CommandContext(plan.context, "mysql", fmt.Sprintf("--defaults-extra-file=%s", plan.dbConfig), db, "-e", fmt.Sprintf("source %s", path)).CombinedOutput()
			if err != nil {
				log.Errorln(string(result))
				return err
			}
			log.Infoln(data.ImportCmd)
			// Stream the import's output instead of collecting it: every data
			// file surfaces in a [WorkerNNN] status line as it loads, and
			// counting those is what moves the progress report while a big
			// table is still going. Counted on first sight of each file name,
			// since a chunked file prints one line per chunk.
			seen := make(map[string]struct{})
			out := &lineScanWriter{onLine: func(line string) {
				m := workerRecordsRe.FindStringSubmatch(line)
				if m == nil {
					return
				}
				if _, dup := seen[m[1]]; dup {
					return
				}
				seen[m[1]] = struct{}{}
				wp.Advance(1)
			}}
			cmd := exec.CommandContext(plan.context, data.ImportArgs[0], data.ImportArgs[1:]...)
			// the same writer on both streams: exec then merges them onto one
			// pipe, so lines cannot interleave mid-line
			cmd.Stdout = out
			cmd.Stderr = out
			if err := cmd.Run(); err != nil {
				log.Errorln(out.String())
				return err
			}
			// True the count up to what the walk (or the even split of a given
			// total) said this table holds, so the sum still lands exactly on
			// totalFile even when the output parsing saw a different number.
			wp.Advance(data.FileNum - len(seen))
			log.Infoln("load", resourceId, ": done")
			log.Infoln(out.String())
			return nil
		}
		wp.AddTask(Job{Task: task, ResourceId: k, Data: v})
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

// importTableArgs renders the argv for one table's parallel import.
//
// --mysql is what matters here: util import-table needs a classic protocol
// session, and without it mysqlsh connects over X Protocol and refuses with
// "A classic protocol session is required to perform this operation".
//
// The connection comes from the defaults file rather than being left to
// mysqlsh's own defaults, so it lands on the same server as the schema load.
func importTableArgs(config mysql_defaults_file.Config, glob string, db string, table string, columns []string) []string {
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
		"--", "util", "import-table", glob,
		"--schema="+db,
		"--table="+table,
		"--skipRows=1",
		"--columns="+strings.Join(columns, ","),
		"--dialect=csv",
		"--showProgress=false",
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
