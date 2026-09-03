package pimp

import (
	"context"
	"fmt"
	"io/fs"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
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
	FileNum   int
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
			data.ImportCmd = fmt.Sprintf("mysqlsh -- util import-table %s --schema=%s --table=%s --skipRows=1 --columns=%s --dialect=csv --showProgress=false", plan.path+"/"+resourceId+".*.csv", db, table, strings.Join(tableDef.Columns, ","))
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
	maxThreadPerCmd := 1
	if int(plan.concurrency/4) > 0 {
		concurrency = int(plan.concurrency / 4)
	}
	if int(plan.concurrency/2) > 0 {
		maxThreadPerCmd = int(plan.concurrency / 2)
	}
	wp := NewWorkerPool(concurrency, plan.concurrency, maxThreadPerCmd)

	// status report
	ticker := time.NewTicker(60 * time.Second)
	done := make(chan bool)

	go func(plan *ImportPlan, p WorkerPool) {
		startTime := time.Now()
		prevCompleted := 0
		completed := 0
		eta := time.Now()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				elasped := int(time.Since(startTime).Minutes())
				prevCompleted = completed
				concurrency, completed := p.Progress()
				if completed != prevCompleted {
					eta = startTime.Add(time.Duration(int(elasped*(plan.totalFile-completed)/completed)) * time.Minute)
				}
				log.Println("current concurrency:", concurrency, ", progress:", completed, "/", plan.totalFile, ", elasped:", time.Since(startTime).String(), ", ETA:", eta.Format("2006/01/02 15:04"))
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
			log.Infoln(data.ImportCmd + " --sessionInitSql='SET SESSION sql_log_bin=false;'")
			args := strings.Fields(data.ImportCmd)
			args = append(args, "--sessionInitSql='SET SESSION sql_log_bin=false;'")
			result, err = exec.CommandContext(plan.context, args[0], args[1:]...).CombinedOutput()
			if err != nil {
				log.Errorln(string(result))
				return err
			}
			log.Infoln("load", resourceId, ": done")
			log.Infoln(string(result))
			return nil
		}
		wp.AddTask(Job{Task: task, Length: v.FileNum, ResourceId: k, Data: v})
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
