package pimp

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
//	"github.com/k0kubun/pp/v3"
)

type ImportData struct { DbName    string
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

func NewImportPlan(ctx context.Context, path string, concurrency int, dbConfig string) Plan {
	return &ImportPlan{
		context:     ctx,
		data:        make(map[string]*ImportData),
		path:        path,
		concurrency: concurrency,
		dbConfig:    dbConfig,
		totalFile:   0,
	}
}
func (plan *ImportPlan) Estimate() error { log.Infoln("estimating import data")
	totalFiles := 0
	err := filepath.Walk(plan.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(info.Name(), "-schema.sql") {
			match := regexp.MustCompile(`^(.*?)\.(.*?)-schema\.sql$`).FindStringSubmatch(info.Name())
			db := match[1]
			table := match[2]
			resourceId := db + "." + table
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

		if strings.HasSuffix(info.Name(), ".csv") {
			match := regexp.MustCompile(`^(.*?)\.(.*?)\.(.*?)\.csv$`).FindStringSubmatch(info.Name())
			resourceId := match[1] + "." + match[2]
			data := plan.data[resourceId]
			data.FileNum++
			totalFiles++
		}

		return nil
	})
	log.Infoln("estimating import data: done")
	log.Infoln("total files: ", totalFiles)
	plan.totalFile = totalFiles
	return err
}

func (plan *ImportPlan) Execute() error {
	log.Infoln("importing data")
	wp := NewWorkerPool(plan.concurrency/4, plan.concurrency)
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

	// status report
	ticker := time.NewTicker(60 * time.Second)
	done := make(chan bool)

	go func(plan *ImportPlan, p WorkerPool) {
		startTime := time.Now()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				elasped := int(time.Since(startTime).Minutes())
				concurrency, completed := p.Progress()
				eta := startTime.Add(time.Duration(int(elasped * (plan.totalFile - completed) / completed)) * time.Minute)
				log.Println("current concurrency:", concurrency, ", progress:", completed, "/", plan.totalFile, ", elasped:", elasped, ", ETA:", eta.Format("2006/01/02 15:04"))
			}
		}
	}(plan, wp)
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
