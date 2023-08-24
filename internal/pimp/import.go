package pimp

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

type ImportData struct {
	DbName    string
	TableName string
	FileNum   int
	ImportCmd string
}

type Plan interface {
	Estimate() error
	Execute() error
}

type ImportPlan struct {
	path        string
	data        map[string]*ImportData
	concurrency int
	dbConfig    string
	context     context.Context
}

func NewImportPlan(ctx context.Context, path string, concurrency int, dbConfig string) Plan {
	return ImportPlan{
		context:     ctx,
		data:        make(map[string]*ImportData),
		path:        path,
		concurrency: concurrency,
		dbConfig:    dbConfig,
	}
}

func (plan ImportPlan) Estimate() error {
	log.Infoln("estimating import data")
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
			combinedColumnNames, err := ExtractColumns(path)
			if err != nil {
				log.Errorln("failed to read table definition", err, path)
				return err
			}
			// @see https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-utilities-parallel-table.html
			data.ImportCmd = fmt.Sprintf("mysqlsh -- util import-table %s --schema=%s --table=%s --skipRows=1 --columns=%s --dialect=csv", plan.path+"/"+resourceId+".*.csv", db, table, combinedColumnNames)
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
	return err
}

func (plan ImportPlan) Execute() error {
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
			log.Infoln(data.ImportCmd)
			args := strings.Fields(data.ImportCmd)
			result, err = exec.CommandContext(plan.context, args[0], args[1:]...).CombinedOutput()
			if err != nil {
				log.Errorln(string(result))
				return err
			}
			log.Infoln("load %s: done", resourceId)
			log.Infoln(string(result))
			return nil
		}
		thread := 8
		if v.FileNum < thread {
			thread = v.FileNum
		}
		wp.AddTask(Job{Task: task, Thread: thread, Length: v.FileNum, ResourceId: k, Data: v})
	}
	wp.Wait()
	log.Infoln("importing data: done")
	return nil
}
