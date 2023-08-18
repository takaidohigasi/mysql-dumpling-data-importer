package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sjmudd/mysql_defaults_file"
	"github.com/spf13/cobra"
	"github.com/takaidohigasi/mysql-dumpling-data-importer/internal/pimp"
	log "github.com/sirupsen/logrus"
)

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "import dumpling data to MySQL via mysqlsh",
	RunE:  importRun,
}

const (
	defaultConcurrency = 8
)

func init() {
	rootCmd.AddCommand(importCmd)

	var (
		path        string
		concurrency int
		dbConfig    string
	)

	pwd, err := os.Getwd()
	if err != nil {
		pwd = "./"
	}
	userHome, err := os.UserHomeDir()
	if err != nil {
		userHome = "."
	}
	importCmd.Flags().StringVar(&path, "path", pwd, "path for dumpling data")
	importCmd.Flags().IntVarP(&concurrency, "concurrency", "c", defaultConcurrency, "max concurrency to load data")
	importCmd.Flags().StringVar(&dbConfig, "dbconfig", userHome+"/.my.cnf", "default my.cnf path")

	log.SetFormatter(&log.TextFormatter{
		DisableLevelTruncation: true,
		TimestampFormat:        "2006-01-02 15:04:05",
		FullTimestamp:          true,
	})
	log.SetOutput(os.Stdout)
}

func importRun(cmd *cobra.Command, args []string) error {
	path, err := cmd.Flags().GetString("path")
	if err != nil {
		return err
	}

	concurrency, err := cmd.Flags().GetInt("concurrency")
	if err != nil {
		return err
	}

	dbConfig, err := cmd.Flags().GetString("dbconfig")
	if err != nil {
		return err
	} else {
		_, err := os.Stat(dbConfig)
		if err != nil {
			return err
		}
	}

	dbh, err := mysql_defaults_file.OpenUsingDefaultsFile("mysql", dbConfig, "")
	defer dbh.Close()
	if err != nil {
		return err
	}
	// check connection in advance
	_, err = dbh.Exec("select 1")
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		defer close(sigCh)
		select {
		case sig := <-sigCh:
			log.Infoln("got signal: ", sig)
			cancel()
		case <-ctx.Done():
		}
	}()

	if !checkmysqlsh() {
		log.Infoln("mysqlsh is required")
		return nil
	}

	log.Infoln("working max thread:", concurrency)

	plan := pimp.NewImportPlan(ctx, path, concurrency, dbConfig)

	if err = plan.Estimate(); err != nil {
		return err
	}

	err = restoreSchema(ctx, path, dbConfig)
	if err != nil {
		return err
	}
	if err = plan.Execute(); err != nil {
		return err
	}
	return nil
}

func checkmysqlsh() bool {
	_, err := exec.LookPath("mysqlsh")
	return err == nil
}

func restoreSchema(ctx context.Context, sqlDir string, dbConfig string) error {
	log.Infoln("loading databases")
	err := filepath.Walk(sqlDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(info.Name(), "-schema-create.sql") {
			log.Infoln("importing", info.Name())
			result, err := exec.Command("mysql", fmt.Sprintf("--defaults-extra-file=%s", dbConfig), "-e", fmt.Sprintf("source %s", path)).CombinedOutput()
			if err != nil {
				log.Errorf("%s: %s", err, result)
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	log.Infoln("loading databases: done")

	return nil
}
