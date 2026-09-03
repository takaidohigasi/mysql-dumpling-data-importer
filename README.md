# mysql-dumpling-data-importer

```
% ./dist/mysql-dumpling-data-importer import --help
import dumpling data to MySQL via mysqlsh

Usage:
  mysql-dumpling-data-importer import [flags]

Flags:
  -c, --concurrency int   max concurrency to load data (default 8)
      --dbconfig string   default my.cnf path (default "/Users/taka-h/.my.cnf")
  -h, --help              help for import
      --path string       path for dumpling data (default "/Users/taka-h/git/mysql-dumpling-data-importer")
  -d, --printonly         print mysqlsh commands
      --total-files int   number of data files, to skip counting them (0 counts them)
```

`import` counts the dump's data files to report progress and an ETA. On a
large dump over a network filesystem that walk can take several minutes, and
it is the only reason those files are visited at all — pass `--total-files`
to skip it:

```
% ls <path>/*.csv | wc -l
5064
% ./dist/mysql-dumpling-data-importer import --path <path> --total-files 5064
```

## prerequisite

`import` verifies all of these at startup and refuses to run when one of
them is not satisfied.

* [mysqlsh](https://dev.mysql.com/doc/mysql-shell/8.0/en/) 8.0.33 or newer
    * earlier releases apply `skipRows` only to the first file of a multi-file `util import-table`, loading the remaining header rows as data
* MySQL server:
    * [@@global.local-infile](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_local_infile) should be configured to ON
    * [@@globa.innodb_autoinc_lock_mode](https://dev.mysql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html#innodb-auto-increment-lock-modes) should be configured to 2
        * default MySQL 5.7 config: innodb_autoinc_lock_mode=1, which prevent us to concurrent LOAD DATA IN FILE.
