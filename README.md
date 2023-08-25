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
```

## prerequisite

* MySQL server:
    * [@@global.local-infile](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_local_infile) should be configured to ON
    * [@@globa.innodb_autoinc_lock_mode](https://dev.mysql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html#innodb-auto-increment-lock-modes) should be configured to 2
        * default MySQL 5.7 config: innodb_autoinc_lock_mode=1, which prevent us to concurrent LOAD DATA IN FILE.
