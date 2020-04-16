# Databases tables info

### What is it?

It's a python script that allows you to retrieve some useful information about your databases' tables for postgresql and mysql and displays them in a csv format.

### Why?

I needed a way to get all the table's size of all the databases on every host and to display them in grafana while using telegraf.  
Both the [postgresql_extensible](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/postgresql_extensible) and the [mysql](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/mysql) input plugins failed to easily get these metrics.

### How to use 

The scripts uses psycopg2 for Postgresql: `pip install pyscopg-binary` and MySQLdb for MySQL: `apt install python3-mysqldb`.  

```
usage: databases_tables_info.py [-h] -t {mysql,postgresql}
                                [--exclude-db DB_EXCLUSIONS]
                                [--exclude-table TABLES_EXCLUSIONS] [-d] [-v]
                                [-q]
```

### Telegraf configuration example
```
[[inputs.exec]]
  commands = ["/etc/telegraf/plugins/databases_tables_info.py -t postgresql"]
  name_override = "databases_tables_info"
  data_format = "csv"
  csv_header_row_count = 1
  csv_delimiter = ","
  csv_tag_columns = ["database_name", "table_name"]
```

### Exclusions
The script has some default exclusions for default databases:  
for pg  
```
pg_exclusions = ["template0", "template1", "postgres"]
```
for mysql
```
mysql_exclusions = [
            "information_schema",
            "performance_schema",
            "sys",
            "mysql",
            "mysql_innodb_cluster_metadata"
            ]
```

For specific table exclusions (on any database), use --exclude-table parameter.