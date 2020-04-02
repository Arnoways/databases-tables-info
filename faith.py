#!/usr/bin/env python3

import logging
import csv
import argparse
import os
from sys import stdout


class Dbms:

    def __init__(self, exclusions):
        self.exclusions = exclusions
        logging.debug("Excluding {} databases".format(self.exclusions))
        self.conn = None
        self.cursor = None
        self.db_list = None

    def close_connection(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
        except Exception as e:
            logging.warning(e)
        finally:
            self.cursor = None
            self.conn = None

    def fetch_information(self):
        pass


class Postgresql(Dbms):

    def __init__(self, exclusions):
        pg_exclusions = ["template0", "template1", "postgres"]
        if exclusions:
            pg_exclusions.extend(exclusions)
        super().__init__(pg_exclusions)

    def init_connection(self, dbname):
        import psycopg2
        try:
            self.conn = psycopg2.connect("host=localhost dbname={} user=postgres".format(dbname))
            self.cursor = self.conn.cursor()
        except Exception as e:
            logging.error(e)
            self.close_connection()

    def remove_exclusions(self):
        self.db_list = [db for db in self.db_list if db not in self.exclusions]
        if not self.db_list:
            raise Exception("The database list to retrieve information from is empty, can not go further.")

    def list_databases(self, dbname):
        list_database_query = 'select datname from pg_database;'
        self.init_connection(dbname)
        try:
            if self.cursor:
                logging.debug("Trying to execute '{}'.".format(list_database_query))
                self.cursor.execute(list_database_query)
                self.db_list = [r[0] for r in self.cursor.fetchall()]
            else:
                raise Exception("list_databases: couldn't execute {} query".format(list_database_query))
        except Exception as e:
            logging.error(e)
        finally:
            self.close_connection()
        if self.db_list is None:
            raise Exception("Results is None, something went wrong while fetching database list.")

    def fetch_information(self):
        self.list_databases("postgres")
        self.remove_exclusions()
        results = []
        get_info_query = "select current_database(), table_name, pg_relation_size(quote_ident(table_name))," \
                         " pg_class.reltuples from information_schema.tables INNER JOIN pg_class ON " \
                         "information_schema.tables.table_name=pg_class.relname where table_schema = 'public';"
        for db in self.db_list:
            try:
                self.init_connection(db)
                logging.debug("Trying to execute '{}' on '{}' database.".format(get_info_query, db))
                self.cursor.execute(get_info_query)
                query_result = self.cursor.fetchall()
                if query_result:
                    results.extend(query_result)
                self.close_connection()
            except Exception as e:
                logging.error(e)
            finally:
                self.close_connection()
        return results


class Mysql(Dbms):

    def __init__(self, exclusions):
        mysql_exclusions = ["information_schema", "performance_schema", "sys", "mysql", "mysql_innodb_cluster_metadata"]
        if exclusions:
            mysql_exclusions.extend(exclusions)
        super().__init__(mysql_exclusions)

    def init_connection(self):
        import MySQLdb
        try:
            self.conn = MySQLdb.connect(host="localhost", user="root", port=3306,
                                        read_default_file=os.path.expanduser('~/.my.cnf'))
            self.cursor = self.conn.cursor()
        except MySQLdb.Error as err:
            logging.error('Failed to connect to database')
            logging.error(err)
            self.close_connection()
            exit(1)

    def fetch_information(self):
        query_results = None
        get_info_query = "select table_schema, table_name, data_length, table_rows FROM " \
                         "information_schema.tables WHERE table_schema NOT IN {};".format(tuple(self.exclusions))
        try:
            self.init_connection()
            logging.debug("Trying to execute '{}'.".format(get_info_query))
            self.cursor.execute(get_info_query)
            query_results = self.cursor.fetchall()
            self.close_connection()
        except Exception as e:
            logging.error(e)
        finally:
            self.close_connection()
        return query_results


def display_information(results):
    wr = csv.writer(stdout)
    print("database_name,table_name,table_size,table_rows")
    for t in results:
        try:
            wr.writerow(t)
        except Exception as e:
            logging.error(e)


def main():
    desc = "Retrieves information about table sizes and their number of rows."
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-t', '--software-type',
                        dest="software_type",
                        required=True,
                        help="Software type (mysql or postgresql)")
    parser.add_argument('-e', '--exclude',
                        action="append",
                        dest="exclusions",
                        help="Exclude databases from the list.")
    parser.add_argument('-d', '--debug',
                        help='Print lots of debugging statements',
                        action='store_const', dest='loglevel',
                        const=logging.DEBUG,
                        default=logging.WARNING)
    parser.add_argument('-v', '--verbose',
                        help='Be verbose',
                        action='store_const', dest='loglevel',
                        const=logging.INFO)
    parser.add_argument('-q', '--quiet',
                        help='Be quiet',
                        action='store_const', dest='loglevel',
                        const=logging.CRITICAL)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)

    if args.software_type == "postgresql":
        dbms = Postgresql(args.exclusions)
    elif args.software_type == "mysql":
        dbms = Mysql(args.exclusions)
    else:
        logging.error("Unsupported dbms type: {}".format(args.software_type))
        return 1
    results = dbms.fetch_information()
    if results:
        display_information(results)
    else:
        logging.info("No information returned, perhaps you should check your exclusion list.")


if __name__ == '__main__':
    main()
