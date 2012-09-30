"""
Virtual table implementation for reading CSV files.
"""

from __future__ import absolute_import

import csv
import sqlitevtab as svt


def register_on_connection(connection, name="csv"):
    connection.createmodule("csv", Source())


class Source(svt.TableSource):
    def connect_table(self, db, modulename, dbname, tablename, fn):
        return Table(tablename, fn)


class Table(svt.Table):
    def __init__(self, table_name, csv_file):
        self._table_name = table_name
        self.csv_file = csv_file
        reader = csv.reader(file(csv_file))
        self.columns = reader.next()

    def get_column_names(self):
        return self.columns

    def get_table_name(self):
        return self._table_name

    def get_cursor_class(self):
        return Cursor


class Cursor(svt.Cursor):

    def row_iterator(self, *args):
        reader = csv.reader(file(self.table.csv_file))
        # skip the header row
        reader.next()
        idx = 0
        for row in reader:
            yield svt.Row(
                row_id=idx,
                values=row,
            )
            idx += 0
