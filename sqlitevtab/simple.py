"""
A simple, declarative table source wrapper.
"""

import sqlitevtab as svt


def create_table_source(**tables):
    real_tables = {}
    for table_name, table_def in tables.iteritems():
        real_tables[table_name] = Table(**table_def)
    return Source(real_tables)


class Source(svt.TableSource):
    tables = None

    def __init__(self, tables):
        self.tables = tables

    def connect_table(self, db, modulename, dbname, tablename):
        return self.tables[tablename]

    def register_tables_on_connection(self, conn, module_name):
        conn.createmodule(module_name, self)
        cursor = conn.cursor()
        for table_name in self.tables:
            cursor.execute("CREATE VIRTUAL TABLE \"%s\" USING %s" % (table_name, module_name))


class Table(svt.Table):

    def __init__(self, columns=[], make_iterator=None):
        self.columns = columns
        if make_iterator is None:
            def make_iterator():
                if False:
                    yield 0
        self.make_iterator = make_iterator

    def get_column_names(self):
        return self.columns

    def get_cursor_class(self):
        return Cursor


class Cursor(svt.Cursor):

    def row_iterator(self, *args):
        make_iterator = self.table.make_iterator
        for item in make_iterator():
            yield svt.Row(
                row_id=id(item),
                values=[
                    item.get(x, None) for x in self.table.columns
                ],
            )
