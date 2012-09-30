"""
Provides abstract base classes to help in constructing SQLite virtual table implementations.

These classes act as adapters from the raw vtable interface expected by the :py:mod:`apsw` module
to a more pythonic interface that can be customized through subclassing and overriding methods.
In particular, the cursor interface is built around iterators, making it easy to surface iterables
as resultsets.

:py:class:`TableSource` is the entry point interface, with other classes descending from it to
provide table instances and cursors.
"""

import abc


class TableSource(object):
    """
    Represents a source of tables.

    Register instances of this class with an :py:class:`apsw.Connection instance` as follows:

    .. codeblock:: python

        connection.createmodule("modulename", SourceSubclass())

    Users of that connection can then instantiate tables from the registered source
    as follows:

    .. codeblock:: sql

        CREATE VIRTUAL TABLE tablename USING modulename
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def connect_table(self, db, module_name, db_name, table_name, *args):
        """
        Connect to an existing table.

        This is called when reconnecting to an existing database that already
        has virtual tables in its schema, to reload the implementation of
        those tables.

        If the ``CREATE VIRTUAL TABLE`` statement for a given table has arguments,
        these are passed in as additional positional parameters, in ``*args``.

        Return an instance of a subclass of :py:class:`Table` representing the
        requested table.
        """
        pass

    def create_table(self, db, module_name, db_name, table_name, *args):
        """
        Create a new table.

        This is called when a ``CREATE VIRTUAL TABLE`` statement is executed,
        to provision any persistent resources required by the table.

        In terms of interface it is identical to :py:meth:`connect_table`,
        and defaults to simply delegating to that method if no special
        create implementation is provided. Only implementations that actually
        need to allocate some persistent resources for each table will
        need to override this.
        """
        return self.connect_table(db, module_name, db_name, table_name, *args)

    def Create(self, db, modulename, dbname, tablename, *args):
        table = self.create_table(db, modulename, dbname, tablename, *args)
        return table.create_table_sql, table

    def Connect(self, db, modulename, dbname, tablename, *args):
        table = self.create_table(db, modulename, dbname, tablename, *args)
        return table.create_table_sql, table


class Table(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_column_names(self):
        """
        Return a sequence of column names that appear in this table.

        Override this to return the names of your columns. The order of these
        items is important because it must match the order used to return
        the row data later.
        """
        return []

    def get_table_name(self):
        """
        Return a name for the table.

        This is optional and doesn't really do much, so the default is fine
        in most cases.
        """
        return "<unnamed>"

    @abc.abstractmethod
    def get_cursor_class(self):
        """
        Return the class that will represent cursors on this table.

        This must be overridden to return a subclass of :py:class:`Cursor`.
        This class will be automatically instantiated each time a cursor
        is required for the table.
        """
        return None

    @property
    def column_names(self):
        return self.get_column_names()

    @property
    def table_name(self):
        return self.get_table_name()

    def find_best_index(self, constraints, orders):
        """
        This is not yet implemented.
        """
        # TODO: Implement this
        return None

    def disconnect(self):
        """
        Do any cleanup required when disconnecting from the table.

        Most implementations don't need this, but if any resources were
        allocated when connecting to this table they should be freed
        here.
        """
        pass

    def drop(self):
        """
        Do any cleanup required when dropping from the table.

        Most implementations don't need this, but if any resources were
        allocated when creating to this table they should be freed
        here.
        """
        pass

    @property
    def create_table_sql(self):
        # FIXME: This should be more careful about escaping quotes in
        # what it gets.
        return "CREATE TABLE \"%s\" (%s)" % (
            self.table_name,
            ", ".join(self.column_names)
        )

    def BestIndex(self, *args):
        # TODO: Implement this in terms of find_best_index
        return None

    def Open(self):
        cls = self.get_cursor_class()
        return cls(self)

    def Disconnect(self):
        self.disconnect()

    def Destroy(self):
        self.drop()


class Cursor(object):
    """
    Represents a cursor on a particular table.

    This class is not instantiated directly by the implementor, but rather
    instantiated when needed to satisfy a query against a table. Consequently
    its constructor should not be overridden. Any initialization behavior
    required can be handled in :py:meth:`row_iterator`.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, table):
        self.table = table
        self.eof = False
        self.row = None

    @abc.abstractmethod
    def row_iterator(self, index_number, index_name, *args):
        """
        Obtain a row iterator for the table, or optionally for an index on
        the table.

        Subclasses must override this to return some kind of iterator
        that produces instances of :py:class:`Row`. The index parameters
        are significant only if this table suggested a particular index
        in its implementation of :py:meth:`Table.find_best_index`.
        """
        pass

    def close(self):
        """
        Free any resources associated with the cursor.

        Called when this cursor is no longer needed.
        """
        pass

    def Filter(self, index_number, index_name, constraintargs):
        self.iter = self.row_iterator(index_number, index_name, *constraintargs)
        # Load the first row
        self.Next()

    def Eof(self):
        return self.eof

    def Rowid(self):
        return self.row.row_id

    def Column(self, idx):
        return self.row.values[idx]

    def Next(self):
        try:
            self.row = self.iter.next()
        except StopIteration:
            self.eof = True

    def Close(self):
        self.close()


class Row(object):
    """
    Represents a result row.

    Each result row has a ``row_id``, which must be an integer, and a
    sequence of values, whose indicies must correspond with the
    column names sequence for the table whose result row this is.
    """

    def __init__(self, row_id, values):
        self.row_id = row_id
        self.values = values


del abc
