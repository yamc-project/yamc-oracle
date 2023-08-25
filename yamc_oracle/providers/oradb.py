# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import re
import time
import os
import threading

import oracledb

from yamc.providers import PerformanceProvider, perf_checker, OperationalError
from yamc.utils import Map

from typing import Dict, List, Type


def makeDictFactory(cursor):
    """
    Create a dictionary from the cursor description.
    """
    columnNames = [d[0].lower() for d in cursor.description]

    def createRow(*args):
        return dict(zip(columnNames, args))

    return createRow


def hide_password(connstr):
    """
    Hide the password in the connection string.
    """
    return re.sub("\/(.+)@", "/(secret)@", connstr)


class OraDBProvider(PerformanceProvider):
    """
    Provider that executes SQL statements against an Oracle database.
    """

    def __init__(self, config, component_id):
        """
        Initialize the provider.
        """
        super().__init__(config, component_id)
        self.cache = Map()

        # configuration
        self.connstr = self.config.value_str("connstr", required=True)
        self.reconnect_after = self.config.value_int("reconnect_after", required=False, default=3600)
        self.sql_files_dir = self.config.get_dir_path(self.config.value_str("sql_files_dir", required=True), check=True)
        self.connect_timeout = self.config.value_int("connect_timeout", default=10)
        self.call_timeout = self.config.value_int("call_timeout", default=None)
        self.max_connections = self.config.value_int("max_connections", default=10)

    def open(
        self, sql_file: str, call_timeout: int = None
    ) -> Map["fname":str, "connection":str, "connect_time":int, "statement":str, "lock" : threading.Lock]:
        """
        Open a connection to the database and return the connection object. The connections are cached based on the
        SQL file name. If the connection is older than the reconnect_after parameter, it is closed and a new one is
        opened.
        """
        fname = os.path.realpath("%s/%s" % (self.sql_files_dir, sql_file))
        if not os.path.isfile(fname):
            raise Exception("The SQL file %s does not exist!" % fname)

        if self.cache.get(fname) is None:
            cache = Map(
                fname=fname,
                connection=None,
                connect_time=None,
                statement=None,
                lock=threading.Lock(),
            )
            lines = []
            with open(fname, "r") as file:
                lines = [x for x in file]
            cache.statement = "".join(lines)
            self.cache[fname] = cache

        cache = self.cache.get(fname)
        if cache.connection is None or time.time() - cache.connect_time > self.reconnect_after:
            if cache.connection is not None:
                cache.connection.close()
                cache.connection = None
            self.log.info(
                f"Opening the DB connection, connstr={hide_password(self.connstr)}, connect_timeout={self.connect_timeout}"
            )
            if len([x for x in self.cache.values() if x.connection is not None]) - 1 > self.max_connections:
                raise Exception("The maximum number of connections (%d) has been reached!" % self.max_connections)
            cache.connection = oracledb.connect(self.connstr, tcp_connect_timeout=self.connect_timeout)
            if call_timeout is not None:
                cache.connection.callTimeout = call_timeout
            elif self.call_timeout is not None:
                cache.connection.callTimeout = self.call_timeout
            cache.connect_time = time.time()
        return cache

    def destroy(self):
        """
        Close all connections to the database.
        """
        super().destroy()
        for k, item in self.cache.items():
            if item.connection is not None:
                self.log.info(f"Closing the DB connection for {k}.")
                item.connection.close()
                item.connection = None
        self.cache = Map()

    @perf_checker(id_arg="sql_file")
    def sql(self, sql_file: str, variables: List[str] = [], types: Dict[str, Type] = None, call_timeout: int = None):
        """
        Execute the SQL statement in the file sql_file. The file is searched in the sql_files_dir directory.
        The variables are passed to the SQL statement as parameters. The types parameter is a dictionary that
        specifies the type of fields in the result set that can be used to explicltty convert the values to the
        specified type.
        """
        try:
            item = self.open(sql_file, call_timeout=call_timeout)
            with item.lock:
                self.log.debug("Running the SQL statement: %s" % re.sub("\s+", " ", item.statement))
                cursor = item.connection.cursor()
                try:
                    query_time = time.time()
                    cursor.execute(item.statement, variables)
                    cursor.rowfactory = makeDictFactory(cursor)
                    data = []
                    for row in cursor:
                        if types is not None and len(types) > 0:
                            for k, v in types.items():
                                if k in row.keys():
                                    row[k] = v(row[k])
                        data.append(row)
                    running_time = time.time() - query_time

                    self.log.info(
                        f"The result of the statement {os.path.basename(sql_file)} has {len(data)} rows and was retrieved "
                        + f"in {running_time:0.04f} seconds."
                    )
                    return data
                finally:
                    cursor.close()
        except Exception as e:
            raise OperationalError(f"Error while executing the SQL statement '{sql_file}': {e}", e)
