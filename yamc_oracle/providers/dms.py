# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time

import dms_collector
import threading
from dms_collector import DmsCollector
from yamc.providers import PerformanceProvider, perf_checker, OperationalError

from dms_collector import TableNotExistError, DataParserError

from yamc.utils import Map


class DmsProvider(PerformanceProvider):
    """
    A yamc provider for Oracle FMW DMS Spy application. DMS Spy provides a massive amount of metrics about WebLogic
    and other software running in WebLogic. The provider is a wrapper around dms-collector.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.dms = Map()

        # configuration
        self.admin_url = self.config.value_str("admin_url", required=True)
        self.username = self.config.value_str("username", required=True)
        self.password = self.config.value_str("password", required=True)
        self.reconnect_after = self.config.value_int("reconnect_after", default=3600)
        self.read_timeout = self.config.value_int("timeout_read", default=dms_collector.dms.TIMEOUT_READ)
        self.connect_timeout = self.config.value_int("timeout_connect", default=dms_collector.dms.TIMEOUT_CONNECT)
        self.log.info(
            f"dms_collector will use the following connection details: url={self.admin_url}, username={self.username}, "
            + f"password=******, reconnect_after={self.reconnect_after}, read_timeout={self.read_timeout}, "
            + f"connect_timeout={self.connect_timeout}"
        )

    @property
    def source(self):
        """
        Returns the source of the data, which is the admin URL.
        """
        return self.admin_url

    def get_dms(self, table):
        """
        Returns a DMS object for the given table. If the object does not exist or it is too old, a new object is
        created. The object is stored in the dms dictionary.
        """
        dms_id = str(threading.get_native_id()) + "/" + table
        dms = self.dms.get(dms_id)
        if dms is None or dms.force_reconnect or time.time() - dms.connect_time > self.reconnect_after:
            if dms is not None:
                if dms.force_reconnect:
                    self.log.info("Reconnecting to DMS due to an error (id=%s)." % dms_id)
                else:
                    self.log.info("Reconnecting to DMS after %d seconds (id=%s)." % (self.reconnect_after, dms_id))
            dms_collector = DmsCollector(
                self.admin_url,
                username=self.username,
                password=self.password,
                read_timeout=self.read_timeout,
                connect_timeout=self.connect_timeout,
            )
            self.log.info(f"dms_collector created: id={dms_id}")
            dms = Map(
                dms_id=dms_id, dms_collector=dms_collector, table=table, connect_time=time.time(), force_reconnect=False
            )
            self.dms[dms_id] = dms
        return dms

    @perf_checker(id_arg="table")
    def table(self, table, include=[], exclude=[], filter=None):
        """
        Returns a list of records from the given table. The table is a DMS table name, e.g., "JVMRuntime".
        The include and exclude parameters are lists of column names to include or exclude from the result.
        The filter parameter is a dictionary of column names and values to filter the result. The function uses
        a cache to store the DMS object for each table name and the calling thread.
        """
        try:
            dms = self.get_dms(table)
            try:
                d = dms.dms_collector.collect(table, include=include, exclude=exclude, filter=filter)
                data = d["data"]
                self.log.info(
                    f"The DMS retrieved {len(data)} records in {d['query_time']:0.4f} seconds from '{table}'."
                )
                return data
            except (DataParserError, TableNotExistError) as e:
                dms.force_reconnect = True
                raise OperationalError(
                    f"Invalid data retrieved from DMS, will reconnect to DMS on the next run. {e}", e
                )
        except Exception as e:
            raise OperationalError(f"Error while retrieving data from DMS: {e}", e)
