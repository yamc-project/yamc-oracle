# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time

import dms_collector
from dms_collector import DmsCollector
from yamc.providers import PerformanceProvider, perf_checker, OperationalError

from yamc.utils import Map, perf_counter


class DmsProvider(PerformanceProvider):
    """
    A yamc provider for Oracle FMW DMS Spy application. DMS Spy provides a massive amount of metrics about WebLogic
    and other software running in WebLogic. The provider is a wrapper around dms-collector.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.connect_time = 0
        self.dms = None

        # configuration
        self.admin_url = self.config.value_str("admin_url", required=True)
        self.username = self.config.value_str("username", required=True)
        self.password = self.config.value_str("password", required=True)
        self.reconnect_after = self.config.value_int("reconnect_after", default=3600)

        # set timeouts for the dms collector
        self.read_timeout = self.config.value_int("timeout_read", default=dms_collector.dms.TIMEOUT_READ)
        self.connect_timeout = self.config.value_int("timeout_connect", default=dms_collector.dms.TIMEOUT_CONNECT)

    def init_dms(self):
        if self.dms is None or time.time() - self.connect_time > self.reconnect_after:
            if self.dms is not None:
                self.log.info("Reconnecting to DMS Spy after %d seconds." % self.reconnect_after)
            self.dms = DmsCollector(
                self.admin_url,
                username=self.username,
                password=self.password,
                read_timeout=self.read_timeout,
                connect_timeout=self.connect_timeout,
            )
            self.log.info(
                "DMS provider initialized: url=%s, username=%s, password=(secret), read-timeout=%d, connect-timeout=%d"
                % (self.admin_url, self.username, self.read_timeout, self.connect_timeout)
            )
            self.connect_time = time.time()

    @perf_checker(id_arg="table")
    def table(self, table, include=[], exclude=[], filter=None):
        try:
            self.init_dms()
            d = self.dms.collect(table, include=include, exclude=exclude, filter=filter)
            data = d["data"]
            self.log.info(f"The DMS retrieved {len(data)} records in {d['query_time']:0.4f} seconds from '{table}'.")
            return data
        except Exception as e:
            raise OperationalError(f"Error while retrieving data from DMS: {e}")
