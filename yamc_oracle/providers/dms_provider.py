# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time

from dms_collector import DmsCollector
from yamc.providers import PerformanceProvider

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

    def init_dms(self):
        if self.dms is None or time.time() - self.connect_time > self.reconnect_after:
            if self.dms is not None:
                self.log.info("Reconnecting to DMS Spy after %d seconds." % self.reconnect_after)
            self.dms = DmsCollector(self.admin_url, username=self.username, password=self.password)
            self.log.info(
                "DMS provider initialized: url=%s, username=%s, password=(secret)" % (self.admin_url, self.username)
            )
            self.connect_time = time.time()

    def table(self, table, include=[], exclude=[], filter=None):
        self.init_dms()
        d = self.dms.collect(table, include=include, exclude=exclude, filter=filter)

        def _add_time(x):
            x["time"] = d["time"]
            return x

        data = list(map(_add_time, d["data"]))
        self.update_perf(table, len(data), d["query_time"])
        self.log.info(f"The DMS retrieved {len(data)} records in {d['query_time']:0.4f} seconds from '{table}'.")
        return data
