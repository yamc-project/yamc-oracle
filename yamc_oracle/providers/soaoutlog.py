# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time
import re
import os

import datetime
import threading

from yamc.providers import PerformanceProvider, perf_checker

from wls_analytics import SOALogReader


def round_time_minutes(time, minutes):
    rounded_minutes = (time.minute // minutes) * minutes
    rounded_time = time.replace(minute=rounded_minutes, second=0, microsecond=0)
    return rounded_time


class SOAOutLogProvider(PerformanceProvider):
    """
    A yamc provider for Oracle SOA out logs.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.soaout_log = self.config.value_str("soaout_log", required=True)
        self.datetime_format = self.config.value("datetime_format", default="%b %d, %Y %I:%M:%S,%f %p UTC")
        self.buffer_minutes = self.config.value("buffer_minutes", default=2)
        self.simulated_time = self.config.value("simulated_time.start", default=None)
        self.simulated_time_delta = self.config.value("simulated_time.delta", default=1)
        self.simulated_time_format = self.config.value("simulated_time.format", default="%Y-%m-%d %H:%M:%S")
        self._time = None
        self.reader = SOALogReader(self.soaout_log, self.datetime_format)
        self.lock = threading.Lock()

    @property
    def source(self):
        return self.soaout_log

    def time(self):
        if self.simulated_time is not None:
            if self._time is None:
                self._time = datetime.datetime.strptime(self.simulated_time, self.simulated_time_format)
            else:
                self._time += datetime.timedelta(minutes=self.simulated_time_delta)
        else:
            self._time = datetime.datetime.now()
        return self._time

    @perf_checker()
    def soaerrors(self, label_parser=None):
        with self.lock:
            _now = self.time()
            time_from = round_time_minutes(_now - datetime.timedelta(minutes=2), 1)
            time_to = round_time_minutes(_now - datetime.timedelta(minutes=1), 1)
            self.log.debug("Reading data from %s to %s.", time_from, time_to)
            data = []

            self.reader.open()
            try:
                for entry in self.reader.read_errors(time_from=time_from, time_to=time_to, label_parser=label_parser):
                    data.append(entry.to_dict())
                return data
            finally:
                self.reader.close()
